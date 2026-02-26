# Market Monitor V2 Improved — 최종 개발 계획서

> Codex 5.3 리뷰 피드백 전면 반영. V1 실패 교훈 + 실구현 검증 완료.

---

## 1. 아키텍처: 점진적 리팩터링

### 1.1 전략: 기존 코드 기반 단계적 개선

V1의 전면 재구성 리스크를 회피하고, 기존 `pipeline/` 구조를 유지하며 모듈별로 교체한다.

```
기존 pipeline/         →  점진적 개선
├── fetcher.py         →  fetchers/ 분리 (Phase 1)
├── config.py          →  Pydantic v2 재작성 (Phase 1)
├── (없음)             →  security.py 신규 (Phase 1)
├── normalizer.py      →  normalize + watermark 연동 (Phase 2)
├── (없음)             →  state.py 신규 (Phase 2)
├── merger.py          →  stable dedup hash 적용 (Phase 3)
├── stats.py           →  실패/partial 추적 강화 (Phase 3)
└── pipeline.py        →  오케스트레이터 통합 (Phase 4)
```

### 1.2 V1 런타임 결함 수정 목록

| 결함 | 원인 | 수정 |
|------|------|------|
| `results` 미정의 | `gather` 반환값 미할당 | `results = await asyncio.gather(...)` |
| `FetchResult` 생성자 | dataclass인데 kwargs 혼재 | `@dataclass`로 명시, 모든 필드 선언 |
| `asdict` import 누락 | stats.py에서 사용하지만 미import | `from dataclasses import asdict` 추가 |
| 출력 타이밍 | 통계 미완료 상태에서 저장 | **stats 완료 → output 저장** 순서로 변경 |

---

## 2. 핵심 클래스 설계

### 2.1 Config (Pydantic v2 완전 호환)

```python
from pydantic import BaseModel, Field, ConfigDict, field_validator, model_validator
from typing import Literal, Any

class FieldMapping(BaseModel):
    model_config = ConfigDict(extra="forbid")
    source_field: str
    target_field: Literal["source", "timestamp", "value", "unit"]
    transform: str | None = None

class SourceConfig(BaseModel):
    model_config = ConfigDict(strict=True, extra="forbid")

    name: str = Field(min_length=1, max_length=100, pattern=r'^[a-zA-Z0-9_-]+$')
    type: Literal["rest", "csv", "websocket", "graphql", "file"]
    url: str | None = None
    path: str | None = None
    timeout: float = Field(default=10.0, gt=0, le=60)

    # Type-specific — mutable defaults 수정
    headers: dict[str, str] = Field(default_factory=dict)
    query: str | None = None
    variables: dict[str, Any] = Field(default_factory=dict)  # Any for nested values
    ws_duration: float = Field(default=5.0, gt=0, le=30)

    # Custom field mapping
    field_mapping: list[FieldMapping] = Field(default_factory=list)

    # Incremental
    incremental: bool = False
    incremental_field: str = "timestamp"

    @model_validator(mode="after")
    def validate_type_requirements(self):
        """타입별 필수 필드 강제"""
        needs_url = {"rest", "websocket", "graphql"}
        needs_path = {"file"}
        can_have_either = {"csv"}  # url 또는 path

        if self.type in needs_url and not self.url:
            raise ValueError(f"type={self.type} requires 'url'")
        if self.type in needs_path and not self.path:
            raise ValueError(f"type={self.type} requires 'path'")
        if self.type in can_have_either and not (self.url or self.path):
            raise ValueError(f"type={self.type} requires 'url' or 'path'")
        if self.type == "graphql" and not self.query:
            raise ValueError("type=graphql requires 'query'")
        return self

    @field_validator('url')
    @classmethod
    def validate_url(cls, v):
        if v is not None:
            from .security import validate_url_format
            validate_url_format(v)
        return v

    @field_validator('path')
    @classmethod
    def validate_path(cls, v):
        if v is not None:
            from .security import validate_file_path
            validate_file_path(v)
        return v

class PipelineConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    sources: list[SourceConfig] = Field(min_length=1)
    output: str = "output.json"
    max_concurrent: int = Field(default=10, ge=1, le=50)
    global_timeout: float = Field(default=15.0, gt=0, le=300)
    allowed_hosts: list[str] = Field(default_factory=list)
    allowed_output_dirs: list[str] = Field(default_factory=lambda: ["."])
    allowed_input_dirs: list[str] = Field(default_factory=lambda: ["."])
    state_file: str = ".state.json"

    @field_validator('output')
    @classmethod
    def validate_output_path(cls, v):
        """Output path allowlist 검증은 run-time에 allowed_output_dirs와 함께"""
        return v
```

**Config 로더**: YAML 통일 (pyyaml). JSON fallback 지원.

```python
# config_loader.py
import yaml
from pathlib import Path

def load_config(path: str) -> PipelineConfig:
    raw = Path(path).read_text()
    if path.endswith(('.yml', '.yaml')):
        data = yaml.safe_load(raw)
    else:
        import json
        data = json.loads(raw)
    return PipelineConfig.model_validate(data)
```

### 2.2 Security Module (SSRF + Path + GraphQL 강화)

```python
# security.py
import ipaddress
import socket
from urllib.parse import urlparse
from pathlib import Path

BLOCKED_RANGES = [
    ipaddress.ip_network("0.0.0.0/8"),
    ipaddress.ip_network("10.0.0.0/8"),
    ipaddress.ip_network("100.64.0.0/10"),   # Carrier-grade NAT
    ipaddress.ip_network("127.0.0.0/8"),
    ipaddress.ip_network("169.254.0.0/16"),
    ipaddress.ip_network("172.16.0.0/12"),
    ipaddress.ip_network("192.0.0.0/24"),
    ipaddress.ip_network("192.168.0.0/16"),
    ipaddress.ip_network("198.18.0.0/15"),    # Benchmarking
    ipaddress.ip_network("::1/128"),
    ipaddress.ip_network("fc00::/7"),
    ipaddress.ip_network("fe80::/10"),
]

def _is_blocked_ip(ip_str: str) -> bool:
    ip = ipaddress.ip_address(ip_str)
    return any(ip in net for net in BLOCKED_RANGES)

def validate_url_format(url: str) -> None:
    """Config 시점: scheme + hostname 기본 검증"""
    parsed = urlparse(url)
    if parsed.scheme not in ("http", "https", "ws", "wss"):
        raise ValueError(f"Disallowed scheme: {parsed.scheme}")
    if not parsed.hostname:
        raise ValueError("Missing hostname")

def validate_url_at_connect(url: str, allowed_hosts: list[str] | None = None) -> str:
    """연결 시점: DNS resolve → IP 검증. 반환값은 resolved URL."""
    parsed = urlparse(url)
    hostname = parsed.hostname

    if allowed_hosts and hostname not in allowed_hosts:
        raise ValueError(f"Host not in allowlist: {hostname}")

    # Resolve and check every IP
    try:
        infos = socket.getaddrinfo(hostname, parsed.port or 443, proto=socket.IPPROTO_TCP)
    except socket.gaierror:
        raise ValueError(f"Cannot resolve: {hostname}")

    for _, _, _, _, addr in infos:
        if _is_blocked_ip(addr[0]):
            raise ValueError(f"Blocked IP: {addr[0]} for host {hostname}")

    return url

def create_ssrf_safe_connector() -> "aiohttp.TCPConnector":
    """Redirect hop별 IP 재검증 커넥터"""
    import aiohttp

    class SSRFSafeResolver(aiohttp.DefaultResolver):
        async def resolve(self, host, port=0, family=socket.AF_INET):
            results = await super().resolve(host, port, family)
            for r in results:
                if _is_blocked_ip(r['host']):
                    raise ValueError(f"SSRF blocked: {r['host']} (resolved from {host})")
            return results

    return aiohttp.TCPConnector(
        resolver=SSRFSafeResolver(),
        limit=20,
    )

def validate_file_path(path: str, allowed_dirs: list[str] | None = None) -> Path:
    """Path traversal 방지 + symlink 해소"""
    resolved = Path(path).resolve()
    if not allowed_dirs:
        allowed_dirs = ["."]
    for allowed in allowed_dirs:
        allowed_resolved = Path(allowed).resolve()
        if resolved.is_relative_to(allowed_resolved):
            return resolved
    raise ValueError(f"Path outside allowed directories: {path}")

def validate_output_path(path: str, allowed_dirs: list[str]) -> Path:
    """Output path allowlist 검증"""
    return validate_file_path(path, allowed_dirs)

def sanitize_graphql_query(query: str) -> str:
    """
    GraphQL 안전성 검증:
    1. AST 파싱 시도 (graphql-core 있으면)
    2. Fallback: 키워드 기반 차단
    """
    try:
        from graphql import parse as gql_parse, DocumentNode
        doc: DocumentNode = gql_parse(query)
        for defn in doc.definitions:
            op = getattr(defn, 'operation', None)
            if op and op.value != 'query':
                raise ValueError(f"Only 'query' operations allowed, got '{op.value}'")
            # Introspection field check
            selections = getattr(defn, 'selection_set', None)
            if selections:
                for sel in selections.selections:
                    name = getattr(sel, 'name', None)
                    if name and name.value.startswith('__'):
                        raise ValueError("Introspection queries blocked")
        return query
    except ImportError:
        pass  # Fallback to string-based

    # Fallback: conservative string checks
    normalized = query.strip().lower()
    if any(kw in normalized for kw in ("mutation", "subscription")):
        raise ValueError("Only queries allowed")
    if "__schema" in normalized or "__type" in normalized:
        raise ValueError("Introspection queries blocked")
    if "${" in query or "#{" in query:
        raise ValueError("String interpolation not allowed")
    return query
```

### 2.3 Fetchers

```python
# fetchers/base.py
from dataclasses import dataclass, field
from typing import Any

@dataclass
class FetchResult:
    source_name: str
    records: list[dict[str, Any]] = field(default_factory=list)
    fetch_time_ms: float = 0.0
    error: str | None = None
    is_partial: bool = False

class BaseFetcher(ABC):
    def __init__(self, config: "SourceConfig", state_store: "StateStore"):
        self.config = config
        self.state = state_store

    @abstractmethod
    async def fetch(self, session: "aiohttp.ClientSession") -> FetchResult:
        ...

    def get_incremental_filter(self) -> Any | None:
        if not self.config.incremental:
            return None
        return self.state.get_watermark(self.config.name)
```

```python
# fetchers/rest.py
import time
import aiohttp

class RestFetcher(BaseFetcher):
    async def fetch(self, session: aiohttp.ClientSession) -> FetchResult:
        t0 = time.perf_counter()
        last = self.get_incremental_filter()
        params = {}
        if last is not None:
            params["since"] = last

        async with session.get(
            self.config.url,
            headers=self.config.headers,
            params=params,
            timeout=aiohttp.ClientTimeout(total=self.config.timeout),
        ) as resp:
            resp.raise_for_status()
            data = await resp.json()
            records = data if isinstance(data, list) else data.get("data", [data])
            elapsed = (time.perf_counter() - t0) * 1000
            return FetchResult(
                source_name=self.config.name,
                records=records,
                fetch_time_ms=elapsed,
            )
```

```python
# fetchers/csv_fetcher.py
import csv
import io
import time

class CsvFetcher(BaseFetcher):
    async def fetch(self, session) -> FetchResult:
        t0 = time.perf_counter()
        if self.config.url:
            async with session.get(
                self.config.url,
                timeout=aiohttp.ClientTimeout(total=self.config.timeout),
            ) as resp:
                resp.raise_for_status()
                text = await resp.text()
        else:
            text = Path(self.config.path).read_text()

        # Line-by-line parsing (메모리 효율)
        reader = csv.DictReader(io.StringIO(text))
        records = list(reader)

        elapsed = (time.perf_counter() - t0) * 1000
        return FetchResult(
            source_name=self.config.name,
            records=records,
            fetch_time_ms=elapsed,
        )
```

```python
# fetchers/websocket.py
import asyncio
import json
import time

class WebSocketFetcher(BaseFetcher):
    async def fetch(self, session) -> FetchResult:
        t0 = time.perf_counter()
        records = []
        is_partial = False
        try:
            async with asyncio.timeout(self.config.ws_duration):
                async with session.ws_connect(self.config.url) as ws:
                    async for msg in ws:
                        if msg.type == aiohttp.WSMsgType.TEXT:
                            records.append(json.loads(msg.data))
                        elif msg.type in (aiohttp.WSMsgType.ERROR, aiohttp.WSMsgType.CLOSED):
                            break
        except TimeoutError:
            is_partial = True  # ws_duration 만료 — 정상 종료

        elapsed = (time.perf_counter() - t0) * 1000
        return FetchResult(
            source_name=self.config.name,
            records=records,
            fetch_time_ms=elapsed,
            is_partial=is_partial,
        )
```

```python
# fetchers/graphql.py
class GraphQLFetcher(BaseFetcher):
    async def fetch(self, session) -> FetchResult:
        t0 = time.perf_counter()
        query = sanitize_graphql_query(self.config.query)
        variables = dict(self.config.variables)

        last = self.get_incremental_filter()
        if last is not None:
            variables["since"] = last

        payload = {"query": query, "variables": variables}
        async with session.post(
            self.config.url,
            json=payload,
            headers=self.config.headers,
            timeout=aiohttp.ClientTimeout(total=self.config.timeout),
        ) as resp:
            resp.raise_for_status()
            data = await resp.json()
            if "errors" in data:
                raise ValueError(f"GraphQL errors: {data['errors']}")
            records = self._extract_records(data.get("data", {}))
            elapsed = (time.perf_counter() - t0) * 1000
            return FetchResult(
                source_name=self.config.name,
                records=records,
                fetch_time_ms=elapsed,
            )

    def _extract_records(self, data: dict) -> list[dict]:
        """첫 번째 list 값 추출, 없으면 단일 dict를 list로"""
        if isinstance(data, list):
            return data
        for v in data.values():
            if isinstance(v, list):
                return v
        return [data] if data else []
```

```python
# fetchers/file.py
import json
from pathlib import Path
import time

class FileFetcher(BaseFetcher):
    async def fetch(self, session) -> FetchResult:
        t0 = time.perf_counter()
        p = Path(self.config.path)
        text = p.read_text()

        if p.suffix == ".json":
            data = json.loads(text)
            records = data if isinstance(data, list) else [data]
        elif p.suffix == ".xml":
            import xml.etree.ElementTree as ET
            root = ET.fromstring(text)
            records = [
                {child.tag: child.text for child in elem}
                for elem in root
            ]
        else:
            raise ValueError(f"Unsupported file type: {p.suffix}")

        elapsed = (time.perf_counter() - t0) * 1000
        return FetchResult(
            source_name=self.config.name,
            records=records,
            fetch_time_ms=elapsed,
        )
```

```python
# fetchers/__init__.py
def create_fetcher(config: "SourceConfig", state: "StateStore") -> BaseFetcher:
    mapping = {
        "rest": RestFetcher,
        "csv": CsvFetcher,
        "websocket": WebSocketFetcher,
        "graphql": GraphQLFetcher,
        "file": FileFetcher,
    }
    cls = mapping.get(config.type)
    if not cls:
        raise ValueError(f"Unknown source type: {config.type}")
    return cls(config, state)
```

### 2.4 Incremental State (완전한 설계)

```python
# state.py
import json
import shutil
from pathlib import Path
from datetime import datetime
from filelock import FileLock

STATE_VERSION = 1

class StateStore:
    """
    파일 기반 incremental state.
    
    설계 원칙:
    1. Source별 normalize 직후 watermark 계산 (max값)
    2. 파이프라인 성공 후에만 disk에 persist
    3. 원자적 쓰기 (tmp → rename)
    4. FileLock으로 다중 프로세스 충돌 방지
    5. 버전 관리 + 자동 백업
    """

    def __init__(self, path: str = ".state.json"):
        self.path = Path(path)
        self.lock = FileLock(f"{path}.lock", timeout=10)
        self._state: dict = {"version": STATE_VERSION, "sources": {}}
        self._pending: dict[str, str] = {}  # source → pending watermark
        self._load()

    def _load(self):
        if not self.path.exists():
            return
        with self.lock:
            try:
                data = json.loads(self.path.read_text())
                if data.get("version", 0) != STATE_VERSION:
                    self._migrate(data)
                else:
                    self._state = data
            except (json.JSONDecodeError, KeyError):
                # 손상된 state → 백업 후 초기화
                backup = self.path.with_suffix(f".corrupt.{datetime.utcnow():%Y%m%dT%H%M%S}.json")
                shutil.copy2(self.path, backup)
                self._state = {"version": STATE_VERSION, "sources": {}}

    def _migrate(self, old_data: dict):
        """이전 버전 state 마이그레이션"""
        self._state = {"version": STATE_VERSION, "sources": {}}
        # V0 → V1: flat dict를 sources 아래로
        for k, v in old_data.items():
            if k not in ("version",) and isinstance(v, dict):
                self._state["sources"][k] = v

    def get_watermark(self, source: str) -> str | None:
        return self._state["sources"].get(source, {}).get("watermark")

    def stage_watermark(self, source: str, value: str):
        """
        Normalize 직후 호출. 아직 disk에 쓰지 않음.
        여러 번 호출 시 max값 유지.
        """
        current = self._pending.get(source)
        if current is None or value > current:
            self._pending[source] = value

    def commit(self, successful_sources: set[str], partial_sources: set[str]):
        """
        파이프라인 종료 시 호출.

        갱신 정책:
        - success: pending watermark 저장
        - partial: pending watermark 저장 (받은 데이터까지는 유효)
        - failed: 이전 watermark 유지 (다음 실행에서 재시도)
        """
        for source in successful_sources | partial_sources:
            wm = self._pending.get(source)
            if wm is not None:
                if source not in self._state["sources"]:
                    self._state["sources"][source] = {}
                self._state["sources"][source]["watermark"] = wm
                self._state["sources"][source]["updated_at"] = datetime.utcnow().isoformat()
        self._pending.clear()
        self._save()

    def _save(self):
        """원자적 쓰기 + 백업"""
        with self.lock:
            # 기존 state 백업
            if self.path.exists():
                backup = self.path.with_suffix(".bak")
                shutil.copy2(self.path, backup)
            # 원자적 쓰기
            tmp = self.path.with_suffix(".tmp")
            tmp.write_text(json.dumps(self._state, indent=2))
            tmp.rename(self.path)

    def clear(self, source: str | None = None):
        if source:
            self._state["sources"].pop(source, None)
            self._pending.pop(source, None)
        else:
            self._state["sources"].clear()
            self._pending.clear()
        self._save()
```

### 2.5 Statistics

```python
# stats.py
from dataclasses import dataclass, field, asdict
from typing import Literal

@dataclass
class SourceStats:
    name: str
    status: Literal["success", "failed", "timeout", "partial"] = "success"
    records_fetched: int = 0
    records_after_normalize: int = 0
    fetch_time_ms: float = 0.0
    error: str | None = None

@dataclass
class PipelineStats:
    started_at: str = ""
    finished_at: str = ""
    total_time_ms: float = 0.0
    sources: list[SourceStats] = field(default_factory=list)
    total_records: int = 0
    duplicates_removed: int = 0

    @property
    def failed_sources(self) -> list[str]:
        return [s.name for s in self.sources if s.status in ("failed", "timeout")]

    @property
    def success_rate(self) -> float:
        if not self.sources:
            return 0.0
        ok = sum(1 for s in self.sources if s.status in ("success", "partial"))
        return ok / len(self.sources)

    def to_dict(self) -> dict:
        return {
            "started_at": self.started_at,
            "finished_at": self.finished_at,
            "total_time_ms": round(self.total_time_ms, 2),
            "total_records": self.total_records,
            "duplicates_removed": self.duplicates_removed,
            "success_rate": round(self.success_rate, 4),
            "failed_sources": self.failed_sources,
            "sources": [asdict(s) for s in self.sources],
        }
```

### 2.6 Normalizer (watermark 연동)

```python
# normalizer.py
from typing import Any

class Normalizer:
    """레코드 정규화 + watermark 계산"""

    def normalize(
        self,
        records: list[dict[str, Any]],
        source_config: "SourceConfig",
        state_store: "StateStore",
    ) -> list[dict[str, Any]]:
        normalized = []
        for rec in records:
            try:
                n = self._normalize_one(rec, source_config)
                normalized.append(n)
            except (KeyError, ValueError, TypeError):
                continue  # 스킵, 통계에서 차이로 추적

        # Watermark: normalize 직후 계산 & stage
        if source_config.incremental and normalized:
            wm_field = source_config.incremental_field
            watermark_values = [
                r[wm_field] for r in normalized
                if wm_field in r and r[wm_field] is not None
            ]
            if watermark_values:
                state_store.stage_watermark(
                    source_config.name,
                    str(max(watermark_values)),
                )

        return normalized

    def _normalize_one(self, rec: dict, config: "SourceConfig") -> dict:
        if config.field_mapping:
            return self._apply_mapping(rec, config.field_mapping)
        return {
            "source": config.name,
            "timestamp": rec.get("timestamp") or rec.get("time") or rec.get("date"),
            "value": float(rec.get("value") or rec.get("price") or rec.get("amount", 0)),
            "unit": rec.get("unit") or rec.get("currency") or "unknown",
        }

    def _apply_mapping(self, rec: dict, mappings: list) -> dict:
        result = {}
        for m in mappings:
            val = rec.get(m.source_field)
            if m.transform == "float":
                val = float(val) if val is not None else 0.0
            elif m.transform == "strip":
                val = str(val).strip() if val else ""
            elif m.transform == "isoformat":
                pass  # assume already ISO
            result[m.target_field] = val
        return result
```

### 2.7 Merger (안정 dedup hash)

```python
# merger.py
import json
import hashlib

class Merger:
    def merge(self, records: list[dict]) -> list[dict]:
        seen = set()
        deduped = []
        for r in records:
            key = self._stable_hash(r)
            if key not in seen:
                seen.add(key)
                deduped.append(r)
        # Sort by timestamp (stable)
        deduped.sort(key=lambda r: r.get("timestamp") or "")
        return deduped

    def _stable_hash(self, record: dict) -> str:
        """JSON 직렬화 → deterministic hash (dict/list 안전)"""
        canonical = json.dumps(record, sort_keys=True, default=str)
        return hashlib.md5(canonical.encode()).hexdigest()
```

### 2.8 Pipeline Orchestrator (수정된 타이밍)

```python
# pipeline.py
import asyncio
import json
import time
from datetime import datetime
from pathlib import Path

class Pipeline:
    def __init__(self, config: "PipelineConfig"):
        self.config = config
        self.state = StateStore(config.state_file)
        self.stats = PipelineStats()
        self.normalizer = Normalizer()
        self.merger = Merger()

    async def run(self) -> PipelineStats:
        self.stats.started_at = datetime.utcnow().isoformat()
        start = time.perf_counter()

        # 0. Output path validation
        validate_output_path(self.config.output, self.config.allowed_output_dirs)

        # 1. Pre-flight security: 모든 URL 연결 시점 검증
        for src in self.config.sources:
            if src.url:
                validate_url_at_connect(src.url, self.config.allowed_hosts or None)
            if src.path:
                validate_file_path(src.path, self.config.allowed_input_dirs)

        # 2. Concurrent fetch with SSRF-safe connector
        connector = create_ssrf_safe_connector()
        sem = asyncio.Semaphore(self.config.max_concurrent)
        results: list = []  # ← V1 bug fix: 명시적 할당

        async with aiohttp.ClientSession(connector=connector) as session:
            tasks = [
                self._fetch_source(session, src, sem)
                for src in self.config.sources
            ]
            try:
                async with asyncio.timeout(self.config.global_timeout):
                    results = await asyncio.gather(*tasks, return_exceptions=True)
            except TimeoutError:
                # Global timeout — gather가 이미 시작한 tasks의 결과 수집
                results = [TimeoutError("Global timeout")] * len(tasks)

        # 3. Process results + normalize (watermark 연동)
        all_records = []
        successful = set()
        partial = set()

        for src_cfg, result in zip(self.config.sources, results):
            stat = SourceStats(name=src_cfg.name)

            if isinstance(result, TimeoutError):
                stat.status = "timeout"
                stat.error = str(result)
            elif isinstance(result, Exception):
                stat.status = "failed"
                stat.error = str(result)
            elif isinstance(result, FetchResult):
                stat.records_fetched = len(result.records)
                stat.fetch_time_ms = result.fetch_time_ms

                if result.error:
                    stat.status = "partial"
                    stat.error = result.error
                    partial.add(src_cfg.name)
                elif result.is_partial:
                    stat.status = "partial"
                    partial.add(src_cfg.name)
                else:
                    successful.add(src_cfg.name)

                # Normalize + watermark staging
                normalized = self.normalizer.normalize(
                    result.records, src_cfg, self.state
                )
                stat.records_after_normalize = len(normalized)
                all_records.extend(normalized)

            self.stats.sources.append(stat)

        # 4. Merge + sort + dedup
        merged = self.merger.merge(all_records)
        self.stats.total_records = len(merged)
        self.stats.duplicates_removed = len(all_records) - len(merged)

        # 5. Finalize stats BEFORE output (타이밍 수정)
        self.stats.total_time_ms = (time.perf_counter() - start) * 1000
        self.stats.finished_at = datetime.utcnow().isoformat()

        # 6. Output (stats 완료 후 저장)
        output = {
            "records": merged,
            "stats": self.stats.to_dict(),
        }
        output_path = Path(self.config.output)
        output_path.write_text(json.dumps(output, indent=2, default=str))

        # 7. State commit — 성공/partial만, 파이프라인 완료 후
        self.state.commit(successful, partial)

        return self.stats

    async def _fetch_source(self, session, src_cfg, sem):
        async with sem:
            fetcher = create_fetcher(src_cfg, self.state)
            return await fetcher.fetch(session)
```

---

## 3. 보안 체크리스트

| 위협 | V1 | V2 | V2 Improved |
|------|----|----|-------------|
| SSRF (내부망) | ❌ | DNS resolve | ✅ **연결 시점 resolve + custom resolver로 redirect hop별 재검증** |
| SSRF 대역 | ❌ | 일부 | ✅ **0.0.0.0/8, 100.64/10, 198.18/15 등 추가** |
| Path Traversal | ❌ | resolve | ✅ **input + output 별도 allowlist** |
| GraphQL Injection | ❌ | 문자열 | ✅ **AST 파싱 우선, fallback으로 문자열 차단** |
| DNS Rebinding | ❌ | 사전 resolve | ✅ **커스텀 resolver로 매 연결마다 검증** |
| Output 경로 | ❌ | ❌ | ✅ **allowed_output_dirs allowlist** |
| State 손상 | ❌ | 원자쓰기 | ✅ **버전 + 백업 + 손상 감지 복구** |
| 다중 프로세스 | ❌ | FileLock | ✅ **FileLock + timeout** |

---

## 4. 테스트 전략 (Tier 분리)

### Tier 1: 회귀 핵심 (CI 필수, ~2시간)

반드시 통과해야 하는 핵심 테스트. 모든 PR에서 실행.

```python
# test_config.py — 15 cases
def test_valid_config_all_types(): ...
def test_invalid_missing_url_for_rest(): ...
def test_invalid_mutable_defaults_isolated(): ...
def test_model_validator_type_requirements(): ...
def test_extra_fields_forbidden(): ...
def test_field_constraints(): ...

# test_security.py — 20 cases
def test_ssrf_localhost_127(): ...
def test_ssrf_private_10(): ...
def test_ssrf_private_172(): ...
def test_ssrf_private_192(): ...
def test_ssrf_carrier_nat_100_64(): ...
def test_ssrf_ipv6_loopback(): ...
def test_ssrf_link_local_169_254(): ...
def test_ssrf_allowed_host_pass(): ...
def test_ssrf_redirect_revalidation(): ...        # ← 신규
def test_path_traversal_dotdot(): ...
def test_path_traversal_symlink(): ...
def test_path_output_allowlist(): ...              # ← 신규
def test_graphql_mutation_blocked(): ...
def test_graphql_introspection_blocked(): ...
def test_graphql_ast_parse_if_available(): ...     # ← 신규

# test_fetchers/ — 각 5 cases × 5 = 25 cases
async def test_rest_normal(): ...
async def test_rest_incremental(): ...
async def test_rest_http_error(): ...
async def test_csv_url(): ...
async def test_csv_file(): ...
async def test_ws_normal(): ...
async def test_ws_timeout(): ...
async def test_graphql_normal(): ...
async def test_graphql_error_response(): ...
async def test_file_json(): ...
async def test_file_xml(): ...

# test_normalizer.py — 10 cases
def test_normalize_default_mapping(): ...
def test_normalize_custom_mapping(): ...
def test_normalize_missing_field_skip(): ...
def test_normalize_watermark_staging(): ...        # ← 신규

# test_merger.py — 8 cases
def test_merge_sort_by_timestamp(): ...
def test_merge_dedup(): ...
def test_merge_stable_hash_nested(): ...           # ← 신규: dict/list values
def test_merge_empty(): ...

# test_state.py — 12 cases
def test_state_stage_and_commit(): ...
def test_state_failed_source_not_updated(): ...    # ← V1 회귀 방지
def test_state_partial_source_updated(): ...
def test_state_atomic_write(): ...
def test_state_corrupted_recovery(): ...           # ← 신규
def test_state_version_migration(): ...            # ← 신규
def test_state_concurrent_lock(): ...
```

### Tier 2: 통합 테스트 (~1시간)

```python
# test_pipeline.py — 8 cases
async def test_pipeline_all_sources_success(): ...
async def test_pipeline_partial_failure(): ...
async def test_pipeline_all_fail_graceful(): ...
async def test_pipeline_global_timeout(): ...      # ← 신규
async def test_pipeline_incremental_e2e(): ...
async def test_pipeline_output_contains_stats(): ...
async def test_pipeline_stats_timing_correct(): ... # ← finished_at 존재 확인
async def test_pipeline_output_path_blocked(): ...  # ← 신규
```

### Tier 3: 성능 / Nightly (~30분)

```python
# test_pipeline_perf.py
async def test_1000_records_under_15s(): ...
async def test_memory_under_100mb_for_10000_records(): ...
async def test_p95_fetch_time(): ...
```

### 테스트 인프라

```python
# conftest.py
import pytest
from aiohttp.test_utils import AioHTTPTestCase
from aiohttp import web

@pytest.fixture
async def mock_server(aiohttp_server):
    app = web.Application()
    app.router.add_get("/api/data", rest_handler)
    app.router.add_get("/api/data/incremental", rest_incremental_handler)
    app.router.add_get("/data.csv", csv_handler)
    app.router.add_get("/ws", websocket_handler)
    app.router.add_post("/graphql", graphql_handler)
    app.router.add_get("/redirect", redirect_handler)  # SSRF redirect 테스트
    server = await aiohttp_server(app)
    return server

@pytest.fixture
def tmp_state(tmp_path):
    return StateStore(str(tmp_path / ".state.json"))

@pytest.fixture
def sample_config(mock_server):
    return PipelineConfig(
        sources=[...],
        output=str(tmp_path / "output.json"),
        allowed_output_dirs=[str(tmp_path)],
    )
```

---

## 5. 의존성 (완성)

```toml
[project]
name = "market-monitor"
requires-python = ">=3.11"
dependencies = [
    "aiohttp>=3.9",
    "pydantic>=2.5",
    "pyyaml>=6.0",
    "orjson>=3.9",
    "filelock>=3.13",
]

[project.optional-dependencies]
graphql = [
    "graphql-core>=3.2",      # AST-based GraphQL validation
]
dev = [
    "pytest>=8.0",
    "pytest-asyncio>=0.23",
    "pytest-cov>=4.1",
    "aiohttp[speedups]",
]
```

> `websockets`는 불필요 (aiohttp 내장 ws_connect 사용).
> `aiofiles`는 불필요 (sync file I/O로 충분, fetcher 내 blocking은 to_thread로 처리 가능).
> `graphql-core`는 optional extra로 분리.

---

## 6. 디렉토리 구조 (최종)

```
market_monitor/
├── __init__.py
├── __main__.py              # CLI: python -m market_monitor config.yaml
├── config.py                # Pydantic v2 models + YAML/JSON loader
├── security.py              # SSRF, path, GraphQL validation
├── fetchers/
│   ├── __init__.py          # create_fetcher()
│   ├── base.py              # FetchResult, BaseFetcher
│   ├── rest.py
│   ├── csv_fetcher.py
│   ├── websocket.py
│   ├── graphql.py
│   └── file.py
├── normalizer.py            # normalize + watermark staging
├── merger.py                # merge + stable dedup
├── state.py                 # Incremental state management
├── stats.py                 # Pipeline statistics
└── pipeline.py              # Orchestrator

tests/
├── conftest.py              # Fixtures, mock server
├── test_config.py           # Tier 1
├── test_security.py         # Tier 1
├── test_fetchers/           # Tier 1
│   ├── test_rest.py
│   ├── test_csv.py
│   ├── test_websocket.py
│   ├── test_graphql.py
│   └── test_file.py
├── test_normalizer.py       # Tier 1
├── test_merger.py           # Tier 1
├── test_state.py            # Tier 1
├── test_pipeline.py         # Tier 2
└── test_pipeline_perf.py    # Tier 3
```

---

## 7. 구현 일정 (현실적)

| Phase | 작업 | 예상 시간 | 산출물 |
|-------|------|-----------|--------|
| 1 | config.py + security.py + Tier1 테스트 | 1.5h | 검증된 설정/보안 모듈 |
| 2 | 5개 fetchers + mock server + Tier1 테스트 | 2h | 모든 fetcher 동작 확인 |
| 3 | normalizer + merger + state + Tier1 테스트 | 1.5h | 데이터 파이프라인 완성 |
| 4 | pipeline.py + stats.py + Tier2 통합 테스트 | 1.5h | E2E 동작 확인 |
| 5 | CLI, 문서, Tier3 성능, 최종 검증 | 1h | 제출 준비 완료 |
| **합계** | | **7.5h** | |

---

## 8. V1 → V2 → V2 Improved 변경 추적

| 영역 | V1 | V2 | V2 Improved |
|------|----|----|-------------|
| 아키텍처 | 단일 파일 | 전면 재구성 | **점진적 리팩터링** |
| 런타임 결함 | 다수 | 일부 잔존 | **전부 수정 (results, FetchResult, asdict, 타이밍)** |
| SSRF | 없음 | DNS resolve | **연결시점 + redirect hop별 재검증** |
| GraphQL | 없음 | 문자열 차단 | **AST 파싱 우선 + fallback** |
| Output 경로 | 없음 | 없음 | **allowlist 검증** |
| Incremental | 가짜 | 성공후 save | **source별 watermark staging + 상태별 갱신 정책** |
| State 관리 | 없음 | 원자쓰기 | **버전/백업/손상복구/다중프로세스** |
| Pydantic | v1 | v2 부분 | **v2 완전호환 (default_factory, ConfigDict, model_validator)** |
| 테스트 | ~20% | 90% 목표 | **Tier1/2/3 분리, 현실적 일정** |
| 의존성 | 불완전 | 불완전 | **완성 (optional extras 포함)** |
| Config | 혼재 | 혼재 | **YAML 통일, JSON fallback** |
| 일정 | 2h | 3-4h | **7.5h (현실적)** |
