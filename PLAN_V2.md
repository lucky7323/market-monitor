# Market Monitor V2 — 종합 개발 계획서

## 1. 아키텍처 개요

```
config.yaml
    │
    ▼
┌─────────────┐     ┌──────────────┐     ┌────────────┐     ┌──────────┐
│ ConfigLoader │────▶│ SourceRunner │────▶│ Normalizer │────▶│  Merger   │
│  (Pydantic)  │     │ (asyncio)    │     │            │     │ sort+json │
└─────────────┘     └──────────────┘     └────────────┘     └──────────┘
                           │                                       │
                    ┌──────┴──────┐                          ┌─────▼─────┐
                    │ Fetchers x5 │                          │ StatsCollector │
                    │ REST/CSV/WS │                          │ output.json    │
                    │ GraphQL/File│                          └───────────┘
                    └─────────────┘
                           │
                    ┌──────▼──────┐
                    │ StateStore  │  ← Incremental updates
                    │ (.state.json)│
                    └─────────────┘
```

## 2. 디렉토리 구조

```
market_monitor/
├── __init__.py
├── __main__.py              # CLI entrypoint
├── config.py                # Pydantic v2 config models
├── security.py              # URL/path validation, sanitization
├── fetchers/
│   ├── __init__.py
│   ├── base.py              # BaseFetcher ABC
│   ├── rest.py              # REST API fetcher
│   ├── csv_fetcher.py       # CSV fetcher (file/URL)
│   ├── websocket.py         # WebSocket fetcher
│   ├── graphql.py           # GraphQL fetcher
│   └── file.py              # Local file fetcher (JSON/XML)
├── normalizer.py            # → {source, timestamp, value, unit}
├── merger.py                # Merge + sort + dedup
├── state.py                 # Incremental state management
├── stats.py                 # Pipeline statistics
└── pipeline.py              # Orchestrator

tests/
├── conftest.py              # Shared fixtures, mock server
├── mock_server.py           # aiohttp test server (all 5 types)
├── test_config.py
├── test_security.py
├── test_fetchers/
│   ├── test_rest.py
│   ├── test_csv.py
│   ├── test_websocket.py
│   ├── test_graphql.py
│   └── test_file.py
├── test_normalizer.py
├── test_merger.py
├── test_state.py
├── test_stats.py
├── test_pipeline.py         # Integration tests
└── test_pipeline_perf.py    # Performance (<15s for 1000+ records)
```

## 3. 핵심 클래스 설계

### 3.1 Config (Pydantic v2)

```python
from pydantic import BaseModel, Field, field_validator, model_validator
from typing import Literal
from datetime import timedelta

class FieldMapping(BaseModel):
    """Custom field mapping for bonus feature"""
    source_field: str
    target_field: Literal["source", "timestamp", "value", "unit"]
    transform: str | None = None  # e.g., "float", "isoformat", "strip"

class SourceConfig(BaseModel):
    model_config = ConfigDict(strict=True)
    
    name: str = Field(min_length=1, max_length=100, pattern=r'^[a-zA-Z0-9_-]+$')
    type: Literal["rest", "csv", "websocket", "graphql", "file"]
    url: str | None = None          # for rest, ws, graphql
    path: str | None = None         # for file, csv (local)
    timeout: float = Field(default=10.0, gt=0, le=60)
    
    # Type-specific
    headers: dict[str, str] = {}
    query: str | None = None        # GraphQL query (parameterized only)
    variables: dict[str, str] = {}  # GraphQL variables
    ws_duration: float = Field(default=5.0, gt=0, le=30)
    
    # Custom field mapping (bonus)
    field_mapping: list[FieldMapping] = []
    
    # Incremental
    incremental: bool = False
    incremental_field: str = "timestamp"  # field to track last seen

    @field_validator('url')
    @classmethod
    def validate_url(cls, v):
        # Delegated to security.py
        if v is not None:
            from .security import validate_url
            validate_url(v)
        return v

    @field_validator('path')
    @classmethod
    def validate_path(cls, v):
        if v is not None:
            from .security import validate_file_path
            validate_file_path(v)
        return v

class PipelineConfig(BaseModel):
    sources: list[SourceConfig] = Field(min_length=1)
    output: str = "output.json"
    max_concurrent: int = Field(default=10, ge=1, le=50)
    global_timeout: float = Field(default=15.0, gt=0, le=300)
    allowed_hosts: list[str] = []       # whitelist for SSRF protection
    allowed_paths: list[str] = ["."]    # whitelist for path traversal protection
    state_file: str = ".state.json"
```

### 3.2 Security Module

```python
# security.py — V1 실패 요인 모두 해결

import ipaddress
from urllib.parse import urlparse
from pathlib import Path

# SSRF Protection
BLOCKED_RANGES = [
    ipaddress.ip_network("127.0.0.0/8"),
    ipaddress.ip_network("10.0.0.0/8"),
    ipaddress.ip_network("172.16.0.0/12"),
    ipaddress.ip_network("192.168.0.0/16"),
    ipaddress.ip_network("169.254.0.0/16"),
    ipaddress.ip_network("::1/128"),
    ipaddress.ip_network("fc00::/7"),
]

def validate_url(url: str, allowed_hosts: list[str] | None = None) -> None:
    """SSRF 방지: private IP, localhost, 허용되지 않은 호스트 차단"""
    parsed = urlparse(url)
    if parsed.scheme not in ("http", "https", "ws", "wss"):
        raise ValueError(f"Disallowed scheme: {parsed.scheme}")
    
    hostname = parsed.hostname
    if not hostname:
        raise ValueError("Missing hostname")
    
    # DNS rebinding 방지: 실제 resolve된 IP 체크
    import socket
    try:
        resolved = socket.getaddrinfo(hostname, None)
        for _, _, _, _, addr in resolved:
            ip = ipaddress.ip_address(addr[0])
            for blocked in BLOCKED_RANGES:
                if ip in blocked:
                    raise ValueError(f"Blocked IP range: {ip}")
    except socket.gaierror:
        raise ValueError(f"Cannot resolve: {hostname}")
    
    if allowed_hosts and hostname not in allowed_hosts:
        raise ValueError(f"Host not in allowlist: {hostname}")

def validate_file_path(path: str, allowed_dirs: list[str] | None = None) -> Path:
    """Path traversal 방지: resolve 후 allowed_dirs 내에 있는지 확인"""
    resolved = Path(path).resolve()
    
    if not allowed_dirs:
        allowed_dirs = ["."]
    
    for allowed in allowed_dirs:
        allowed_resolved = Path(allowed).resolve()
        if resolved.is_relative_to(allowed_resolved):
            return resolved
    
    raise ValueError(f"Path outside allowed directories: {path}")

def sanitize_graphql_query(query: str) -> str:
    """GraphQL injection 방지: introspection 차단, mutation 차단"""
    normalized = query.strip().lower()
    
    if "mutation" in normalized or "subscription" in normalized:
        raise ValueError("Only queries allowed")
    if "__schema" in normalized or "__type" in normalized:
        raise ValueError("Introspection queries blocked")
    # Variables만 허용, string interpolation 금지
    if "${" in query or "#{" in query:
        raise ValueError("String interpolation not allowed; use variables")
    
    return query
```

### 3.3 Fetchers

```python
# fetchers/base.py
from abc import ABC, abstractmethod
from typing import Any

class FetchResult:
    """Fetcher 결과 + 메타데이터"""
    records: list[dict[str, Any]]
    source_name: str
    fetch_time_ms: float
    error: str | None = None
    is_partial: bool = False  # timeout으로 일부만 받은 경우

class BaseFetcher(ABC):
    def __init__(self, config: SourceConfig, state_store: StateStore):
        self.config = config
        self.state = state_store
    
    @abstractmethod
    async def fetch(self, session: aiohttp.ClientSession) -> FetchResult:
        ...
    
    def get_incremental_filter(self) -> Any | None:
        """마지막으로 처리한 레코드의 기준값 반환"""
        if not self.config.incremental:
            return None
        return self.state.get_last_value(self.config.name, self.config.incremental_field)

# fetchers/rest.py
class RestFetcher(BaseFetcher):
    async def fetch(self, session) -> FetchResult:
        last = self.get_incremental_filter()
        params = {}
        if last:
            # Incremental: since 파라미터 추가
            params["since"] = last
        
        async with session.get(
            self.config.url, 
            headers=self.config.headers,
            params=params,
            timeout=aiohttp.ClientTimeout(total=self.config.timeout)
        ) as resp:
            resp.raise_for_status()
            data = await resp.json()
            records = data if isinstance(data, list) else data.get("data", [data])
            return FetchResult(records=records, source_name=self.config.name, ...)

# fetchers/websocket.py  
class WebSocketFetcher(BaseFetcher):
    async def fetch(self, session) -> FetchResult:
        records = []
        try:
            async with asyncio.timeout(self.config.ws_duration):
                async with session.ws_connect(self.config.url) as ws:
                    async for msg in ws:
                        if msg.type == aiohttp.WSMsgType.TEXT:
                            records.append(json.loads(msg.data))
                        elif msg.type in (aiohttp.WSMsgType.ERROR, aiohttp.WSMsgType.CLOSED):
                            break
        except TimeoutError:
            pass  # Normal: ws_duration reached
        return FetchResult(records=records, source_name=self.config.name, ...)

# fetchers/graphql.py
class GraphQLFetcher(BaseFetcher):
    async def fetch(self, session) -> FetchResult:
        query = sanitize_graphql_query(self.config.query)
        variables = dict(self.config.variables)
        
        last = self.get_incremental_filter()
        if last:
            variables["since"] = last
        
        payload = {"query": query, "variables": variables}
        async with session.post(
            self.config.url,
            json=payload,
            timeout=aiohttp.ClientTimeout(total=self.config.timeout)
        ) as resp:
            resp.raise_for_status()
            data = await resp.json()
            if "errors" in data:
                raise ValueError(f"GraphQL errors: {data['errors']}")
            # Extract records from data path
            records = self._extract_records(data["data"])
            return FetchResult(records=records, ...)
```

### 3.4 Incremental State (V1에서 실패했던 핵심)

```python
# state.py
import json
from pathlib import Path
from filelock import FileLock

class StateStore:
    """파일 기반 incremental state — 원자적 쓰기, 동시성 안전"""
    
    def __init__(self, path: str = ".state.json"):
        self.path = Path(path)
        self.lock = FileLock(f"{path}.lock")
        self._state: dict = {}
        self._load()
    
    def _load(self):
        if self.path.exists():
            with self.lock:
                self._state = json.loads(self.path.read_text())
    
    def get_last_value(self, source: str, field: str) -> str | None:
        return self._state.get(source, {}).get(f"last_{field}")
    
    def update(self, source: str, field: str, value: str):
        """파이프라인 성공 후에만 호출 — 실패 시 state 오염 방지"""
        if source not in self._state:
            self._state[source] = {}
        self._state[source][f"last_{field}"] = value
    
    def save(self):
        """원자적 쓰기: temp → rename"""
        with self.lock:
            tmp = self.path.with_suffix(".tmp")
            tmp.write_text(json.dumps(self._state, indent=2))
            tmp.rename(self.path)
    
    def clear(self, source: str | None = None):
        if source:
            self._state.pop(source, None)
        else:
            self._state.clear()
        self.save()
```

**V1 실패 원인과 해결:**
- V1: state를 fetch 전에 업데이트 → 실패해도 "이미 봤다"로 처리됨
- V2: **파이프라인 전체 성공 후에만** `state.save()` 호출 (트랜잭션 패턴)

### 3.5 Statistics

```python
# stats.py
from dataclasses import dataclass, field
from time import perf_counter

@dataclass
class SourceStats:
    name: str
    status: Literal["success", "failed", "timeout", "partial"] = "success"
    records_fetched: int = 0
    records_after_normalize: int = 0
    fetch_time_ms: float = 0
    error: str | None = None

@dataclass  
class PipelineStats:
    started_at: str = ""
    finished_at: str = ""
    total_time_ms: float = 0
    sources: list[SourceStats] = field(default_factory=list)
    total_records: int = 0
    duplicates_removed: int = 0
    
    # V1에서 빠졌던 실패 추적
    @property
    def failed_sources(self) -> list[str]:
        return [s.name for s in self.sources if s.status != "success"]
    
    @property
    def success_rate(self) -> float:
        if not self.sources:
            return 0.0
        return sum(1 for s in self.sources if s.status == "success") / len(self.sources)
    
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

### 3.6 Pipeline Orchestrator

```python
# pipeline.py
class Pipeline:
    def __init__(self, config: PipelineConfig):
        self.config = config
        self.state = StateStore(config.state_file)
        self.stats = PipelineStats()
        self.normalizer = Normalizer()
        self.merger = Merger()
    
    async def run(self) -> PipelineStats:
        self.stats.started_at = datetime.utcnow().isoformat()
        start = perf_counter()
        
        # 1. Security validation (모든 URL/path 사전 검증)
        for src in self.config.sources:
            if src.url:
                validate_url(src.url, self.config.allowed_hosts or None)
            if src.path:
                validate_file_path(src.path, self.config.allowed_paths)
        
        # 2. Concurrent fetch with global timeout
        all_records = []
        sem = asyncio.Semaphore(self.config.max_concurrent)
        
        async with aiohttp.ClientSession() as session:
            try:
                async with asyncio.timeout(self.config.global_timeout):
                    tasks = [
                        self._fetch_source(session, src, sem)
                        for src in self.config.sources
                    ]
                    results = await asyncio.gather(*tasks, return_exceptions=True)
            except TimeoutError:
                # Global timeout — 이미 완료된 결과는 유지
                pass
        
        # 3. Process results
        for src_cfg, result in zip(self.config.sources, results):
            stat = SourceStats(name=src_cfg.name)
            if isinstance(result, Exception):
                stat.status = "failed"
                stat.error = str(result)
            elif isinstance(result, FetchResult):
                stat.records_fetched = len(result.records)
                stat.fetch_time_ms = result.fetch_time_ms
                if result.error:
                    stat.status = "partial"
                    stat.error = result.error
                
                # 4. Normalize
                normalized = self.normalizer.normalize(
                    result.records, src_cfg
                )
                stat.records_after_normalize = len(normalized)
                all_records.extend(normalized)
            
            self.stats.sources.append(stat)
        
        # 5. Merge + sort + dedup
        merged = self.merger.merge(all_records)
        self.stats.total_records = len(merged)
        self.stats.duplicates_removed = len(all_records) - len(merged)
        
        # 6. Output
        output = {
            "records": merged,
            "stats": self.stats.to_dict(),
        }
        Path(self.config.output).write_text(
            json.dumps(output, indent=2, default=str)
        )
        
        # 7. State update — 성공한 소스만, 파이프라인 완료 후
        for src_cfg in self.config.sources:
            src_stat = next(s for s in self.stats.sources if s.name == src_cfg.name)
            if src_stat.status == "success" and src_cfg.incremental:
                last_record = self._get_last_record(merged, src_cfg.name)
                if last_record:
                    self.state.update(
                        src_cfg.name,
                        src_cfg.incremental_field,
                        last_record[src_cfg.incremental_field]
                    )
        self.state.save()
        
        self.stats.total_time_ms = (perf_counter() - start) * 1000
        self.stats.finished_at = datetime.utcnow().isoformat()
        return self.stats
    
    async def _fetch_source(self, session, src_cfg, sem):
        async with sem:
            fetcher = create_fetcher(src_cfg, self.state)
            return await fetcher.fetch(session)
```

## 4. 보안 체크리스트 ✅

| 위협 | V1 상태 | V2 대응 |
|------|---------|---------|
| SSRF (내부망 접근) | ❌ 미검증 | ✅ DNS resolve → IP range 차단 + allowlist |
| Path Traversal | ❌ 미검증 | ✅ `Path.resolve()` + `is_relative_to()` |
| GraphQL Injection | ❌ 미검증 | ✅ mutation/introspection 차단, variables만 사용 |
| DNS Rebinding | ❌ 미검증 | ✅ resolve된 실제 IP 검사 |
| Timeout abuse | ❌ 부분적 | ✅ per-source + global timeout |
| State corruption | ❌ fetch 전 업데이트 | ✅ 파이프라인 성공 후만 save, 원자적 쓰기 |
| Config injection | ❌ 미검증 | ✅ Pydantic strict mode, regex patterns |
| Memory DoS | ❌ 미검증 | ✅ streaming 처리, max record limits |

## 5. 테스트 전략 (목표: 90%+ 커버리지)

### 5.1 Mock Server (`tests/mock_server.py`)

```python
# aiohttp.test_utils 기반 — 5가지 소스 타입 모두 지원
@pytest.fixture
async def mock_server(aiohttp_server):
    app = web.Application()
    app.router.add_get("/api/data", rest_handler)        # REST
    app.router.add_get("/data.csv", csv_handler)          # CSV
    app.router.add_get("/ws", websocket_handler)          # WebSocket
    app.router.add_post("/graphql", graphql_handler)      # GraphQL
    # File: tmpdir fixture로 로컬 파일 생성
    server = await aiohttp_server(app)
    return server
```

### 5.2 테스트 매트릭스

| 모듈 | 단위 테스트 | 통합 테스트 | 에러 테스트 |
|------|------------|------------|------------|
| config.py | 유효/무효 config 15+ | - | 잘못된 타입, 범위 초과 |
| security.py | SSRF 10+, path 8+, gql 6+ | - | 모든 차단 케이스 |
| fetchers/* | 각 fetcher 정상 5+ | mock server 연동 | timeout, 4xx, 5xx, malformed |
| normalizer.py | 필드매핑 10+, 커스텀 5+ | - | 누락 필드, 잘못된 타입 |
| merger.py | sort 3+, dedup 3+ | - | 빈 입력, 단일 소스 |
| state.py | CRUD 8+, 원자성 3+ | 동시 접근 | 손상된 파일, 락 충돌 |
| stats.py | 계산 5+ | - | 빈 결과, 전체 실패 |
| pipeline.py | - | 전체 파이프라인 5+ | 부분 실패, 전체 timeout |
| **성능** | - | 1000+ records <15s | - |

### 5.3 특별 테스트

```python
# 보안 테스트
def test_ssrf_localhost(): ...
def test_ssrf_private_ip(): ...
def test_ssrf_ipv6_bypass(): ...
def test_path_traversal_dotdot(): ...
def test_path_traversal_symlink(): ...
def test_graphql_mutation_blocked(): ...
def test_graphql_introspection_blocked(): ...

# Incremental 테스트 (V1 실패 재현 방지)
async def test_incremental_state_not_updated_on_failure(): ...
async def test_incremental_fetches_only_new_records(): ...
async def test_incremental_state_atomic_write(): ...
async def test_incremental_first_run_fetches_all(): ...

# 성능 테스트
async def test_pipeline_1000_records_under_15s(): ...
async def test_memory_usage_10000_records(): ...
```

## 6. 성능 최적화

| 영역 | 방법 |
|------|------|
| 동시성 | `asyncio.gather` + `Semaphore` (max_concurrent 제한) |
| 메모리 | CSV는 chunk 단위 읽기, WS는 버퍼 제한 |
| JSON 출력 | `orjson` 사용 (3-10x 빠른 직렬화) |
| Timeout | per-source + global 이중 timeout |
| Dedup | `set` 기반 O(n) 중복 제거 (hash of source+timestamp+value) |

## 7. 의존성

```toml
[project]
requires-python = ">=3.11"
dependencies = [
    "aiohttp>=3.9",
    "pydantic>=2.0",
    "orjson>=3.9",
    "filelock>=3.13",
    "pyyaml>=6.0",
]

[project.optional-dependencies]
dev = [
    "pytest>=8.0",
    "pytest-asyncio>=0.23",
    "pytest-cov>=4.1",
    "aiohttp[speedups]",
]
```

## 8. 구현 순서 (예상 3-4시간)

| Phase | 작업 | 시간 |
|-------|------|------|
| 1 | `config.py` + `security.py` + 테스트 | 40분 |
| 2 | 5개 fetchers + mock server + 테스트 | 60분 |
| 3 | `normalizer.py` + `merger.py` + 테스트 | 30분 |
| 4 | `state.py` (incremental) + 테스트 | 30분 |
| 5 | `stats.py` + `pipeline.py` + 통합 테스트 | 40분 |
| 6 | 성능 테스트 + 최적화 | 20분 |
| 7 | 문서, CLI, 최종 검증 | 20분 |

## 9. V1→V2 핵심 변경 요약

1. **보안**: 사후 검증 → **사전 검증** (fetch 전 모든 URL/path/query 검증)
2. **Incremental**: 가짜 구현 → **트랜잭션 패턴** (성공 후에만 state 저장)
3. **통계**: 성공만 추적 → **실패/timeout/partial 모두 추적**
4. **테스트**: ~20% → **90%+ 커버리지** (보안/에러/성능 전용 테스트)
5. **Pydantic**: v1 호환 → **v2 전용** (ConfigDict, field_validator, model_validator)
6. **에러 처리**: 전체 실패 → **부분 성공** (graceful degradation)
