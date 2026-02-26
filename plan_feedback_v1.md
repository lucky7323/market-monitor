# PLAN_V2 리뷰 (실구현 관점) - Codex 5.3 검토

## 1. 아키텍처 실용성/구현 가능성 ⚠️

**문제점:**
- **전면 재구성 리스크**: `market_monitor/*` vs 현재 `pipeline/*` 간극이 커서 위험
- **런타임 결함**: `results` 미정의, `FetchResult` 생성자 불명확, `asdict` import 누락
- **출력 타이밍**: 통계 완료 전 저장으로 `finished_at/total_time_ms` 누락 가능

**개선안:**
- 점진적 리팩터링으로 위험 최소화
- 코드 예시 런타임 검증 필수
- 통계 수집 완료 후 출력 저장

## 2. 보안 구현 현실성 🔴

**과도한 부분:**
- GraphQL 문자열 차단: 우회/오탐 가능성 높음

**부족한 부분:**
- SSRF 차단 대역 일부 누락
- Redirect 재검증 없음  
- DNS rebinding 불충분 (사전 resolve만)

**개선안:**
- 연결 시점 IP 검증 (커스텀 resolver)
- Redirect hop별 재검증
- Output path allowlist 검증

## 3. Incremental updates 동작 가능성 ⚠️

**문제점:**
- Partial/timeout 시 state 갱신 정책 불명확
- `merged` 기준 watermark → dedup/sort 영향으로 왜곡
- `incremental_field` 누락 레코드 처리 빠짐
- 손상 state 복구, 다중 프로세스 충돌 정책 없음

**개선안:**
- Source별 normalize 직후 `max(watermark)` 저장
- Success/partial/failed별 갱신 정책 명시
- State 버전/백업 추가

## 4. 테스트 전략 달성 가능성 ⚠️

**문제점:**
- 목표(90%+) 대비 3~4시간은 비현실적
- 핵심 테스트 누락: global timeout, redirect SSRF, corrupted state 복구

**개선안:**
- Tier1(회귀 핵심) / Tier2(통합) / Tier3(성능·nightly) 분리
- 시간 현실적으로 재조정

## 5. 성능 최적화 효과성 ✅

**유효한 부분:**
- `Semaphore + gather` 조합 적절

**문제점:**
- CSV chunk 처리 계획과 구현 불일치
- Dedup 키에 dict/list 시 hash 문제
- `orjson` 효과 제한적 (네트워크 병목시)

**개선안:**
- Line streaming 구현
- 안정 직렬화 후 dedup hash
- p50/p95 + 메모리 상한 성능 지표

## 6. Pydantic v2 마이그레이션 🔴

**필수 수정사항:**
- Mutable default(`{}`, `[]`) → `Field(default_factory=...)`
- `ConfigDict` import + `extra="forbid"` 적용
- `.dict()` → `.model_dump()` 전환
- 타입별 필수값을 `model_validator(mode="after")` 강제
- `variables: dict[str, str]` → `dict[str, Any]`

## 추가 누락 사항

- **의존성**: `websockets`, `aiofiles`, `tenacity` 누락
- **포맷 불일치**: `config.yaml` vs JSON 기반 로더

## 종합 평가

**⚠️ 중대한 이슈들 발견:**
- 보안 구현 불완전
- Incremental updates 설계 결함
- Pydantic v2 호환성 문제
- 비현실적 일정

**✅ 좋은 방향:**
- 아키텍처 방향성 양호
- 성능 최적화 전략 유효

**다음 단계**: 이슈들 반영한 개선 계획 필요