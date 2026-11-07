# demo-app: 이전 대상 데모 채팅 앱

AI 에이전트들이 계속 대화하는 채팅방입니다. 메시지마다 **처리한 앱의 환경(env-a, env-b)과 저장된 DB(pg-a, pg-b)**를 함께 기록하므로, 이전 과정이 채팅 기록에 그대로 남습니다.

이 저장소는 관리 플랫폼([cloud-migration-platform](https://github.com/SoftBank-Hackathon-2026-Team-Jackal/cloud-migration-platform))이 다루는 **고객의 앱**이라는 설정입니다. 플랫폼은 이 저장소를 스캔하고, 이전 대상 환경에 맞춘 코드 수정을 이 저장소에 PR로 보냅니다. 이전 리허설 환경(CDC PoC, 앱 2개, HAProxy)은 [Finals 저장소의 `poc/demo`](https://github.com/SoftBank-Hackathon-2026-Team-Jackal/Softbank-Hackathon-2026-Finals/tree/main/poc/demo)에 있습니다.

## 구성

| 경로 | 내용 |
|---|---|
| `fault.go` | 이전 시연용 장애 스위치. 켜면 `/api/` 요청에 지연과 500을 낸다 |
| `main.go` | HTTP 서버. `POST /api/messages`, `GET /api/stream`(SSE), `GET /api/messages`, `POST /api/verify`, `GET /api/stats`, `GET /healthz` |
| `store.go` | pgx (PgBouncer transaction 모드에 맞춰 simple protocol 사용), 시작 시 스키마 적용 |
| `hub.go` | DB를 150ms마다 조회해 새 메시지를 SSE 구독자에게 전달 |
| `db/schema.sql` | 채팅 스키마 (바이너리에 embed) |
| `static/` | 채팅 화면 (빌드 단계 없는 순수 JS, 바이너리에 embed) |
| `agents/` | 대화하는 AI 에이전트 (Python, Anthropic SDK) |
| `.github/workflows/image.yml` | `main`에 push하면 `ko`로 amd64·arm64 이미지를 GHCR에 올림 |

## 개발 환경에서 실행

```bash
docker compose up -d --build
open http://localhost:8080
```

에이전트:

```bash
cd agents
echo 'ANTHROPIC_API_KEY=sk-ant-...' > .env   # Git에서 제외됨
uv run --env-file .env chatbots.py --url http://localhost:8080
# API 키 없이 준비된 대사로만 돌리려면 --offline
```

모델 기본값은 `claude-haiku-5-5`이고 `CHAT_MODEL` 환경 변수로 바꿀 수 있습니다. 4~5초에 한 마디씩 말하면 시간당 비용은 약 $0.2입니다. 에이전트를 Ctrl-C로 종료하면 보낸 메시지, 저장 확인, 스트림 수신, 유실, 중복 집계가 출력됩니다.

## 설정

| 환경 변수 | 기본값 | 설명 |
|---|---|---|
| `DATABASE_URL` | (필수) | Postgres 접속 주소. 운영에서는 PgBouncer를 가리킨다 |
| `ENV_NAME` | `local` | 메시지에 기록되는 앱 환경 이름 (env-a, env-b 등) |
| `ADDR` | `:8080` | 수신 주소 |
| `POLL_INTERVAL_MS` | `150` | 새 메시지 조회 주기 |
| `DRAIN_DELAY` | `3s` | 종료 신호를 받은 뒤 로드 밸런서가 알아챌 때까지 기다리는 시간 |
| `FAULT_TOKEN` | (비어 있음) | 장애 스위치(`/__cmp/fault`) 토큰. 비어 있으면 스위치가 없다(404) |

DB 서버에 `app.node_name` 설정이 있으면 메시지의 `db_node`에 그 값이 기록됩니다 (예: `ALTER DATABASE app SET app.node_name = 'pg-a'`). 이 값은 앱이 아니라 DB 운영 쪽에서 서버마다 정합니다.

## 장애 스위치 (이전 시연용)

이전 뒤 안정화 구간에서 "새 환경의 앱이 오류를 낸다"는 상황을 일부러 만들 때 씁니다. 플랫폼이 원본과 대상의 오류율·p99를 비교해 알리고, 사람이 롤백을 승인하는 흐름을 시연합니다.

- 켜면 `/api/` 요청(실시간 스트림 `/api/stream` 제외)에 `delay_ms`만큼 지연을 더하고, `error_rate` 비율만큼 500을 냅니다.
- `/healthz`는 그대로 정상입니다. 헬스 체크는 통과하는데 사용자는 오류를 받는 상황이라 지표로만 알아챌 수 있습니다.
- `FAULT_TOKEN`을 설정해야 스위치가 생기고, 요청에 같은 값의 `X-Fault-Token` 헤더가 있어야 합니다. 플랫폼 중계 서버(Envoy)는 공개 경로의 `/__cmp/`를 403으로 막으므로, 스위치는 대상 앱에 직접 붙어서만 켤 수 있습니다.
- 켠 장애는 `duration_s`(기본 300초, 최대 900초)가 지나면 저절로 꺼집니다. 기본값은 꺼짐이라 설정하지 않으면 동작이 바뀌지 않습니다.

```bash
# 켜기: 요청의 50%에 500, 모든 요청에 800ms 지연, 2분 뒤 자동 해제
curl -X POST -H "X-Fault-Token: $FAULT_TOKEN" http://<대상 앱>/__cmp/fault \
  -d '{"error_rate":0.5,"delay_ms":800,"duration_s":120}'
curl -H "X-Fault-Token: $FAULT_TOKEN" http://<대상 앱>/__cmp/fault             # 상태
curl -X DELETE -H "X-Fault-Token: $FAULT_TOKEN" http://<대상 앱>/__cmp/fault   # 바로 끄기
```

## 이미지

```bash
# CI가 main push마다 올린다
docker pull ghcr.io/softbank-hackathon-2026-team-jackal/demo-app:latest

# 직접 올리려면
brew install go ko
KO_DOCKER_REPO=ghcr.io/softbank-hackathon-2026-team-jackal/demo-app ko build --bare .
```

## 설계 메모

- **id 순서와 커밋 순서를 맞춥니다.** 동시에 들어온 INSERT는 id 순서와 커밋 순서가 어긋날 수 있고, 그러면 `id > 마지막 id` 조회가 늦게 커밋된 작은 id를 건너뜁니다. 그래서 INSERT를 advisory lock으로 직렬화합니다.
- **유실 판단은 `client_msg_id`로 합니다.** 시퀀스는 중복 거부나 롤백에도 번호를 소비하므로 id가 건너뛴 것을 유실로 보면 안 됩니다.
- **헬스 체크는 DB를 확인하지 않습니다.** DB 전환 중 PgBouncer PAUSE 동안 헬스 체크까지 실패하면 로드 밸런서가 모든 인스턴스를 내립니다.
- **종료 순서:** SIGTERM → 헬스 체크 503 → SSE 연결 종료(브라우저가 다른 인스턴스로 재접속) → `DRAIN_DELAY` 대기 → 진행 중인 요청 완료 후 종료.
- **`LISTEN/NOTIFY`를 쓰지 않습니다.** PgBouncer transaction 모드에서 동작하지 않고 논리 복제로도 전달되지 않으므로, 공유 DB를 조회하는 방식을 메시지 버스로 씁니다.
- **스키마는 앱이 적용합니다.** `CREATE TABLE IF NOT EXISTS`는 테이블이 이미 있으면 카탈로그를 바꾸지 않으므로, DDL을 금지하는 이전 구간에 앱이 재시작되어도 안전합니다.
