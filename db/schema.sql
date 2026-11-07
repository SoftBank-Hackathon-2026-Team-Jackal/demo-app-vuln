-- 데모 채팅 앱 스키마. 앱이 시작할 때 적용한다 (store.go의 Migrate).
-- 여러 번 실행해도 안전해야 한다: 이미 있으면 아무것도 바꾸지 않는다.
CREATE TABLE IF NOT EXISTS chat_messages (
  id            bigserial PRIMARY KEY,
  -- 클라이언트가 만든 ID. 재전송해도 한 번만 저장되고, 유실 검증의 기준이 된다
  client_msg_id text NOT NULL UNIQUE CHECK (length(client_msg_id) BETWEEN 8 AND 64),
  author        text NOT NULL,
  body          text NOT NULL,
  -- 이 메시지를 처리한 앱의 환경 (env-a, env-b)
  app_env       text NOT NULL,
  -- 이 메시지가 처음 저장된 DB. 기본값은 쓰기를 받은 서버에서 계산되고,
  -- 논리 복제는 값 자체를 옮기므로 복제된 행에서도 원래 값이 유지된다.
  -- app.node_name은 DB 운영 쪽에서 서버마다 설정한다 (없으면 unknown)
  db_node       text NOT NULL DEFAULT coalesce(current_setting('app.node_name', true), 'unknown'),
  created_at    timestamptz NOT NULL DEFAULT now()
);

-- 일본어 번역 (에이전트가 함께 보낸다. 사람이 보낸 메시지는 비어 있다).
-- 컬럼이 이미 있으면 아무것도 바꾸지 않는다
ALTER TABLE chat_messages ADD COLUMN IF NOT EXISTS translation text;
