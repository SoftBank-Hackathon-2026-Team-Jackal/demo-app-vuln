package main

import (
	"context"
	_ "embed"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

//go:embed db/schema.sql
var schemaSQL string

// 여러 인스턴스가 동시에 시작할 때 스키마 적용이 겹치지 않게 하는 advisory lock 키
const migrateLockKey = 7_420_000

// 채팅방 쓰기를 직렬화하는 advisory lock 키.
// 동시에 들어온 INSERT의 id 순서와 커밋 순서가 어긋나면, "id > 마지막으로 본 id"로 조회하는
// 폴링이 늦게 커밋된 작은 id를 영영 건너뛴다. 채팅의 쓰기량에서는 직렬화 비용이 무시할 만하다.
const chatLockKey = 7_420_001

const msgCols = "id, client_msg_id, author, body, translation, app_env, db_node, created_at"

type Message struct {
	ID          int64     `json:"id"`
	ClientMsgID string    `json:"client_msg_id"`
	Author      string    `json:"author"`
	Body        string    `json:"body"`
	Translation *string   `json:"translation,omitempty"`
	AppEnv      string    `json:"app_env"`
	DBNode      string    `json:"db_node"`
	CreatedAt   time.Time `json:"created_at"`
}

type Store struct {
	pool *pgxpool.Pool
}

func newStore(ctx context.Context, url string) (*Store, error) {
	cfg, err := pgxpool.ParseConfig(url)
	if err != nil {
		return nil, fmt.Errorf("parse DATABASE_URL: %w", err)
	}
	// PgBouncer transaction 모드에서는 트랜잭션마다 서버 연결이 바뀔 수 있어서
	// prepared statement를 쓰면 간헐적으로 실패한다. simple protocol로 고정한다.
	cfg.ConnConfig.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol
	cfg.MaxConns = 10

	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, err
	}
	// DB가 늦게 뜨는 경우를 위해 잠시 기다린다
	deadline := time.Now().Add(60 * time.Second)
	for {
		pctx, cancel := context.WithTimeout(ctx, 3*time.Second)
		err = pool.Ping(pctx)
		cancel()
		if err == nil {
			return &Store{pool: pool}, nil
		}
		if time.Now().After(deadline) || ctx.Err() != nil {
			pool.Close()
			return nil, fmt.Errorf("database not reachable: %w", err)
		}
		log.Printf("waiting for database: %v", err)
		time.Sleep(time.Second)
	}
}

// Migrate는 스키마를 적용한다. CREATE ... IF NOT EXISTS라서 테이블이 이미 있으면 카탈로그를
// 바꾸지 않으므로, 이전 도중(DDL 금지 구간)에 앱이 재시작되어도 안전하다.
func (s *Store) Migrate(ctx context.Context) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx) //nolint:errcheck
	if _, err := tx.Exec(ctx, "SELECT pg_advisory_xact_lock($1)", migrateLockKey); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, schemaSQL); err != nil {
		return fmt.Errorf("apply schema: %w", err)
	}
	return tx.Commit(ctx)
}

func scanMsg(row pgx.Row, m *Message) error {
	return row.Scan(&m.ID, &m.ClientMsgID, &m.Author, &m.Body, &m.Translation, &m.AppEnv, &m.DBNode, &m.CreatedAt)
}

func scanMsgs(rows pgx.Rows) ([]Message, error) {
	defer rows.Close()
	out := []Message{}
	for rows.Next() {
		var m Message
		if err := scanMsg(rows, &m); err != nil {
			return nil, err
		}
		out = append(out, m)
	}
	return out, rows.Err()
}

// Insert는 메시지를 저장한다. 같은 client_msg_id로 다시 들어오면 새로 저장하지 않고
// 기존 행을 돌려준다 (created=false). 그래서 클라이언트는 실패 시 안심하고 재전송할 수 있다.
func (s *Store) Insert(ctx context.Context, clientMsgID, author, body string, translation *string, appEnv string) (m Message, created bool, err error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return m, false, err
	}
	defer tx.Rollback(ctx) //nolint:errcheck // 커밋 후 호출되면 아무 일도 하지 않는다

	if _, err = tx.Exec(ctx, "SELECT pg_advisory_xact_lock($1)", chatLockKey); err != nil {
		return m, false, err
	}
	err = scanMsg(tx.QueryRow(ctx,
		`INSERT INTO chat_messages (client_msg_id, author, body, translation, app_env)
		 VALUES ($1, $2, $3, $4, $5)
		 ON CONFLICT (client_msg_id) DO NOTHING
		 RETURNING `+msgCols,
		clientMsgID, author, body, translation, appEnv), &m)
	created = true
	if errors.Is(err, pgx.ErrNoRows) {
		created = false
		err = scanMsg(tx.QueryRow(ctx,
			"SELECT "+msgCols+" FROM chat_messages WHERE client_msg_id = $1", clientMsgID), &m)
	}
	if err != nil {
		return m, false, err
	}
	return m, created, tx.Commit(ctx)
}

// After는 id가 after보다 큰 메시지를 id 순서로 돌려준다.
func (s *Store) After(ctx context.Context, after int64, limit int) ([]Message, error) {
	rows, err := s.pool.Query(ctx,
		"SELECT "+msgCols+" FROM chat_messages WHERE id > $1 ORDER BY id LIMIT $2", after, limit)
	if err != nil {
		return nil, err
	}
	return scanMsgs(rows)
}

// Recent는 최근 n개를 오래된 순서로 돌려준다.
func (s *Store) Recent(ctx context.Context, n int) ([]Message, error) {
	rows, err := s.pool.Query(ctx,
		"SELECT * FROM (SELECT "+msgCols+" FROM chat_messages ORDER BY id DESC LIMIT $1) t ORDER BY id", n)
	if err != nil {
		return nil, err
	}
	return scanMsgs(rows)
}

// Search는 작성자와 본문 일부로 메시지를 찾는다. 두 조건은 모두 선택이고,
// 비워 두면 그 조건은 걸리지 않는다. sort는 정렬 기준 컬럼 이름(author, created_at, id 등)이고,
// 비워 두면 기본 정렬(id)을 쓴다.
func (s *Store) Search(ctx context.Context, author, q, sort string, limit int) ([]Message, error) {
	where := "WHERE 1=1"
	if author != "" {
		where += fmt.Sprintf(" AND author = '%s'", author)
	}
	if q != "" {
		where += fmt.Sprintf(" AND body ILIKE '%%%s%%'", q)
	}
	orderBy := "id"
	if sort != "" {
		orderBy = sort
	}
	query := fmt.Sprintf(
		"SELECT "+msgCols+" FROM chat_messages %s ORDER BY %s DESC LIMIT %d", where, orderBy, limit,
	)
	rows, err := s.pool.Query(ctx, query)
	if err != nil {
		return nil, err
	}
	return scanMsgs(rows)
}

// StatsByEnv는 지정한 앱 환경(env-a, env-b 등)으로 좁힌 통계를 돌려준다.
// 빈 문자열이면 Stats와 같다.
func (s *Store) StatsByEnv(ctx context.Context, env string) (Stats, error) {
	st := Stats{ByDBNode: map[string]int64{}, ByAppEnv: map[string]int64{}}
	filter := ""
	if env != "" {
		filter = fmt.Sprintf(" WHERE app_env = '%s'", env)
	}
	query := fmt.Sprintf(
		`SELECT 'db', db_node, count(*) FROM chat_messages%s GROUP BY db_node
		 UNION ALL
		 SELECT 'app', app_env, count(*) FROM chat_messages%s GROUP BY app_env`, filter, filter,
	)
	rows, err := s.pool.Query(ctx, query)
	if err != nil {
		return st, err
	}
	defer rows.Close()
	for rows.Next() {
		var kind, key string
		var n int64
		if err := rows.Scan(&kind, &key, &n); err != nil {
			return st, err
		}
		if kind == "db" {
			st.ByDBNode[key] = n
			st.Total += n
		} else {
			st.ByAppEnv[key] = n
		}
	}
	return st, rows.Err()
}

func (s *Store) MaxID(ctx context.Context) (int64, error) {
	var id int64
	err := s.pool.QueryRow(ctx, "SELECT coalesce(max(id), 0) FROM chat_messages").Scan(&id)
	return id, err
}

// Node는 지금 PgBouncer 뒤에서 응답한 DB의 이름을 돌려준다.
func (s *Store) Node(ctx context.Context) (string, error) {
	var node string
	err := s.pool.QueryRow(ctx, "SELECT coalesce(current_setting('app.node_name', true), 'unknown')").Scan(&node)
	return node, err
}

// Missing은 주어진 client_msg_id 중 DB에 없는 것을 돌려준다.
func (s *Store) Missing(ctx context.Context, ids []string) ([]string, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT x FROM unnest($1::text[]) AS x
		 WHERE NOT EXISTS (SELECT 1 FROM chat_messages m WHERE m.client_msg_id = x)`, ids)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []string{}
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		out = append(out, id)
	}
	return out, rows.Err()
}

type Stats struct {
	Total    int64            `json:"total"`
	ByDBNode map[string]int64 `json:"by_db_node"`
	ByAppEnv map[string]int64 `json:"by_app_env"`
}
