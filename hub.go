package main

import (
	"context"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

// Hub는 DB를 짧은 주기로 조회해 새 메시지를 이 앱 인스턴스의 SSE 구독자에게 나눠 준다.
//
// 두 환경의 앱이 동시에 떠 있을 때 서로의 메시지를 받으려면 공유 메시지 버스가 필요하다.
// Postgres LISTEN/NOTIFY는 PgBouncer transaction 모드에서 동작하지 않고 논리 복제로도
// 전달되지 않으므로, 공유 DB 자체를 버스로 쓴다. DB가 전환되어도 같은 방식으로 동작한다.
type Hub struct {
	store    *Store
	interval time.Duration

	mu   sync.Mutex
	subs map[chan Message]struct{}

	node atomic.Value // string: 마지막으로 확인한 DB 노드 이름
}

func newHub(store *Store, interval time.Duration) *Hub {
	h := &Hub{store: store, interval: interval, subs: map[chan Message]struct{}{}}
	h.node.Store("unknown")
	return h
}

func (h *Hub) Node() string { return h.node.Load().(string) }

func (h *Hub) Subscribe() chan Message {
	ch := make(chan Message, 256)
	h.mu.Lock()
	h.subs[ch] = struct{}{}
	h.mu.Unlock()
	return ch
}

func (h *Hub) Unsubscribe(ch chan Message) {
	h.mu.Lock()
	if _, ok := h.subs[ch]; ok {
		delete(h.subs, ch)
		close(ch)
	}
	h.mu.Unlock()
}

func (h *Hub) broadcast(m Message) {
	h.mu.Lock()
	defer h.mu.Unlock()
	for ch := range h.subs {
		select {
		case ch <- m:
		default:
			// 따라오지 못하는 구독자는 끊는다. 브라우저가 Last-Event-ID로 재접속해
			// 빠진 구간을 DB에서 다시 받으므로 유실되지 않는다.
			delete(h.subs, ch)
			close(ch)
		}
	}
}

func (h *Hub) Run(ctx context.Context) {
	var last int64
	for {
		id, err := h.store.MaxID(ctx)
		if err == nil {
			last = id
			break
		}
		log.Printf("hub: max id: %v", err)
		select {
		case <-ctx.Done():
			return
		case <-time.After(time.Second):
		}
	}

	poll := time.NewTicker(h.interval)
	defer poll.Stop()
	nodeTick := time.NewTicker(time.Second)
	defer nodeTick.Stop()
	h.refreshNode(ctx)

	for {
		select {
		case <-ctx.Done():
			return
		case <-nodeTick.C:
			h.refreshNode(ctx)
		case <-poll.C:
			qctx, cancel := context.WithTimeout(ctx, 5*time.Second)
			msgs, err := h.store.After(qctx, last, 500)
			cancel()
			if err != nil {
				// 전환 순간(PgBouncer PAUSE)에는 조회가 잠시 막히거나 실패할 수 있다. 다음 주기에 다시 시도한다
				log.Printf("hub: poll: %v", err)
				continue
			}
			for _, m := range msgs {
				h.broadcast(m)
				last = m.ID
			}
		}
	}
}

func (h *Hub) refreshNode(ctx context.Context) {
	qctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	if node, err := h.store.Node(qctx); err == nil {
		h.node.Store(node)
	}
}
