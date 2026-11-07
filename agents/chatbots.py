"""데모 채팅방에서 계속 대화하는 AI 에이전트.

에이전트는 실제 사용자와 같은 경로(로드 밸런서 → 앱 → PgBouncer → DB)로만 채팅방에 접속한다.
그래서 이전하는 동안 메시지가 빠지거나 대화가 끊기면, 에이전트가 그 사실을 알아채고 말한다.

    uv run chatbots.py --url http://localhost:8080            # Claude로 대화
    uv run chatbots.py --url http://localhost:8080 --offline  # API 없이 준비된 대사로 대화
    uv run chatbots.py --url http://localhost:8090 --metrics-url http://localhost:8091/api/metrics
                                                              # 이전 리허설 제어판의 실측 지표를 보고 대화

종료(Ctrl-C)하면 보낸 메시지, 저장 확인, 스트림 수신, 유실, 중복 집계를 출력한다.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import random
import secrets
import signal
import time
from collections import Counter
from dataclasses import dataclass, field

import anthropic
import httpx
from pydantic import BaseModel, Field

log = logging.getLogger("chatbots")

MODEL = os.environ.get("CHAT_MODEL", "claude-haiku-5-5")
HISTORY_WINDOW = 20  # 프롬프트에 넣는 최근 메시지 수
MISSING_AFTER_S = 10.0  # 저장 확인 후 이 시간 안에 스트림에 안 보이면 유실 의심

COMMON_RULES = """\
너는 사내 채팅방에 있는 팀원이다. 팀은 지금 운영 중인 서비스를 다른 클라우드로 옮기는 중이다.
규칙:
- 한두 문장으로 짧게, 메신저 말투로 말한다. 이모지는 가끔만 쓴다.
- 다른 사람 말에 자연스럽게 반응하고, 가끔 질문도 던진다.
- [시스템 알림]에 환경이나 DB가 바뀌었다는 내용이 있으면 그 변화를 자연스럽게 언급한다.
- [시스템 알림]에 네 메시지가 사라졌다는 내용이 있으면 그 사실을 바로 지적한다.
- 메시지는 한국어로 쓴다. 이름표나 따옴표 없이 네가 할 말만 쓴다.
- 같은 이야기를 반복하지 말고, 대화가 한 주제에서 맴돌면 다른 화제로 넘어간다.
- 숫자(지연 시간, 처리량, 건수, 용량, 시각)는 [실시간 지표]에 있는 값만 그대로 인용한다. 지표에 없는 숫자는 절대 지어내지 않는다.
  지표가 없으면 숫자 없이 말한다. 지표를 매번 나열하지 말고 대화 흐름에 맞을 때 한두 개만 자연스럽게 언급한다.
- 이전 순서: 먼저 앱 트래픽을 env-a에서 env-b로 옮기고(이 동안 env-b 앱이 pg-a DB에 쓰는 것은 정상이다), 그다음 DB를 pg-a에서 pg-b로 전환한다.
  전환 후에는 롤백에 대비해 pg-b에서 pg-a로 역방향 복제가 계속 돈다. 이 순서를 사고로 착각하지 않는다.
- ja에는 같은 메시지를 자연스러운 일본어 메신저 말투로 번역해 넣는다. 이모지는 그대로 둔다."""


class Reply(BaseModel):
    """에이전트의 한 마디. 화면에는 한국어 아래에 일본어 번역이 붙는다."""

    ko: str = Field(description="채팅방에 보낼 한국어 메시지")
    ja: str = Field(description="ko를 일본어로 번역한 문장")

# 대화가 한 주제에 머물지 않도록 가끔 던져 주는 화제
TOPICS = [
    "오늘 점심 메뉴", "트래픽 전환 비율을 언제 올릴지", "롤백 리허설 결과", "새 환경의 비용",
    "모니터링 대시보드에서 본 지연 시간", "이전 끝나고 할 회고", "온프레미스 서버 상태",
    "주말 당직", "새로 들어온 고객 문의", "보안 점검 체크리스트",
]
TOPIC_NUDGE_RATE = 0.3
MENTION_REPLY_RATE = 0.6  # 이름이 불린 에이전트가 바로 답할 확률


@dataclass
class Persona:
    name: str
    role: str
    fallback: list[tuple[str, str]]  # (한국어, 일본어). API를 쓸 수 없을 때 대신 쓰는 대사


PERSONAS = [
    Persona(
        "SRE봇",
        "걱정 많은 SRE. 지연 시간, 롤백 계획, 장애 가능성을 늘 신경 쓴다.",
        [
            ("지연 그래프 보는 중인데 아직 괜찮네요", "レイテンシのグラフを見てますが、まだ大丈夫です"),
            ("롤백 경로는 살아 있죠?", "ロールバックの経路は生きてますよね？"),
            ("CDC 지연 0 유지 중입니다", "CDCの遅延は0のままです"),
        ],
    ),
    Persona(
        "PM봇",
        "낙관적인 PM. 일정과 사용자 영향을 챙기고 팀을 격려한다.",
        [
            ("사용자 문의는 아직 0건이에요 👍", "ユーザーからの問い合わせはまだ0件です 👍"),
            ("이대로면 오늘 안에 끝나겠네요", "この調子なら今日中に終わりそうですね"),
            ("다들 고생 많아요!", "皆さんお疲れさまです！"),
        ],
    ),
    Persona(
        "보안봇",
        "의심 많은 보안 담당자. 공개 포트, 권한, 암호화를 꼼꼼히 따진다.",
        [
            ("새 환경 DB는 사설망에서만 열려 있는 거 맞죠?", "新環境のDBはプライベートネットワークだけで開いてますよね？"),
            ("시크릿이 평문으로 남은 곳은 없나요?", "シークレットが平文で残っている場所はありませんか？"),
            ("감사 로그도 같이 넘어가야 해요", "監査ログも一緒に移行する必要があります"),
        ],
    ),
    Persona(
        "ケンジ",
        "일본 지사에서 참여한 개발자. 한국어를 잘하지만 가끔 일본식 표현이 섞인다. 차분하고 꼼꼼하다.",
        [
            ("이쪽도 문제없이 돌아가고 있어요", "こちらも問題なく動いています"),
            ("이전 순조롭네요", "移行、順調ですね"),
            ("로그 확인했는데 에러 없습니다", "ログを確認しました、エラーなしです"),
        ],
    ),
]


def new_id() -> str:
    return secrets.token_hex(16)


@dataclass
class Sent:
    persona: str
    body: str
    sent_at: float
    acked_at: float | None = None
    msg_id: int | None = None
    reported_missing: bool = False


@dataclass
class Room:
    base_url: str
    metrics_url: str | None = None
    history: list[dict] = field(default_factory=list)
    last_id: int = 0
    ledger: dict[str, Sent] = field(default_factory=dict)
    seen_counts: Counter = field(default_factory=Counter)  # client_msg_id -> 스트림에서 받은 횟수
    notes: dict[str, list[str]] = field(default_factory=dict)  # persona -> 다음 발언 때 전달할 알림
    last_route: dict[str, tuple[str, str]] = field(default_factory=dict)  # persona -> (app_env, db_node)
    reconnects: int = 0
    stream_status: str = "-"

    def note(self, persona: str, text: str) -> None:
        self.notes.setdefault(persona, []).append(text)

    # ---- 수신: SSE ----
    async def listen(self, http: httpx.AsyncClient) -> None:
        while True:
            headers = {"Accept": "text/event-stream"}
            if self.last_id:
                headers["Last-Event-ID"] = str(self.last_id)
            try:
                async with http.stream("GET", f"{self.base_url}/api/stream", headers=headers, timeout=httpx.Timeout(10, read=30)) as resp:
                    if resp.status_code != 200:
                        raise httpx.HTTPStatusError("stream", request=resp.request, response=resp)
                    event, data = "message", []
                    async for line in resp.aiter_lines():
                        if line == "":
                            if data:
                                self._on_event(event, "\n".join(data))
                            event, data = "message", []
                        elif line.startswith("event:"):
                            event = line[6:].strip()
                        elif line.startswith("data:"):
                            data.append(line[5:].lstrip())
            except (httpx.HTTPError, asyncio.TimeoutError) as e:
                log.info("stream disconnected: %s", type(e).__name__)
            self.reconnects += 1
            await asyncio.sleep(1)

    def _on_event(self, event: str, data: str) -> None:
        payload = json.loads(data)
        if event in ("hello", "status"):
            self.stream_status = f"{payload['app_env']} · {payload['db_node']}"
            return
        if event != "message":
            return
        m = payload
        if m["id"] <= self.last_id:
            return
        self.last_id = m["id"]
        self.history.append(m)
        del self.history[:-200]
        self.seen_counts[m["client_msg_id"]] += 1
        print(f"[{m['app_env']} · {m['db_node']}] {m['author']}: {m['body']}", flush=True)
        if m.get("translation"):
            print(f"    └ {m['translation']}", flush=True)

    # ---- 전송: 성공할 때까지 같은 client_msg_id로 재전송 ----
    async def post(self, http: httpx.AsyncClient, persona: str, body: str, translation: str = "") -> None:
        cid = new_id()
        self.ledger[cid] = Sent(persona=persona, body=body, sent_at=time.monotonic())
        delay = 0.3
        deadline = time.monotonic() + 60
        while time.monotonic() < deadline:
            try:
                r = await http.post(
                    f"{self.base_url}/api/messages",
                    json={"client_msg_id": cid, "author": persona, "body": body, "translation": translation},
                    timeout=35,
                )
                if r.status_code in (200, 201):
                    m = r.json()
                    s = self.ledger[cid]
                    s.acked_at, s.msg_id = time.monotonic(), m["id"]
                    self._track_route(persona, m["app_env"], m["db_node"])
                    return
                if 400 <= r.status_code < 500:
                    log.warning("rejected %s: %s", r.status_code, r.text)
                    return
            except httpx.HTTPError as e:
                log.info("post retry (%s)", type(e).__name__)
            await asyncio.sleep(delay)
            delay = min(delay * 2, 3)
        log.error("gave up sending %s", cid)

    def _track_route(self, persona: str, app_env: str, db_node: str) -> None:
        prev = self.last_route.get(persona)
        self.last_route[persona] = (app_env, db_node)
        if prev and prev != (app_env, db_node):
            self.note(
                persona,
                f"방금 네 메시지는 {app_env} 환경의 앱이 처리했고 {db_node} DB에 저장됐다. "
                f"직전에는 {prev[0]} 앱, {prev[1]} DB였다.",
            )

    # ---- 유실 감시 ----
    async def watch_missing(self) -> None:
        while True:
            await asyncio.sleep(2)
            now = time.monotonic()
            for cid, s in self.ledger.items():
                if s.acked_at and not s.reported_missing and cid not in self.seen_counts and now - s.acked_at > MISSING_AFTER_S:
                    s.reported_missing = True
                    log.warning("MISSING: %s %r (id=%s)", s.persona, s.body, s.msg_id)
                    self.note(s.persona, f"네가 보낸 메시지 \"{s.body}\"가 저장됐다고 응답받았는데 채팅방에 보이지 않는다.")

    def post_latency_ms(self, window_s: float = 60) -> dict | None:
        """최근 window_s초 동안 보낸 메시지가 저장 확인까지 걸린 시간 (사용자가 체감하는 전송 지연)."""
        now = time.monotonic()
        lat = sorted((s.acked_at - s.sent_at) * 1000 for s in self.ledger.values() if s.acked_at and now - s.acked_at <= window_s)
        if not lat:
            return None
        pick = lambda q: round(lat[min(len(lat) - 1, int(len(lat) * q))])  # noqa: E731
        return {"count": len(lat), "p50": pick(0.5), "p95": pick(0.95), "max": round(lat[-1])}

    async def fetch_metrics(self, http: httpx.AsyncClient) -> dict | None:
        if not self.metrics_url:
            return None
        try:
            r = await http.get(self.metrics_url, timeout=1.5)
            return r.json() if r.status_code == 200 else None
        except (httpx.HTTPError, ValueError):
            return None

    def summary(self) -> dict:
        acked = [c for c, s in self.ledger.items() if s.acked_at]
        seen = [c for c in self.ledger if c in self.seen_counts]
        return {
            "sent": len(self.ledger),
            "acked": len(acked),
            "seen_in_stream": len(seen),
            "missing_in_stream": len([c for c in acked if c not in self.seen_counts]),
            "duplicates": sum(1 for c in self.ledger if self.seen_counts[c] > 1),
            "stream_reconnects": self.reconnects,
        }


def fmt_bytes(n) -> str:
    if n is None:
        return "-"
    for unit in ("B", "KB", "MB", "GB"):
        if abs(n) < 1024 or unit == "GB":
            return f"{n:.0f}{unit}" if unit == "B" else f"{n:.1f}{unit}"
        n /= 1024
    return str(n)


def format_metrics(m: dict | None, chat: dict | None) -> str:
    """제어판 지표(/api/metrics)와 에이전트가 직접 잰 전송 지연을 프롬프트용 문장으로 만든다."""
    lines = []
    if m:
        lines.append(f"- {m.get('time', '')} 기준 상황: {m.get('stage', '-')}")
        apps = m.get("apps") or {}
        lines.append("- 앱: " + ", ".join(
            f"{a}(env-{a[-1]}) 트래픽 가중치 {v.get('weight')} 상태 {v.get('status')}" for a, v in apps.items()))
        lines.append(f"- 주 DB(쓰기를 받는 DB): {m.get('primary_db')}")
        if rep := m.get("replication"):
            lines.append(f"- CDC 복제 {rep['direction']}: {rep['phase']}, 복제 지연 {fmt_bytes(rep.get('lag_bytes'))}")
        if sw := m.get("last_switchover"):
            detail = (f"전환 가능 시점까지 {sw.get('waited_for_ready_ms')}ms 기다림, 검증은 RESUME 후 "
                      f"{'일치' if sw.get('verify_ok') else '불일치' if sw.get('verify_ok') is False else '생략'}"
                      if sw.get("method", "").startswith("빠른") else f"그중 체크섬 검증 {sw.get('checksum_ms')}ms")
            lines.append(f"- 마지막 DB 전환: {sw['direction']}, {sw.get('method', '')}, {sw['seconds_ago']}초 전, "
                         f"쓰기 중단 {sw['write_pause_ms']}ms ({detail}). 지금까지 전환 {m.get('switchover_count')}회")
        else:
            lines.append("- DB 전환은 아직 한 번도 하지 않았다")
        if lg := m.get("db_load"):
            lines.append(f"- 주문 서비스 DB 부하(최근 60초): 초당 {lg.get('tx_per_sec')}건, 지연 p50 {lg.get('p50_ms_60s')}ms, "
                         f"p95 {lg.get('p95_ms_60s')}ms, p99 {lg.get('p99_ms_60s')}ms, 최대 {lg.get('max_ms_60s')}ms, 에러 {lg.get('errors_60s')}건")
        else:
            lines.append("- 주문 서비스 부하 생성기는 멈춰 있다")
        if cm := m.get("chat_messages"):
            lines.append(f"- 채팅 메시지 누적 {cm.get('total')}건. 처리한 앱 {cm.get('by_app_env')}, 저장된 DB {cm.get('by_db_node')}")
        if r := m.get("switchover_readiness"):
            lines.append(f"- 지금 DB 전환 가능 시점인가: {'가능' if r.get('ready_now') else '대기 (' + ', '.join(r.get('not_ready_reasons') or []) + ')'}. "
                         f"가장 긴 트랜잭션 {r.get('longest_xact_ms')}ms, 잠금 대기 {r.get('lock_waiters')}건, 초당 커밋 {r.get('commits_per_sec')}")
        if f := m.get("last_fault_test"):
            detail = ", ".join(f"{k}={v}" for k, v in f.items() if k not in ("name", "seconds_ago", "pass"))
            lines.append(f"- 마지막 장애 주입 시험(운영자가 일부러 일으킨 장애): {f['name']}, {f['seconds_ago']}초 전, "
                         f"{'기대대로 버팀' if f.get('pass') else '기대와 다른 결과'} ({detail})")
    if chat:
        lines.append(f"- 채팅 전송 지연(에이전트가 직접 측정, 최근 60초 {chat['count']}건): p50 {chat['p50']}ms, p95 {chat['p95']}ms, 최대 {chat['max']}ms")
    return "\n".join(lines)


class Speaker:
    """다음에 말할 에이전트를 고르고, 할 말을 만든다."""

    def __init__(self, room: Room, offline: bool):
        self.room = room
        self.offline = offline
        self.http: httpx.AsyncClient | None = None
        self.client = None if offline else anthropic.AsyncAnthropic(max_retries=1, timeout=20)

    def pick(self) -> Persona:
        last = self.room.history[-1] if self.room.history else None
        if last and random.random() < MENTION_REPLY_RATE:
            # 마지막 메시지에서 이름이 불린 에이전트가 답한다 (항상 그러면 두 명끼리만 대화가 이어진다)
            for p in PERSONAS:
                if p.name in last["body"] and p.name != last["author"]:
                    return p
        # 알림이 쌓인 에이전트가 있으면 먼저 말하게 한다
        for p in PERSONAS:
            if self.room.notes.get(p.name):
                return p
        # 최근에 말한 사람은 덜 고른다
        recent = [m["author"] for m in self.room.history[-3:]]
        candidates = [p for p in PERSONAS if p.name not in recent] or [p for p in PERSONAS if not last or p.name != last["author"]]
        return random.choice(candidates)

    async def line_for(self, p: Persona) -> tuple[str, str]:
        """(한국어, 일본어 번역)을 돌려준다."""
        notes = self.room.notes.pop(p.name, [])
        if self.offline:
            if notes and "보이지 않는다" in " ".join(notes):
                return "어? 방금 내 메시지 어디 갔어요?", "あれ？今の私のメッセージ、どこに行きました？"
            if notes:
                env, db = self.room.last_route[p.name]
                return (f"오 방금 제 메시지는 {env} 앱 → {db} DB로 갔네요",
                        f"おっ、今のメッセージは {env} のアプリ → {db} のDBに行きましたね")
            m = await self.room.fetch_metrics(self.http) if self.http else None
            lg = (m or {}).get("db_load")
            if lg and random.random() < 0.3:
                return (f"지금 DB p95 {lg.get('p95_ms_60s')}ms, 에러 {lg.get('errors_60s')}건이에요 (최근 60초)",
                        f"今のDBのp95は{lg.get('p95_ms_60s')}ms、エラー{lg.get('errors_60s')}件です（直近60秒）")
            return random.choice(p.fallback)

        transcript = "\n".join(
            f"[{m['app_env']} · {m['db_node']}] {m['author']}: {m['body']}" for m in self.room.history[-HISTORY_WINDOW:]
        ) or "(아직 대화가 없다. 네가 먼저 말을 꺼낸다.)"
        prompt = f"최근 채팅:\n{transcript}"
        metrics = format_metrics(await self.room.fetch_metrics(self.http) if self.http else None, self.room.post_latency_ms())
        if metrics:
            prompt += f"\n\n[실시간 지표] (실제 측정값. 숫자는 여기 있는 것만 인용한다)\n{metrics}"
        if notes:
            prompt += "\n\n[시스템 알림]\n" + "\n".join(f"- {n}" for n in notes)
        if random.random() < TOPIC_NUDGE_RATE:
            prompt += f"\n\n(원하면 이 화제를 꺼내도 된다: {random.choice(TOPICS)})"
        prompt += f"\n\n너({p.name})의 다음 메시지를 써라."

        try:
            resp = await self.client.messages.parse(
                model=MODEL,
                max_tokens=2048,
                output_config={"effort": "low"},
                output_format=Reply,
                system=f"너의 이름은 {p.name}. {p.role}\n\n{COMMON_RULES}",
                messages=[{"role": "user", "content": prompt}],
            )
        except anthropic.RateLimitError:
            log.warning("rate limited; using fallback line")
            return random.choice(p.fallback)
        except anthropic.APIStatusError as e:
            log.warning("api error %s; using fallback line", e.status_code)
            return random.choice(p.fallback)
        except anthropic.APIConnectionError:
            log.warning("api unreachable; using fallback line")
            return random.choice(p.fallback)

        reply = resp.parsed_output
        if resp.stop_reason == "refusal" or reply is None:
            return random.choice(p.fallback)
        ko, ja = strip_name(reply.ko, p.name), strip_name(reply.ja, p.name)
        if not ko:
            return random.choice(p.fallback)
        return ko[:500], ja[:1000]


def strip_name(text: str, name: str) -> str:
    """모델이 "이름: 내용" 형식으로 쓰는 경우가 있어 이름표를 떼어 낸다."""
    text = text.strip()
    for prefix in (f"{name}:", f"{name}："):
        while text.startswith(prefix):
            text = text[len(prefix):].strip()
    return text


async def run(base_url: str, offline: bool, min_gap: float, max_gap: float, metrics_url: str | None = None) -> Room:
    room = Room(base_url=base_url.rstrip("/"), metrics_url=metrics_url)
    speaker = Speaker(room, offline)
    async with httpx.AsyncClient() as http:
        speaker.http = http
        tasks = [asyncio.create_task(room.listen(http)), asyncio.create_task(room.watch_missing())]
        stop = asyncio.Event()
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, stop.set)

        await asyncio.sleep(1)  # 기존 대화를 먼저 받아 온다
        while not stop.is_set():
            p = speaker.pick()
            body, translation = await speaker.line_for(p)
            await room.post(http, p.name, body, translation)
            try:
                await asyncio.wait_for(stop.wait(), timeout=random.uniform(min_gap, max_gap))
            except asyncio.TimeoutError:
                pass

        # 마지막으로 보낸 메시지가 스트림에 도착할 시간을 준다
        await asyncio.sleep(2)
        for t in tasks:
            t.cancel()
    return room


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--url", default=os.environ.get("CHAT_URL", "http://localhost:8080"))
    ap.add_argument("--metrics-url", default=os.environ.get("CHAT_METRICS_URL"),
                    help="실측 지표 JSON 주소 (이전 리허설 제어판의 /api/metrics). 주면 에이전트가 이 수치로만 말한다")
    ap.add_argument("--offline", action="store_true", help="Claude API 없이 준비된 대사로 대화")
    ap.add_argument("--min-gap", type=float, default=3.0, help="발언 사이 최소 간격(초)")
    ap.add_argument("--max-gap", type=float, default=6.0, help="발언 사이 최대 간격(초)")
    args = ap.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    logging.getLogger("httpx").setLevel(logging.WARNING)

    room = asyncio.run(run(args.url, args.offline, args.min_gap, args.max_gap, args.metrics_url))
    print(json.dumps(room.summary(), ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
