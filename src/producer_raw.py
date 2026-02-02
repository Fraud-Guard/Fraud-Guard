# 데이터 유입(Flask)는 이 파일에 구현 부탁드립니다.

# =========================
# 표준 라이브러리 import
# =========================
import argparse   # 터미널에서 옵션(--csv, --rate 같은 것) 받기 위한 라이브러리
import csv        # CSV 파일을 한 줄씩 읽기 위한 라이브러리
import json       # Python dict -> JSON 문자열로 변환하기 위한 라이브러리
import os         # 환경변수 읽기, 파일 존재 여부 확인 같은 OS 기능
import random     # jitter(랜덤 흔들림) 구현용
import signal     # Ctrl+C(SIGINT), 종료(SIGTERM) 같은 신호 처리
import sys        # stderr 출력 등
import time       # sleep()으로 전송 속도 제어
from datetime import datetime, timezone  # 타임스탬프 처리(UTC)


# =========================
# 전역 상태: 종료 플래그
# =========================
STOP = False
# - Ctrl+C(또는 컨테이너 종료 신호)를 받으면 STOP=True로 바꿔서
#   루프를 안전하게 빠져나오도록 함


def handle_signal(sig, frame):
    """
    SIGINT(Ctrl+C), SIGTERM(docker stop) 같은 종료 신호를 받았을 때 실행되는 함수.
    STOP=True로 바꿔서, for-loop가 다음 반복에서 빠져나오게 한다.
    """
    global STOP
    STOP = True


def utc_now_iso() -> str:
    """
    현재 시간을 UTC로 ISO8601 문자열로 반환.
    예: '2026-02-02T03:21:10.123456+00:00'
    """
    return datetime.now(timezone.utc).isoformat()


def load_checkpoint(path: str) -> int:
    """
    체크포인트 파일에서 '마지막으로 보낸 CSV 라인 번호'를 읽어온다.
    - 재시작했을 때 이어서 보내기 위해 필요.
    - 파일이 없거나 읽기 실패하면 0 반환(맨 처음부터 시작)
    """
    if not path or not os.path.exists(path):
        return 0
    try:
        with open(path, "r", encoding="utf-8") as f:
            return int((f.read() or "0").strip())
    except Exception:
        return 0


def save_checkpoint(path: str, line_no: int) -> None:
    """
    현재까지 성공적으로 전송한 CSV 라인 번호(line_no)를 체크포인트 파일에 저장한다.
    - tmp 파일에 먼저 쓰고 os.replace로 교체(원자적 교체)하여 파일 깨짐 방지
    """
    if not path:
        return
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        f.write(str(line_no))
    os.replace(tmp, path)


def parse_event_time(value: str):
    """
    (옵션) CSV에 거래시간 컬럼이 있을 때 그 값을 datetime으로 파싱하려고 시도한다.

    event_time 모드일 때 쓰는 이유:
    - CSV의 거래 시간 간격을 '실시간처럼' 흉내 내기 위해서
    - 연속 두 거래의 시간 차이(gap)를 계산해서 sleep 시간을 결정한다.

    지원하려는 입력 형태:
    - ISO8601: 2026-02-02T12:34:56Z / 2026-02-02T12:34:56+00:00
    - 흔한 포맷: "YYYY-MM-DD HH:MM:SS" 등

    실패하면 None 반환.
    """
    if value is None:
        return None
    s = str(value).strip()
    if not s:
        return None

    # 1) ISO8601 파싱 시도
    try:
        # 'Z'는 UTC 의미이므로 '+00:00'으로 치환
        dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
        # tzinfo 없는 값이면 UTC로 보정
        return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
    except Exception:
        pass

    # 2) 흔히 쓰는 날짜/시간 포맷들로 파싱 시도
    fmts = [
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d %H:%M:%S.%f",
        "%m/%d/%Y %H:%M",
        "%m/%d/%Y %H:%M:%S",
    ]
    for fmt in fmts:
        try:
            dt = datetime.strptime(s, fmt).replace(tzinfo=timezone.utc)
            return dt
        except Exception:
            continue

    return None


def build_producer(bootstrap_servers: str, client_id: str, acks: str):
    """
    KafkaProducer 객체 생성.
    - bootstrap_servers: kafka 브로커 주소(예: localhost:9092, kafka:9092)
    - client_id: 클라이언트 식별자(로그/모니터링에 표시)
    - acks: producer ACK 정책(0/1/all)

    key_serializer/value_serializer:
    - Kafka는 bytes를 보내므로, 문자열/JSON을 bytes로 바꿔주는 함수가 필요.
    """
    from kafka import KafkaProducer

    return KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        client_id=client_id,

        # 메시지 key(파티션 결정에 사용)를 UTF-8 bytes로 변환
        key_serializer=lambda k: k.encode("utf-8") if k is not None else None,

        # 메시지 value(dict)를 JSON 문자열로 만든 뒤 UTF-8 bytes로 변환
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),

        # acks="all"이면 min.insync.replicas 기준으로 복제 완료 확인 후 ACK
        acks=acks,

        # 네트워크 문제 등 transient error가 있을 때 재시도 횟수
        retries=10,

        # linger_ms: 조금 모아서(batch) 보내 throughput을 올림(지연은 약간 늘 수 있음)
        linger_ms=20,

        # 요청 타임아웃
        request_timeout_ms=30000,

        # 한 커넥션에서 동시에 날리는 요청 수 제한 (재시도/순서 관련 안정성)
        max_in_flight_requests_per_connection=5,

        # kafka 버전 자동 협상 관련 타임아웃
        api_version_auto_timeout_ms=30000,
    )


def main():
    """
    프로그램 시작점:
    1) CLI 옵션 파싱
    2) 시그널 핸들러 등록
    3) 체크포인트 읽기
    4) CSV를 한 줄씩 읽어 Kafka로 전송
    5) 속도 제어(rate 또는 event_time 기반)
    6) 종료 시 flush/close
    """
    parser = argparse.ArgumentParser(description="MVP Kafka producer for raw-topic (CSV -> Kafka)")

    # Kafka 주소: 환경변수(KAFKA_BOOTSTRAP) 있으면 그걸 쓰고 없으면 localhost:9092
    parser.add_argument(
        "--bootstrap",
        default=os.getenv("KAFKA_BOOTSTRAP", "localhost:9092"),
        help="Kafka bootstrap servers (e.g., localhost:9092 or kafka:9092)"
    )

    # 보낼 토픽명
    parser.add_argument("--topic", default=os.getenv("KAFKA_TOPIC", "raw-topic"))

    # 입력 CSV 경로(필수)
    parser.add_argument("--csv", required=True, help="Path to transaction.csv")

    # 체크포인트 파일(마지막 전송 라인 저장)
    parser.add_argument(
        "--checkpoint",
        default=os.getenv("CHECKPOINT_PATH", "checkpoint.txt"),
        help="Checkpoint file path (line number). Use volume mount in Docker."
    )

    # Kafka key로 사용할 CSV 컬럼명(예: card_id)
    parser.add_argument(
        "--key-field",
        default=os.getenv("KEY_FIELD", "card_id"),
        help="CSV column to use as Kafka message key (partitioning)."
    )

    # event_time 모드에서 사용할 CSV 시간 컬럼명
    parser.add_argument(
        "--event-time-field",
        default=os.getenv("EVENT_TIME_FIELD", ""),
        help="CSV column name for event time. Used only in --mode event_time."
    )

    # 속도 제어 모드:
    # - rate: 초당 N건 고정 전송
    # - event_time: CSV 거래시간 간격을 스케일링하여 sleep
    parser.add_argument(
        "--mode",
        choices=["rate", "event_time"],
        default="rate",
        help="rate: fixed messages/sec, event_time: mimic CSV time gaps (scaled)"
    )

    # rate 모드에서 초당 메시지 수
    parser.add_argument(
        "--rate",
        type=float,
        default=float(os.getenv("RATE", "50")),
        help="Messages per second in rate mode (0 = no sleep)"
    )

    # event_time 모드에서 시간 간격을 얼마나 빨리 재생할지
    # 예: gap이 60초인데 time-scale=60이면 sleep=1초(60배 빠르게 재생)
    parser.add_argument(
        "--time-scale",
        type=float,
        default=float(os.getenv("TIME_SCALE", "60")),
        help="In event_time mode: time gap is divided by this scale (bigger=faster)."
    )

    # 지터: sleep 시간을 약간 랜덤으로 흔들어서 더 실시간스럽게 보이게
    parser.add_argument(
        "--jitter",
        type=float,
        default=float(os.getenv("JITTER", "0.0")),
        help="Random jitter ratio added to sleep (0.1 = ±10%). Useful for realism."
    )

    # acks 설정: 0/1/all
    parser.add_argument(
        "--acks",
        default=os.getenv("ACKS", "all"),
        choices=["0", "1", "all"],
        help="Kafka acks: 0/1/all. all = strongest durability."
    )

    # 메시지별로 send의 결과를 기다릴지 여부
    # - 켜면: 전송 실패 즉시 감지(데모 안정) / 느릴 수 있음
    # - 끄면: 비동기 전송(빠름) / 실패 감지가 늦을 수 있음
    parser.add_argument(
        "--sync-ack",
        action="store_true",
        help="Wait for broker ack per message (future.get). Safer for demo, slower."
    )

    # Kafka로 보내지 않고 출력만(테스트용)
    parser.add_argument("--dry-run", action="store_true",
                        help="Do not send. Print JSON lines instead.")

    # 최대 N건만 보내고 종료(테스트/데모 리허설)
    parser.add_argument("--max", type=int, default=0,
                        help="Send at most N messages then stop (0 = all).")

    args = parser.parse_args()

    # 시그널 핸들러 등록: Ctrl+C / docker stop 시 STOP=True
    global STOP
    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    # 체크포인트 읽기(이 라인 번호까지는 이미 보냈다고 보고 skip)
    start_line = load_checkpoint(args.checkpoint)

    sent = 0  # 실제로 보낸 메시지 수
    seq = 0   # 메시지 sequence(프로세스 내 증가)

    # dry-run이 아니면 KafkaProducer 생성
    producer = None if args.dry_run else build_producer(
        bootstrap_servers=args.bootstrap,
        client_id="tx-producer-mvp",
        acks=args.acks
    )

    # rate 모드의 기본 sleep 계산(초당 rate건 => 건당 sleep = 1/rate)
    fixed_sleep = 0.0
    if args.mode == "rate" and args.rate > 0:
        fixed_sleep = 1.0 / args.rate

    # event_time 모드에서 이전 이벤트 시간 저장용
    last_event_dt = None

    # CSV 파일 오픈 후 DictReader로 "한 행 = dict" 형태로 읽음
    with open(args.csv, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)

        # enumerate(reader, start=1): CSV 데이터의 "행 번호"를 1부터 매김
        # (header는 DictReader가 자동 처리하고, 여기 line_no는 데이터행 기준)
        for line_no, row in enumerate(reader, start=1):
            # 종료 신호 받았으면 루프 탈출
            if STOP:
                break

            # 체크포인트까지는 이미 전송했으니 skip
            if line_no <= start_line:
                continue

            seq += 1

            # Kafka key: row에서 key-field 컬럼 값을 꺼냄 (없으면 None)
            # key가 있으면 같은 key는 같은 파티션으로 가기 쉬움(순서 보장 범위)
            key = row.get(args.key_field) or None

            # 기본 sleep은 rate 모드 기준
            sleep_sec = fixed_sleep

            # event_time 모드에서 사용할 변수들
            event_dt = None
            event_time_raw = None

            # event_time 모드일 때는 CSV의 시간 컬럼을 파싱해서 gap 기반 sleep 계산
            if args.mode == "event_time":
                if not args.event_time_field:
                    print("[WARN] --mode event_time인데 --event-time-field가 비어있습니다. rate 모드처럼 동작합니다.",
                          file=sys.stderr)
                else:
                    event_time_raw = row.get(args.event_time_field)
                    event_dt = parse_event_time(event_time_raw)

                    # 이전 이벤트 시간(last_event_dt)이 있고, 현재 파싱 성공(event_dt)했으면 gap 계산
                    if event_dt and last_event_dt:
                        gap = (event_dt - last_event_dt).total_seconds()

                        # 시간 역전 등 이상치 방어
                        if gap < 0:
                            gap = 0

                        # gap을 time-scale로 나눠서 "빠르게 재생"하도록 함
                        sleep_sec = gap / max(args.time_scale, 1e-9)

                    # 이번 이벤트 시간이 정상 파싱됐으면 last_event_dt 갱신
                    last_event_dt = event_dt if event_dt else last_event_dt

            # jitter 적용: sleep_sec를 ±jitter 비율만큼 랜덤 변동
            if sleep_sec and args.jitter > 0:
                factor = 1.0 + random.uniform(-args.jitter, args.jitter)
                sleep_sec = max(0.0, sleep_sec * factor)

            # Kafka로 보낼 메시지 구성(JSON)
            # - produced_at: producer가 만든 시각(UTC)
            # - event_time: CSV 거래시간(파싱되면 isoformat, 아니면 원문 문자열)
            # - payload: CSV 한 행 전체(문자열 dict)
            msg = {
                "seq": seq,
                "produced_at": utc_now_iso(),
                "event_time": (event_dt.isoformat() if event_dt else (event_time_raw or None)),
                "payload": row,
            }

            # 전송 로직
            if args.dry_run:
                # dry-run이면 실제 Kafka 전송 대신 콘솔 출력
                print(json.dumps({"key": key, "value": msg}, ensure_ascii=False))
            else:
                # 실제 Kafka 전송(비동기)
                fut = producer.send(args.topic, key=key, value=msg)

                # sync-ack 옵션이면 메시지마다 ACK를 기다려서 실패를 즉시 감지
                if args.sync_ack:
                    try:
                        fut.get(timeout=10)
                    except Exception as e:
                        print(f"[ERROR] send failed at line={line_no}: {e}", file=sys.stderr)
                        STOP = True
                        break

            # 여기까지 왔다는 건(특히 sync-ack일 때) 성공적으로 보냈다고 보고 체크포인트 갱신
            save_checkpoint(args.checkpoint, line_no)
            sent += 1

            # max 옵션이면 N개만 보내고 종료
            if args.max > 0 and sent >= args.max:
                break

            # 계산된 sleep 적용(실시간처럼 보이게)
            if sleep_sec > 0:
                time.sleep(sleep_sec)

    # 종료 전 producer flush/close: 버퍼에 남은 메시지 모두 전송 후 닫기
    if producer is not None:
        producer.flush(10)
        producer.close()

    # 결과 출력
    print(f"[DONE] sent={sent}, last_line={load_checkpoint(args.checkpoint)}")


# Python 파일을 직접 실행했을 때 main() 호출
if __name__ == "__main__":
    main()