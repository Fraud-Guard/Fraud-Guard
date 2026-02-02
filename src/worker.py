import os
import json
import time
import pymysql
from confluent_kafka import Consumer, Producer
from dotenv import load_dotenv
from pathlib import Path

# ---------------------------------------------------------------------------
# 0. Load Environment Variables (.env)
# ---------------------------------------------------------------------------
# worker.py의 위치: /app/src/worker.py
# .env의 위치: /app/Docker/.env (Docker 볼륨 마운트 기준)

# 현재 파일(worker.py)의 디렉토리 경로
BASE_DIR = Path(__file__).resolve().parent

# Docker/.env 경로 계산 (../Docker/.env)
ENV_PATH = BASE_DIR.parent / 'Docker' / '.env'

# .env 파일 로드
if ENV_PATH.exists():
    load_dotenv(dotenv_path=ENV_PATH)
    print(f"[INFO] Loaded .env from: {ENV_PATH}")
else:
    print(f"[WARNING] .env file not found at: {ENV_PATH}")

# ---------------------------------------------------------------------------
# 1. Configuration & Connection Setup
# ---------------------------------------------------------------------------

# Kafka Configuration
KAFKA_BROKER = 'kafka:9092'
SOURCE_TOPIC = 'raw-topic'
TARGET_TOPIC = '2nd-topic'
CONSUMER_GROUP = 'fraud-core-group'

# MySQL Configuration (.env에서 로드된 값 사용)
DB_HOST = 'mysql'
DB_USER = 'root'
# .env의 MYSQL_ROOT_PASSWORD 또는 없으면 기본값
DB_PASSWORD = os.environ.get('MYSQL_ROOT_PASSWORD', 'root') 
# .env의 MYSQL_DATABASE 또는 없으면 기본값
DB_NAME = os.environ.get('MYSQL_DATABASE', 'fraud_detection') 

# Initialize Clients
# 1. Kafka Consumer
consumer_conf = {
    'bootstrap.servers': KAFKA_BROKER,
    'group.id': CONSUMER_GROUP,
    'auto.offset.reset': 'latest',
    'enable.auto.commit': True
}
consumer = Consumer(consumer_conf)
consumer.subscribe([SOURCE_TOPIC])

# 2. Kafka Producer
producer_conf = {
    'bootstrap.servers': KAFKA_BROKER
}
producer = Producer(producer_conf)

# ---------------------------------------------------------------------------
# 2. Helper Functions
# ---------------------------------------------------------------------------

def get_db_connection():
    """MySQL 커넥션을 생성하여 반환합니다."""
    return pymysql.connect(
        host=DB_HOST,
        user=DB_USER,
        password=DB_PASSWORD,
        db=DB_NAME,
        charset='utf8mb4',
        cursorclass=pymysql.cursors.DictCursor
    )

def check_integrity(data):
    """
    Logic 1: 무결성 검증 (Rule-Check)
    MySQL을 조회하여 데이터가 유효한지 확인합니다.
    """
    connection = None
    try:
        connection = get_db_connection()
        with connection.cursor() as cursor:
            # 1. Client ID 검증
            sql_user = "SELECT 1 FROM users_data WHERE id = %s LIMIT 1"
            cursor.execute(sql_user, (data['client_id'],))
            if cursor.fetchone() is None:
                print(f"[FAIL] Invalid Client ID: {data['client_id']}")
                return False

            # 2. Card ID 검증 (Card ID 존재 및 소유주 일치 여부)
            sql_card = "SELECT 1 FROM cards_data WHERE id = %s AND client_id = %s LIMIT 1"
            cursor.execute(sql_card, (data['card_id'], data['client_id']))
            if cursor.fetchone() is None:
                print(f"[FAIL] Invalid Card ID: {data['card_id']} or Owner Mismatch")
                return False

            # 3. Merchant ID 검증
            sql_merchant = "SELECT 1 FROM merchants_data WHERE id = %s LIMIT 1"
            cursor.execute(sql_merchant, (data['merchant_id'],))
            if cursor.fetchone() is None:
                print(f"[FAIL] Invalid Merchant ID: {data['merchant_id']}")
                return False

            return True # 모든 검증 통과

    except Exception as e:
        print(f"[ERROR] DB Check Failed: {e}")
        # DB 에러 시 보수적으로 False 반환
        return False
    finally:
        if connection:
            connection.close()

def delivery_report(err, msg):
    """Kafka Producer 전송 콜백"""
    if err is not None:
        print(f'[ERROR] Message delivery failed: {err}')
    else:
        # pass
        pass

# ---------------------------------------------------------------------------
# 3. Main Processor Loop
# ---------------------------------------------------------------------------

def main():
    print(f"[INFO] Worker started. DB: {DB_NAME}")
    print("[INFO] Waiting for messages...")
    
    try:
        while True:
            msg = consumer.poll(1.0) # 1초 대기

            if msg is None:
                continue
            if msg.error():
                print(f"[ERROR] Consumer error: {msg.error()}")
                continue

            # 1. 데이터 파싱
            try:
                raw_data = json.loads(msg.value().decode('utf-8'))
                order_id = raw_data.get('order_id')
            except Exception as e:
                print(f"[ERROR] JSON Parsing failed: {e}")
                continue

            # -------------------------------------------------------
            # Step 1.5: Integrity Check (MySQL)
            # -------------------------------------------------------
            is_valid = check_integrity(raw_data)

            # -------------------------------------------------------
            # Step 2: ML Fraud Check (Placeholder)
            # -------------------------------------------------------
            # 현재는 무조건 False(정상)로 설정 (추후 모델 연동 시 변경)
            is_fraud = False 
            
            # -------------------------------------------------------
            # Final Action: 결정 및 전송 (API 응답 없음)
            # -------------------------------------------------------
            
            # 최종 결정 로직 (무결성 통과 AND 사기 아님)
            # API 응답은 안 하지만, 데이터 파이프라인에는 기록을 남겨야 함
            
            # Kafka 전송용 데이터 구성
            output_data = raw_data.copy()
            output_data['is_valid'] = is_valid
            output_data['is_fraud'] = is_fraud
            
            # TARGET_TOPIC으로 전송 (이후 Spark가 처리)
            producer.produce(
                TARGET_TOPIC,
                json.dumps(output_data).encode('utf-8'),
                callback=delivery_report
            )
            producer.poll(0) # 비동기 전송 트리거

            # 간단한 로그 출력
            status = "APPROVED" if (is_valid and not is_fraud) else "REJECTED"
            print(f"[PROCESSED] Order: {order_id} | Valid: {is_valid}, Fraud: {is_fraud} -> {status}")

    except KeyboardInterrupt:
        print("[INFO] Aborted by user")
    finally:
        consumer.close()
        producer.flush()
        print("[INFO] Worker shutdown complete")

if __name__ == '__main__':
    # DB 컨테이너 구동 대기용
    time.sleep(10) 
    main()