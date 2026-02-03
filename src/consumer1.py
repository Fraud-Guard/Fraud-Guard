# consumer group 1 의 역할을 수행합니다.

import os
import json
import pymysql
from kafka import KafkaConsumer
from pathlib import Path

# 환경 변수 설정 (docker-compose와 연동)
KAFKA_BOOTSTRAP = os.getenv('KAFKA_BOOTSTRAP', 'kafka:9092')
KAFKA_TOPIC = '2nd-topic'
MYSQL_HOST = 'mysql'
MYSQL_USER = 'root'
MYSQL_PASSWORD = os.getenv('MYSQL_ROOT_PASSWORD', 'root')
MYSQL_DB = 'fraud_guard'

def get_db_connection():
    return pymysql.connect(
        host=MYSQL_HOST,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
        db=MYSQL_DB,
        charset='utf8mb4',
        cursorclass=pymysql.cursors.DictCursor
    )

def main():
    # 1. 카프카 컨슈머 설정
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=[KAFKA_BOOTSTRAP],
        auto_offset_reset='earliest', # 처음부터 읽기
        group_id='consumer-group-1',  # 컨슈머 그룹 지정
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))# JSON 역직렬화 설정
    )

    print(f"📥 {KAFKA_TOPIC} 모니터링 시작 및 DB 적재 대기 중...")

    conn = get_db_connection()
    cursor = conn.cursor()

    try:
        # 1. Transactions Table (최종 적재용)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS transactions_data (
                id INT PRIMARY KEY,
                order_id INT,
                order_time DATETIME(3),
                client_id INT,
                card_id INT,
                merchant_id INT,
                amount DECIMAL(10, 2),
                is_valid BOOLEAN,
                is_fraud BOOLEAN,
                processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (client_id) REFERENCES users_data(id),
                FOREIGN KEY (card_id) REFERENCES cards_data(id),
                FOREIGN KEY (merchant_id) REFERENCES merchants_data(id)
            )
        """)
        # 2. 메시지 소비 및 적재 루프
        for message in consumer:
            data = message.value
            
            # 2. DB Insert 쿼리
            sql = """
                INSERT INTO transactions_data (id, order_id, order_time, client_id, card_id, merchant_id, amount, is_valid, is_fraud)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE processed_at = CURRENT_TIMESTAMP
            """
            val = (
                data['id'], 
                data.get('order_id', 0),# 없을 경우 0 넣기 
                data['order_time'], 
                data['client_id'], 
                data['card_id'], 
                data['merchant_id'], 
                data['amount'],
                data['is_valid'],
                data['is_fraud']
            )

            cursor.execute(sql, val)
            conn.commit()
            
            print(f"✅ [DB 저장 완료] ID: {data['id']} | Time: {data['order_time']}")

    except Exception as e:
        print(f"❌ 에러 발생: {e}")
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    main()