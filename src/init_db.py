import os
import csv
import time
import pymysql
from dotenv import load_dotenv
from pathlib import Path  # <--- [필수 추가] 경로 처리를 위해 필요

# .env 로드
load_dotenv()

# 설정
DB_HOST = 'mysql'
DB_USER = 'root'
DB_PASSWORD = os.environ.get('MYSQL_ROOT_PASSWORD', 'root')
DB_NAME = os.environ.get('MYSQL_DATABASE', 'fraud_detection')

# [수정] 경로를 Path 객체로 정의하고 'origin' 폴더까지 지정
# Docker에서 ../:/app 으로 마운트했으므로, /app/data/origin 에 파일이 존재함
DATA_DIR = Path('/app/data/origin') 

def get_connection():
    return pymysql.connect(
        host=DB_HOST,
        user=DB_USER,
        password=DB_PASSWORD,
        charset='utf8mb4',
        cursorclass=pymysql.cursors.DictCursor
    )

def create_database_and_tables():
    """DB 및 테이블 스키마 생성 (DDL)"""
    # 초기 접속은 DB_NAME 없이 접속 (DB가 없을 수도 있으므로)
    conn = pymysql.connect(host=DB_HOST, user=DB_USER, password=DB_PASSWORD)
    try:
        with conn.cursor() as cursor:
            # DB 생성
            cursor.execute(f"CREATE DATABASE IF NOT EXISTS {DB_NAME}")
            cursor.execute(f"USE {DB_NAME}")
            
            # 1. Users Table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS users_data (
                    id INT PRIMARY KEY,
                    current_age INT,
                    retirement_age INT,
                    birth_year INT,
                    birth_month INT,
                    gender VARCHAR(10),
                    address VARCHAR(255),
                    latitude FLOAT,
                    longitude FLOAT,
                    per_capita_income INT,
                    yearly_income INT,
                    total_debt INT,
                    credit_score INT,
                    num_credit_cards INT
                )
            """)

            # 2. Merchants Table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS merchants_data (
                    id INT PRIMARY KEY,
                    business_name VARCHAR(255),
                    business_type VARCHAR(100),
                    address VARCHAR(255),
                    latitude FLOAT,
                    longitude FLOAT
                )
            """)

            # 3. Cards Table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS cards_data (
                    id INT PRIMARY KEY,
                    client_id INT,
                    card_brand VARCHAR(50),
                    card_type VARCHAR(50),
                    card_number VARCHAR(20),
                    expires VARCHAR(10),
                    cvv INT,
                    has_chip VARCHAR(10),
                    num_cards_issued INT,
                    credit_limit FLOAT,
                    acct_open_date VARCHAR(20),
                    year_pin_last_changed INT,
                    FOREIGN KEY (client_id) REFERENCES users_data(id)
                )
            """)
            
        conn.commit()
        print("[INIT] Schema creation checked/completed.")
    finally:
        conn.close()

def load_csv(table_name, file_name):
    """CSV 파일을 읽어 DB에 적재"""
    # Path 객체끼리의 연산이므로 이제 정상 작동함 (/)
    file_path = DATA_DIR / file_name
    
    # 1. 파일 존재 확인
    if not file_path.exists():
        print(f"[WARN] File not found: {file_path}. Skipping {table_name} load.")
        return
    
    conn = get_connection()
    try:
        with conn.cursor() as cursor:
            # DB 선택 (Connection 생성 시 DB_NAME을 안 넣었을 수 있으므로 명시)
            cursor.execute(f"USE {DB_NAME}")
            
            # 2. 데이터 존재 여부 확인 (Idempotency Check)
            cursor.execute(f"SELECT COUNT(*) as cnt FROM {table_name}")
            result = cursor.fetchone()
            
            if result['cnt'] > 0:
                print(f"[SKIP] Table '{table_name}' already has {result['cnt']} rows.")
                return

            # 3. CSV 읽기 및 적재
            print(f"[LOAD] Loading {file_name} into {table_name}...")
            
            # encoding='utf-8-sig'는 엑셀 CSV의 BOM 문자 제거용 (안전장치)
            with open(file_path, 'r', encoding='utf-8-sig') as f:
                reader = csv.DictReader(f)
                rows = list(reader)
                
                if not rows:
                    return

                # 동적 쿼리 생성
                columns = rows[0].keys()
                cols_str = ', '.join(columns)
                vals_str = ', '.join(['%s'] * len(columns))
                
                sql = f"INSERT INTO {table_name} ({cols_str}) VALUES ({vals_str})"
                
                batch_size = 1000
                data_tuples = []
                for row in rows:
                    data_tuples.append(tuple(row[col] for col in columns))
                
                for i in range(0, len(data_tuples), batch_size):
                    batch = data_tuples[i:i + batch_size]
                    cursor.executemany(sql, batch)
                    conn.commit()
                    print(f"   - Inserted {len(batch)} rows...")
            
            print(f"[SUCCESS] Loaded {len(rows)} rows into {table_name}.")
            
    except Exception as e:
        print(f"[ERROR] Failed to load {table_name}: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    # wait_for_db() 제거됨 -> Docker Healthcheck가 대신 보장함
    print("--- DB Initializer Started ---")
    
    create_database_and_tables()
    
    # 순서 중요 (Foreign Key): Users -> Merchants -> Cards
    load_csv('users_data', 'users_data.csv')
    load_csv('merchants_data', 'merchants_data.csv')
    load_csv('cards_data', 'cards_data.csv')
    
    print("--- DB Initializer Completed ---")