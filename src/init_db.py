# src/init_db.py
import os
import csv
import time
import pymysql
from dotenv import load_dotenv
from pathlib import Path
import redis
# ---------------------------------------------------------------------------
# 0) Load .env (Docker/.env)
# ---------------------------------------------------------------------------
BASE_DIR = Path(__file__).resolve().parent
ENV_PATH = BASE_DIR.parent / "Docker" / ".env"

if ENV_PATH.exists():
    load_dotenv(dotenv_path=ENV_PATH)

DB_HOST = os.getenv("MYSQL_HOST", "mysql")
DB_USER = os.getenv("MYSQL_APP_USER", "app_user")
DB_PASSWORD = os.getenv("MYSQL_APP_PASSWORD", "MYSQL_ROOT_PASSWORD")
ROOT_PASSWORD = os.getenv("MYSQL_ROOT_PASSWORD")
DB_NAME = os.getenv("MYSQL_DATABASE", "fraud_guard")
DATA_DIR = Path("/app/data/origin")

print(DB_HOST, DB_USER, DB_PASSWORD, DB_NAME)

REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))
REDIS_PASSWORD = os.getenv("REDIS_PASSWORD", None)

# ---------------------------------------------------------------------------
# 1) Wait for DB
# ---------------------------------------------------------------------------
def wait_for_db():
    """MySQL 서버가 실제 쿼리를 받을 준비가 될 때까지 대기합니다."""
    retries = 30
    while retries > 0:
        try:
            conn = pymysql.connect(
                host=DB_HOST,
                user="root",
                password=ROOT_PASSWORD,
                charset="utf8mb4",
                connect_timeout=10,  # 연결 시도 10초 지나면 에러
                read_timeout=30,     # 쿼리 실행 후 30초 동안 응답 없으면 에러
                write_timeout=30     # (선택) 데이터 전송 30초 제한
            )
            conn.close()
            print("✅ MySQL is ready to accept connections!")
            return
        except pymysql.MySQLError as e:
            print(f"⏳ Waiting for MySQL... ({retries} retries left) | {e}")
            time.sleep(2)
            retries -= 1
    raise Exception("❌ MySQL Connection Timed Out.")

# ---------------------------------------------------------------------------
# 2) Connections
# ---------------------------------------------------------------------------
def get_connection(with_db: bool = False):
    """with_db=True면 DB_NAME까지 선택된 커넥션 반환"""
    return pymysql.connect(
        host=DB_HOST,
        user="root",
        password=ROOT_PASSWORD,
        db=DB_NAME if with_db else None,
        charset="utf8mb4",
        cursorclass=pymysql.cursors.DictCursor,
        connect_timeout=10,
        read_timeout=30,
        write_timeout=30
    )

# ---------------------------------------------------------------------------
# 3) Create DB + Master Tables
# ---------------------------------------------------------------------------
def create_database_and_tables():
    conn = get_connection(with_db=False)
    try:
        with conn.cursor() as cursor:
            cursor.execute(f"CREATE DATABASE IF NOT EXISTS {DB_NAME}")
            cursor.execute(f"USE {DB_NAME}")

            # Users Table
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

            # Merchants Table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS merchants_data (
                    id INT PRIMARY KEY,
                    merchant_city VARCHAR(30),
                    merchant_state VARCHAR(255),
                    zip INT,
                    mcc INT,
                    mcc_full VARCHAR(100)
                )
            """)

            # Cards Table
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
                    card_on_dark_web VARCHAR(10),
                    FOREIGN KEY (client_id) REFERENCES users_data(id)
                )
            """)

            # Transactions Table (최종 적재용)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS transactions_data (
                    id INT PRIMARY KEY,
                    order_id INT,
                    order_time DATETIME(3),
                    client_id INT,
                    card_id INT,
                    merchant_id INT,
                    amount DECIMAL(10, 2),
                    error VARCHAR(100),
                    is_valid BOOLEAN,
                    is_fraud BOOLEAN,
                    is_severe_fraud BOOLEAN,
                    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (client_id) REFERENCES users_data(id),
                    FOREIGN KEY (card_id) REFERENCES cards_data(id),
                    FOREIGN KEY (merchant_id) REFERENCES merchants_data(id)
                )
            """)

            # [추가] App User 생성 및 권한 부여 로직 (이 부분을 추가하세요!)
            print(f"[SECURITY] Creating App User: {DB_USER}...")
            
            # (1) 유저 생성 (없을 경우에만)
            # '%'는 '모든 호스트(컨테이너)'에서의 접속을 허용한다는 뜻입니다.
            cursor.execute(f"CREATE USER IF NOT EXISTS '{DB_USER}'@'%' IDENTIFIED BY '{DB_PASSWORD}';")
            
            # (2) 권한 부여
            # Root 권한(GRANT OPTION 등)은 주지 않고, 
            # fraud_guard DB에 대해서만 CRUD(Select, Insert, Update, Delete) 권한 부여
            cursor.execute(f"GRANT SELECT, INSERT, UPDATE, DELETE ON {DB_NAME}.* TO '{DB_USER}'@'%';")
            
            # (3) 권한 적용
            cursor.execute("FLUSH PRIVILEGES;")
            
            print(f"[SECURITY] ✅ App User '{DB_USER}' created and privileges granted.")

        conn.commit()
        print("[INIT] Master schema creation checked/completed.")

    finally:
        conn.close()

# ---------------------------------------------------------------------------
# 4) CSV Loader
# ---------------------------------------------------------------------------
def clean_currency(value):
    """$ 기호 제거 및 빈 문자열 처리"""
    if value is None:
        return None
    if isinstance(value, str):
        clean_val = value.replace("$", "")
        if clean_val.strip() == "":
            return None
        return clean_val
    return value

# ---------------------------------------------------------------------------
# 5) Redis Connection (설정부 하단에 위치)
# ---------------------------------------------------------------------------
# decode_responses=True를 주어야 문자열로 다루기 편합니다.
r = redis.StrictRedis(host=REDIS_HOST, port=REDIS_PORT, db=0, password=REDIS_PASSWORD, decode_responses=True)

# ... (wait_for_db, get_connection 함수 등) ...

def load_csv(table_name, file_name):
    """CSV 파일을 읽어 DB에 적재"""
    file_path = DATA_DIR / file_name
    if not file_path.exists():
        print(f"[WARN] File not found: {file_path}. Skip {table_name}.")
        return

    # get_connection 내부에서 에러가 나더라도, 이미 wait_for_db를 통과했으므로
    # 여기서는 논리적 에러 외에 연결 에러는 거의 발생하지 않음
    conn = get_connection(with_db=True)
    try:
        with conn.cursor() as cursor:
            cursor.execute(f"SELECT COUNT(*) as cnt FROM {table_name}")
            result = cursor.fetchone()
            if result and result["cnt"] > 0:
                print(f"[SKIP] {table_name} already has {result['cnt']} rows.")
                return

            print(f"[LOAD] Loading {file_name} -> {table_name}")

            with open(file_path, "r", encoding="utf-8-sig") as f:
                reader = csv.DictReader(f)
                rows = list(reader)
                if not rows:
                    print(f"[WARN] Empty file: {file_name}")
                    return

                columns = rows[0].keys()
                cols_str = ", ".join(columns)
                vals_str = ", ".join(["%s"] * len(columns))
                sql = f"INSERT INTO {table_name} ({cols_str}) VALUES ({vals_str})"

                batch_size = 1000
                data_tuples = []
                for row in rows:
                    cleaned_row = [clean_currency(row[col]) for col in columns]
                    data_tuples.append(tuple(cleaned_row))
                # -------------------------------------------------------
                # [핵심] 레디스에 실시간 적재 (Memory-Write)
                # -------------------------------------------------------
                if table_name == "users_data":# "                                                                                                               실시간성"이 확보됩니다.
                    r.sadd("check:users", str(row['id']))
                elif table_name == "merchants_data":
                    r.sadd("check:merchants", str(row['id']))
                elif table_name == "cards_data":
                    # 카드는 card_id -> client_id 매핑 저장
                    r.set(f"check:card:{row['id']}", row['client_id'])

                # MySQL 배치 인서트
                for i in range(0, len(data_tuples), batch_size):
                    batch = data_tuples[i : i + batch_size]                                     
                    cursor.executemany(sql, batch)
                    conn.commit()
                    print(f"   - Inserted {len(batch)} rows...")

            print(f"[SUCCESS] Loaded {len(rows)} rows into {table_name}.")
    finally:
        conn.close()

# ---------------------------------------------------------------------------
# 5) Main
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    print("--- DB Initializer Started ---")
    wait_for_db()
    create_database_and_tables()

    load_csv("users_data", "users_data.csv")
    load_csv("merchants_data", "merchants_data.csv")
    load_csv("cards_data", "cards_data.csv")

    print("--- DB Initializer Completed ---")
