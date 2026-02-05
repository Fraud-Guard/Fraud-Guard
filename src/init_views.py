import os
import time
import pymysql
from dotenv import load_dotenv
from pathlib import Path

# .env 로드 (Docker/.env)
BASE_DIR = Path(__file__).resolve().parent
ENV_PATH = BASE_DIR.parent / "Docker" / ".env"
if ENV_PATH.exists():
    load_dotenv(dotenv_path=ENV_PATH)

DB_HOST = os.getenv("MYSQL_HOST", "mysql")
DB_USER = os.getenv("MYSQL_USER", "root")
DB_PASSWORD = os.getenv("MYSQL_ROOT_PASSWORD", "root")
DB_NAME = os.getenv("MYSQL_DATABASE", "fraud_guard")

# ------------------------------------------------------------
# 공통: DB 준비 대기
# ------------------------------------------------------------
def wait_for_db():
    retries = 30
    while retries > 0:
        try:
            conn = pymysql.connect(
                host=DB_HOST,
                user=DB_USER,
                password=DB_PASSWORD,
                charset="utf8mb4",
            )
            conn.close()
            print("✅ MySQL is ready!")
            return
        except pymysql.MySQLError as e:
            print(f"⏳ Waiting for MySQL... ({retries} left) {e}")
            time.sleep(2)
            retries -= 1
    raise Exception("❌ MySQL not ready (timeout)")

def get_conn():
    return pymysql.connect(
        host=DB_HOST,
        user=DB_USER,
        password=DB_PASSWORD,
        db=DB_NAME,
        charset="utf8mb4",
        autocommit=True
    )

# ------------------------------------------------------------
# 1) agg 테이블 생성
# ------------------------------------------------------------
def create_agg_tables():
    ddl_template = """
    CREATE TABLE IF NOT EXISTS {table_name} (
        window_start DATETIME(3) NOT NULL,
        window_end   DATETIME(3) NOT NULL,
        merchant_id  INT NOT NULL,

        txn_cnt     BIGINT NOT NULL,
        amount_sum  DECIMAL(18,2) NOT NULL,
        fraud_cnt   BIGINT NOT NULL,
        valid_cnt   BIGINT NOT NULL,

        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                   ON UPDATE CURRENT_TIMESTAMP,

        PRIMARY KEY (window_start, window_end, merchant_id)
    );
    """
    tables = ["agg_txn_1m", "agg_txn_1h", "agg_txn_1d"]

    conn = get_conn()
    try:
        with conn.cursor() as cur:
            for t in tables:
                cur.execute(ddl_template.format(table_name=t))
        print("✅ agg_txn tables ensured.")
    finally:
        conn.close()

# ------------------------------------------------------------
# 2) vw_agg_* 뷰 생성 (Tableau용)
# ------------------------------------------------------------
def create_agg_views():
    # agg + merchants 조인으로 Tableau에서 차원(지역/mcc) 같이 쓰기 좋게
    view_sql = {
        "vw_agg_1m": """
            CREATE OR REPLACE VIEW vw_agg_1m AS
            SELECT
              a.window_start,
              a.window_end,
              a.merchant_id,
              m.merchant_city,
              m.merchant_state,
              m.mcc,
              m.mcc_full,
              a.txn_cnt,
              a.amount_sum,
              a.fraud_cnt,
              a.valid_cnt,
              (a.txn_cnt - a.valid_cnt) AS rejected_cnt,
              CASE WHEN a.txn_cnt = 0 THEN 0 ELSE a.valid_cnt / a.txn_cnt END AS approval_rate,
              CASE WHEN a.txn_cnt = 0 THEN 0 ELSE a.fraud_cnt / a.txn_cnt END AS fraud_rate,
              CASE WHEN a.txn_cnt = 0 THEN 0 ELSE a.amount_sum / a.txn_cnt END AS avg_amount
            FROM agg_txn_1m a
            LEFT JOIN merchants_data m ON a.merchant_id = m.id;
        """,
        "vw_agg_1h": """
            CREATE OR REPLACE VIEW vw_agg_1h AS
            SELECT
              a.window_start,
              a.window_end,
              a.merchant_id,
              m.merchant_city,
              m.merchant_state,
              m.mcc,
              m.mcc_full,
              a.txn_cnt,
              a.amount_sum,
              a.fraud_cnt,
              a.valid_cnt,
              (a.txn_cnt - a.valid_cnt) AS rejected_cnt,
              CASE WHEN a.txn_cnt = 0 THEN 0 ELSE a.valid_cnt / a.txn_cnt END AS approval_rate,
              CASE WHEN a.txn_cnt = 0 THEN 0 ELSE a.fraud_cnt / a.txn_cnt END AS fraud_rate,
              CASE WHEN a.txn_cnt = 0 THEN 0 ELSE a.amount_sum / a.txn_cnt END AS avg_amount
            FROM agg_txn_1h a
            LEFT JOIN merchants_data m ON a.merchant_id = m.id;
        """,
        "vw_agg_1d": """
            CREATE OR REPLACE VIEW vw_agg_1d AS
            SELECT
              a.window_start,
              a.window_end,
              a.merchant_id,
              m.merchant_city,
              m.merchant_state,
              m.mcc,
              m.mcc_full,
              a.txn_cnt,
              a.amount_sum,
              a.fraud_cnt,
              a.valid_cnt,
              (a.txn_cnt - a.valid_cnt) AS rejected_cnt,
              CASE WHEN a.txn_cnt = 0 THEN 0 ELSE a.valid_cnt / a.txn_cnt END AS approval_rate,
              CASE WHEN a.txn_cnt = 0 THEN 0 ELSE a.fraud_cnt / a.txn_cnt END AS fraud_rate,
              CASE WHEN a.txn_cnt = 0 THEN 0 ELSE a.amount_sum / a.txn_cnt END AS avg_amount
            FROM agg_txn_1d a
            LEFT JOIN merchants_data m ON a.merchant_id = m.id;
        """,
    }

    conn = get_conn()
    try:
        with conn.cursor() as cur:
            for name, sql in view_sql.items():
                cur.execute(sql)
        print("✅ vw_agg views ensured.")
    finally:
        conn.close()

# ------------------------------------------------------------
# 3) (선택) 고객 마케팅용 뷰: vw_customer_marketing
# ------------------------------------------------------------
def create_customer_marketing_view():
    sql = """
    CREATE OR REPLACE VIEW vw_customer_marketing AS
    SELECT
      u.id AS client_id,
      u.current_age,
      CASE
        WHEN u.current_age BETWEEN 18 AND 30 THEN '18 to 30'
        WHEN u.current_age BETWEEN 31 AND 40 THEN '31 to 40'
        WHEN u.current_age BETWEEN 41 AND 50 THEN '41 to 50'
        WHEN u.current_age BETWEEN 51 AND 59 THEN '51 to 59'
        ELSE '60+'
      END AS age_group,
      u.gender,
      u.address,
      TRIM(SUBSTRING_INDEX(SUBSTRING_INDEX(u.address, ',', -1), ' ', 1)) AS state_guess,
      u.latitude,
      u.longitude,
      u.per_capita_income,
      u.yearly_income,
      u.total_debt,
      CASE
        WHEN u.yearly_income IS NULL OR u.yearly_income = 0 THEN NULL
        ELSE (u.total_debt * 1.0 / u.yearly_income)
      END AS dti_ratio,
      u.credit_score,
      u.num_credit_cards,
      c.id AS card_id,
      c.card_brand,
      c.card_type,
      c.credit_limit
    FROM users_data u
    LEFT JOIN cards_data c ON c.client_id = u.id;
    """
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(sql)
        print("✅ vw_customer_marketing ensured.")
    finally:
        conn.close()

if __name__ == "__main__":
    print("--- View Initializer Started ---")
    wait_for_db()
    create_agg_tables()
    create_agg_views()
    create_customer_marketing_view()  # 원치 않으면 이 줄만 주석처리
    print("--- View Initializer Completed ---")
