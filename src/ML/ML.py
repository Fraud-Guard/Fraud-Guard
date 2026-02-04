import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, when,
    # NULL 처리
    coalesce, isnan,
    # 문자열 함수
    concat, concat_ws, substring, length, trim, ltrim, rtrim,
    upper, lower, initcap, regexp_replace, regexp_extract, split,
    lpad, rpad,
    # 날짜/시간 함수
    current_date, current_timestamp, to_date, to_timestamp, date_format,
    year, month, dayofmonth, dayofweek, hour, minute,
    date_add, date_sub, datediff, months_between, trunc,
    # 집계/윈도우
    count, sum, avg, min, max, first, last,
    row_number, rank, dense_rank, lag, lead,
    # 기타
    round as spark_round, expr,
)
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType

# 현재 경로 출력
#print(Path.cwd())

"""
transactions_data.csv
id,client_id,card_id,merchant_id,amount,use_chip,errors

users_data.csv
id,current_age,retirement_age,birth_year,birth_month,gender,address,latitude,longitude,per_capita_income,yearly_income,total_debt,credit_score,num_credit_cards

cards_data.csv
id,client_id,card_brand,card_type,card_number,expires,cvv,has_chip,num_cards_issued,credit_limit,acct_open_date,year_pin_last_changed,card_on_dark_web

merchants_data.csv
id,merchant_city,merchant_state,zip,mcc,mcc_full
"""

spark = SparkSession.builder.appName("DataMerging").getOrCreate()

# CSV 로드 (InferSchema는 데이터가 아주 크면 끄는 것이 빠릅니다)
df_a = spark.read.csv("data/origin/transactions_data.csv", header=True)
df_b = spark.read.json("data/origin/train_fraud_labels.json", header=True)

# Join 수행 (예: 'id' 컬럼 기준)
print('hello')

# CSV로 저장 (분산 저장되므로 하나의 파일로 만들려면 .coalesce(1) 사용)
#merged_df.write.csv("merged_data_output", header=True)