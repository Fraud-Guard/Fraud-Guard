from datetime import datetime

def get_scaled_timestamp(row, index):
    """
    요구사항: 시:분까지는 현재 시간, 초와 밀리초는 index 기반으로 생성
    """
    now = datetime.now()
    
    # 1. 날짜와 시:분 추출 (예: 2026-02-02 21:40)
    base_time = now.strftime('%Y-%m-%d %H:%M')
    
    # 2. 초(Second) 계산: 1000건당 1초씩 증가시키거나, 단순히 index 활용
    # 여기서는 index를 60으로 나눈 나머지를 사용하여 0~59초가 반복되게 함
    seconds = (index // 1000) % 60 
    
    # 3. 밀리초(Millisecond) 계산: index의 마지막 3자리를 사용 (000~999)
    millis = index % 1000
    
    # 4. 최종 문자열 조립
    order_time = f"{base_time}:{seconds:02d}.{millis:03d}"
    
    # 금액 데이터 정제 ($ 기호 등 제거)
    raw_amount = str(row['amount'])
    clean_amount = "".join(c for c in raw_amount if c.isdigit() or c in ['.', '-'])
    
    return {
        "order_time": order_time,
        "client_id": int(row['client_id']),
        "card_id": int(row['card_id']),
        "merchant_id": int(row['merchant_id']),
        "amount": float(clean_amount)
    }