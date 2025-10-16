import pandas as pd
import random
from datetime import datetime

# ----------------------------------------------------
# 1. Config 클래스: 사용자 생성 규칙 정의
# ----------------------------------------------------
class Config:
    """
    사용자 풀 생성을 위한 기본 설정값.
    """
    # --- 사용자 프로필 ---
    USER_PROFILES = {
        '직장인': 0.25,
        '중고생': 0.25,
        '대학생': 0.25,
        '기타': 0.25
    }
    PROFILE_TO_AGE_RANGE = {
        '직장인': (27, 55),
        '중고생': (14, 19),
        '대학생': (20, 26),
        '기타': (10, 70)
    }
    # (사용자 요청에 따라 '기타' 성별 추가)
    GENDER_RATIO = {
        '여성': 0.49,
        '남성': 0.49,
        '기타': 0.02
    }

# ----------------------------------------------------
# 2. 사용자 생성을 위한 샘플 데이터
# ----------------------------------------------------
LOCATIONS_BY_CITY = {
    '서울시': ['강남구', '마포구', '서초구', '송파구', '영등포구', '종로구'],
    '부산시': ['해운대구', '부산진구', '동래구', '남구', '수영구'],
    '인천시': ['남동구', '부평구', '서구', '연수구'],
    '대구시': ['수성구', '달서구', '중구', '북구'],
    '대전시': ['서구', '유성구', '중구'],
    '광주시': ['서구', '광산구', '북구']
}
PROMO_SENSITIVITY_LEVELS = ['high', 'medium', 'low']

# ----------------------------------------------------
# 3. 나이 기반 기기 할당 함수
# ----------------------------------------------------
def get_device_by_age(age):
    """
    나이를 기준으로 갤럭시/아이폰 사용 비율에 따라 기기를 할당합니다.
    """
    if age < 30: iphone_ratio = 0.6
    elif age < 40: iphone_ratio = 0.5
    elif age < 50: iphone_ratio = 0.3
    elif age < 60: iphone_ratio = 0.1
    elif age < 70: iphone_ratio = 0.05
    else: iphone_ratio = 0.02
        
    return 'iPhone' if random.random() < iphone_ratio else 'Galaxy'

# ----------------------------------------------------
# 4. 사용자 생성 함수 (수정)
# ----------------------------------------------------
def create_new_user_for_pool(config, user_sequence):
    """
    사용자 풀에 저장될 사용자 1명의 데이터를 생성합니다.
    """
    # 프로필, 성별, 나이 생성
    profile = random.choice(list(config.USER_PROFILES.keys()))
    gender = random.choices(list(config.GENDER_RATIO.keys()), weights=list(config.GENDER_RATIO.values()), k=1)[0]
    min_age, max_age = config.PROFILE_TO_AGE_RANGE[profile]
    age = random.randint(min_age, max_age)
    
    # 순번 기반 ID 및 상세 지역 정보 생성
    user_id = f"{user_sequence:08d}"
    city = random.choice(list(LOCATIONS_BY_CITY.keys()))
    district = random.choice(LOCATIONS_BY_CITY[city])
    location = f"{city} {district}"
    
    # 프로모션 민감도, 기기 정보 생성
    promo_sensitivity = random.choice(PROMO_SENSITIVITY_LEVELS)
    device = get_device_by_age(age)
    
    # ★★ 추가된 로직: 'ever' 카테고리 랜덤 할당 ★★
    ever = random.choice([True, False])
    
    return {
        'user_id': user_id,
        'gender': gender,
        'age': age,
        'profile': profile,
        'location': location,
        'promo_sensitivity': promo_sensitivity,
        'device': device,
        'ever': ever  # ever 정보 추가
    }

# ----------------------------------------------------
# 5. 메인 실행 코드
# ----------------------------------------------------
if __name__ == '__main__':
    NUMBER_OF_USERS_TO_CREATE = 10000
    OUTPUT_CSV_FILE = 'user_pool.csv'
    
    config = Config()
    user_list = []

    print(f"총 {NUMBER_OF_USERS_TO_CREATE}명의 사용자 데이터 생성을 시작합니다...")

    for i in range(1, NUMBER_OF_USERS_TO_CREATE + 1):
        new_user = create_new_user_for_pool(config, i)
        user_list.append(new_user)

    user_pool_df = pd.DataFrame(user_list)
    user_pool_df.to_csv(OUTPUT_CSV_FILE, index=False, encoding='utf-8-sig')

    print(f"✅ 완료! 사용자 풀이 '{OUTPUT_CSV_FILE}' 파일로 성공적으로 저장되었습니다.")
    print("\n--- 생성된 데이터 샘플 (상위 5명) ---")
    print(user_pool_df.head())