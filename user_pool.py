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
    # --- [삭제] USER_PROFILES 및 PROFILE_TO_AGE_RANGE ---
    
    # --- [추가] 연령대별 분포 ---
    AGE_DISTRIBUTION = {
        (0, 4): 0.024,
        (5, 9): 0.034,
        (10, 14): 0.044,
        (15, 19): 0.044,
        (20, 24): 0.051,
        (25, 29): 0.067,
        (30, 34): 0.071,
        (35, 39): 0.064,
        (40, 44): 0.075,
        (45, 49): 0.075,
        (50, 54): 0.085,
        (55, 59): 0.083,
        (60, 64): 0.080,
        (65, 69): 0.071,
        (70, 74): 0.049,
        (75, 79): 0.035,
        (80, 84): 0.026,
        (85, 89): 0.015,
        (90, 94): 0.006,
        (95, 99): 0.001
    }

    # --- (기존 유지) ---
    GENDER_RATIO = {
        '여성': 0.49,
        '남성': 0.49,
        '기타': 0.02
    }
    
    EVER_CATEGORY_PROB_TRUE = {
        'ever_M': 0.3,
        'ever_Y': 0.6,
        'ever_K': 0.15
    }

# ----------------------------------------------------
# 2. 사용자 생성을 위한 샘플 데이터 (변경 없음)
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
# 3. 나이 기반 기기 할당 함수 (변경 없음)
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
    # --- [수정] 프로필 로직 삭제, 나이 생성 로직 변경 ---
    # 성별 생성
    gender = random.choices(list(config.GENDER_RATIO.keys()), weights=list(config.GENDER_RATIO.values()), k=1)[0]
    
    # Config의 AGE_DISTRIBUTION에 따라 나이대 선택
    age_ranges = list(config.AGE_DISTRIBUTION.keys())
    age_weights = list(config.AGE_DISTRIBUTION.values())
    selected_range = random.choices(age_ranges, weights=age_weights, k=1)[0]
    
    # 선택된 나이대 안에서 랜덤 나이 생성
    min_age, max_age = selected_range
    age = random.randint(min_age, max_age)
    
    # --- (기존 로직 유지) ---
    # 순번 기반 ID 및 상세 지역 정보 생성
    user_id = f"{user_sequence:08d}"
    city = random.choice(list(LOCATIONS_BY_CITY.keys()))
    district = random.choice(LOCATIONS_BY_CITY[city])
    location = f"{city} {district}"
    
    # 프로모션 민감도, 기기 정보 생성
    promo_sensitivity = random.choice(PROMO_SENSITIVITY_LEVELS)
    device = get_device_by_age(age)
    
    # 'ever' 카테고리 로직
    ever_M = random.random() < config.EVER_CATEGORY_PROB_TRUE['ever_M']
    ever_Y = random.random() < config.EVER_CATEGORY_PROB_TRUE['ever_Y']
    ever_K = random.random() < config.EVER_CATEGORY_PROB_TRUE['ever_K']
    
    # --- [수정] 반환 딕셔너리에서 'profile' 삭제 ---
    return {
        'user_id': user_id,
        'gender': gender,
        'age': age,
        # 'profile': profile, # <- 삭제됨
        'location': location,
        'promo_sensitivity': promo_sensitivity,
        'device': device,
        'ever_M': ever_M,
        'ever_Y': ever_Y,
        'ever_K': ever_K
    }

# ----------------------------------------------------
# 5. 메인 실행 코드 (변경 없음)
# ----------------------------------------------------
if __name__ == '__main__':
    NUMBER_OF_USERS_TO_CREATE = 1000000
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