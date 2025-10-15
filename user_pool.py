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
    # --- 사용자 프로필 (★★수정된 부분: 모든 프로필 비율을 균등하게 조정★★) ---
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
    GENDER_RATIO = {
        '여성': 0.7,
        '남성': 0.3
    }

# ----------------------------------------------------
# 2. 사용자 생성을 위한 샘플 데이터
# ----------------------------------------------------
LOCATIONS = ['서울', '경기', '부산', '인천', '대구', '대전', '광주']
PROMO_SENSITIVITY_LEVELS = ['high', 'medium', 'low']

# ----------------------------------------------------
# 3. 사용자 생성 함수
# ----------------------------------------------------
def create_new_user_for_pool(config, user_sequence):
    """
    사용자 풀에 저장될 사용자 1명의 데이터를 생성합니다.
    """
    # 프로필, 성별, 나이 생성
    profile = random.choices(list(config.USER_PROFILES.keys()), weights=list(config.USER_PROFILES.values()), k=1)[0]
    gender = random.choices(list(config.GENDER_RATIO.keys()), weights=list(config.GENDER_RATIO.values()), k=1)[0]
    min_age, max_age = config.PROFILE_TO_AGE_RANGE[profile]
    age = random.randint(min_age, max_age)
    
    # 순번 기반 ID 생성
    user_id = f"{user_sequence:08d}"
    
    # 위치 및 프로모션 민감도 랜덤 할당
    location = random.choice(LOCATIONS)
    promo_sensitivity = random.choice(PROMO_SENSITIVITY_LEVELS)
    
    return {
        'user_id': user_id,
        'gender': gender,
        'age': age,
        'profile': profile,
        'location': location,
        'promo_sensitivity': promo_sensitivity
    }

# ----------------------------------------------------
# 4. 메인 실행 코드
# ----------------------------------------------------
if __name__ == '__main__':
    # --- 설정 ---
    NUMBER_OF_USERS_TO_CREATE = 10000
    OUTPUT_CSV_FILE = 'user_pool.csv'
    
    # --- 객체 생성 ---
    config = Config()
    user_list = []

    print(f"총 {NUMBER_OF_USERS_TO_CREATE}명의 사용자 데이터 생성을 시작합니다...")

    # --- 사용자 생성 루프 ---
    for i in range(1, NUMBER_OF_USERS_TO_CREATE + 1):
        new_user = create_new_user_for_pool(config, i)
        user_list.append(new_user)

    # --- DataFrame 변환 및 파일 저장 ---
    user_pool_df = pd.DataFrame(user_list)
    user_pool_df.to_csv(OUTPUT_CSV_FILE, index=False, encoding='utf-8-sig')

    print(f"✅ 완료! 사용자 풀이 '{OUTPUT_CSV_FILE}' 파일로 성공적으로 저장되었습니다.")
    print("\n--- 생성된 데이터 샘플 (상위 5명) ---")
    print(user_pool_df.head())