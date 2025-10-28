import pandas as pd
import random
import uuid
import json
from datetime import datetime

# ----------------------------------------------------
# 1. Config 클래스: 모든 규칙과 확률 정의
# ----------------------------------------------------
class Config:
    """
    데이터 생성에 필요한 모든 기본 설정(고정값, 확률)을 관리하는 클래스.
    """
    # --- 사용자 프로필 ---
    USER_PROFILES = {
        '직장인': 0.513,
        '중고생': 0.280,
        '대학생': 0.150,
        '기타': 0.057
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
    USER_INITIAL_LOGIN_RATIO = {
        'login': 0.95,
        'not_login': 0.05
    }

    # --- 행동 시나리오 확률 ---
    PROB_ON_LOGIN_ATTEMPT = {
        'login_success': 0.9,
        'out': 0.1
    }
    PROB_MAINPAGE_NOT_LOGIN = {
        'search': 0.5,
        'recommand': 0.1,
        'promotion': 0.35,
        'login': 0.05
    }
    PROB_MAINPAGE_LOGIN = {
        'search': 0.5,
        'recommand': 0.1,
        'promotion': 0.35,
        'mypage': 0.05
    }
    PROB_MYPAGE_LOGIN = {
        'order_detail': 1
    }
    PROB_MYPAGE_NOT_LOGIN = {
        'login': 1
    }
    PROB_SEARCH = {
        'search_text': 0.3,
        'view_recommended_item': 0.7
    }
    PROB_ORDER_DETAIL = {
        'mainpage': 0.1,
        'out': 0.9
    }
    PROB_ACTION_AFTER_PROMOTION = {
        'mainpage': 1
    }
    PROB_RECOMMANDED_ITEM = {
        'item': 0.2,
        'mainpage': 0.7,
        'out': 0.1
    }
    PROB_VIEW_ITEM_LIST = {
        'click_item': 0.95,
        'out': 0.05
    }
    PROB_VIEW_ITEM_NOT_LOGIN = {
        'login': 0.5,
        'out': 0.5
    }
    PROB_VIEW_ITEM_LOGIN = {
        'add_to_cart': 0.3,
        'out': 0.5,
        'buy_baro': 0.1,
        'purchase': 0.1
    }
    PROB_ACTION_AFTER_ADD_TO_CART = {
        'view_cart': 0.7,
        'return_mainpage': 0.05,
        'return_item_list': 0.25
    }
    PROB_ACTION_AFTER_VIEW_CART = {
        'purchase': 0.6,
        'abandon': 0.35,
        'out': 0.05
    }
    PROB_BARO_SHOP = {
        'choose_shop': 0.9,
        'out': 0.1
    }
    PROB_BARO_VISIT = {
        'choose_visit': 1
    }
    PROB_BARO_PURCHASE = {
        'purchase': 0.95,
        'out': 0.05
    }
    PROB_PURCHASE = {
        'purchase': 0.95,
        'out': 0.05
    }
    PROB_PURCHASE_CLEAR = {
        'return_mainpage': 0.15,
        'order_detail': 0.6,
        'out': 0.25
    }

# ----------------------------------------------------
# 2. 사용자 생성 함수 (기존 함수 삭제됨)
# ----------------------------------------------------

# ----------------------------------------------------
# 3. 메인 데이터 생성기 클래스 (user_pool 로드 로직 추가)
# ----------------------------------------------------
class SyntheticDataGenerator:
    def __init__(self, config, book_db, input_data, user_pool_path='user_pool.csv'):
        self.config = config
        self.book_db = book_db
        self.input_data = input_data
        
        # user_pool 로드
        try:
            self.user_pool = pd.read_csv(user_pool_path)
            print(f"✅ 사용자 풀 ('{user_pool_path}') 로딩 성공!")
        except FileNotFoundError:
            print(f"⚠️ 사용자 풀 ('{user_pool_path}')을 찾을 수 없습니다. 프로그램을 종료합니다.")
            exit(0)

        # 프로필 비율에 맞는 유저 선택을 위한 가중치 계산
        self.weights = []
        total_weight = sum(self.config.USER_PROFILES.values())
        
        for profile in self.user_pool['profile']:
            # 해당 유저의 프로필에 해당하는 Config 비율을 가중치로 사용
            weight = self.config.USER_PROFILES.get(profile, 0) / total_weight 
            self.weights.append(weight)

        # 가중치 정규화 (선택 로직의 안정성 확보)
        weight_sum = sum(self.weights)
        if weight_sum > 0:
            self.weights = [w / weight_sum for w in self.weights]
        else:
            print("⚠️ 유저 풀의 프로필이 Config와 일치하지 않아 가중치 부여가 불가능합니다. 균등 확률을 사용합니다.")
            self.weights = [1.0 / len(self.user_pool)] * len(self.user_pool)
            
    def _get_random_user(self):
        """
        Config의 USER_PROFILES 비율에 맞춰 user_pool에서 사용자 1명을 선택합니다.
        선택된 행(row)을 딕셔너리 형태로 반환합니다.
        """
        # 가중치를 적용하여 user_pool에서 하나의 행(유저)을 선택
        selected_user_row = self.user_pool.sample(n=1, weights=self.weights).iloc[0]
        
        # 필요한 정보만 추출하여 딕셔너리로 반환 (세션 시작 시 로그인 상태 추가)
        login_type = random.choices(
            list(self.config.USER_INITIAL_LOGIN_RATIO.keys()), 
            weights=list(self.config.USER_INITIAL_LOGIN_RATIO.values()), k=1
        )[0]
        
        return {
            'user_id': selected_user_row['user_id'],
            'gender': selected_user_row['gender'],
            'age': selected_user_row['age'], # int64 타입이 유지될 수 있음
            'profile': selected_user_row['profile'],
            'initial_login_status': (login_type == 'login')
        }

    def _get_next_action(self, prob_dict):
        return random.choices(list(prob_dict.keys()), weights=list(prob_dict.values()), k=1)[0]

    def _generate_event(self, event_name, session_id, user_id, properties={}):
        return {
            'event_name': event_name,
            'session_id': session_id,
            'user_id': user_id,
            'timestamp': datetime.now().isoformat(),
            'properties': properties
        }

    def generate_sessions(self):
        all_event_logs = []
        total_sessions = self.input_data.get('total_sessions', 100)
        
        print(f"총 {total_sessions}개의 세션 생성을 시작합니다...")
        
        for _ in range(total_sessions):
            session_events = self._create_one_session()
            all_event_logs.extend(session_events)
            
        print(f"총 {len(all_event_logs)}개의 이벤트 로그가 생성되었습니다.")
        return all_event_logs
        
    def _create_one_session(self):
        # 유저 풀에서 유저를 가져옴 (기존 create_new_user 호출 대체)
        user = self._get_random_user()
        
        session_id = str(uuid.uuid4())
        event_logs = []
        is_logged_in = user['initial_login_status']
        
        event_logs.append(self._generate_event('App Launch', session_id, user['user_id']))
        event_logs.append(self._generate_event('View Main Page', session_id, user['user_id'], {'is_logged_in': is_logged_in}))
        
        current_rule_name = 'PROB_MAINPAGE_LOGIN' if is_logged_in else 'PROB_MAINPAGE_NOT_LOGIN'
        
        while True:
            prob_dict = getattr(self.config, current_rule_name)
            chosen_action = self._get_next_action(prob_dict)
            
            event_logs.append(self._generate_event(current_rule_name, session_id, user['user_id']))
            
            if chosen_action == 'out':
                break

            # 다음 행동 분기점을 결정하는 로직 (유지)
            if chosen_action == 'login':
                current_rule_name = 'PROB_ON_LOGIN_ATTEMPT'
                # (참고: 로그인 성공 시 is_logged_in 상태 변경 로직 추가 필요)
            elif chosen_action in ['search', 'search_text', 'view_recommended_item']:
                current_rule_name = 'PROB_VIEW_ITEM_LIST'
            elif chosen_action in ['item', 'click_item']:
                current_rule_name = 'PROB_VIEW_ITEM_LOGIN' if is_logged_in else 'PROB_VIEW_ITEM_NOT_LOGIN'
            elif chosen_action == 'add_to_cart':
                current_rule_name = 'PROB_ACTION_AFTER_ADD_TO_CART'
            elif chosen_action == 'view_cart':
                current_rule_name = 'PROB_ACTION_AFTER_VIEW_CART'
            elif chosen_action == 'purchase':
                current_rule_name = 'PROB_PURCHASE_CLEAR'
            elif chosen_action == 'buy_baro':
                current_rule_name = 'PROB_BARO_SHOP'
            elif chosen_action == 'choose_shop':
                current_rule_name = 'PROB_BARO_VISIT'
            elif chosen_action == 'choose_visit':
                current_rule_name = 'PROB_BARO_PURCHASE'
            elif chosen_action == 'mypage':
                current_rule_name = 'PROB_MYPAGE_LOGIN'
            elif chosen_action == 'order_detail':
                current_rule_name = 'PROB_ORDER_DETAIL'
            elif chosen_action in ['mainpage', 'return_mainpage', 'return_item_list', 'abandon', 'promotion', 'recommand']:
                current_rule_name = 'PROB_MAINPAGE_LOGIN' if is_logged_in else 'PROB_MAINPAGE_NOT_LOGIN'
            else:
                break
                
        return event_logs

# ----------------------------------------------------
# 4. 테스트 코드 (JSON 직렬화 및 CSV 저장 로직 포함)
# ----------------------------------------------------

# NumPy 타입(int64 등)을 Python 표준 타입으로 변환하기 위한 함수
def convert_to_python_native(obj):
    # pandas에서 오는 int64와 같은 NumPy 정수 타입을 파이썬 int로 변환
    if obj.__class__.__name__ in ['int64', 'int32', 'int16']:
        return int(obj)
    # 다른 타입에 대한 처리 (예: datetime 객체)
    if isinstance(obj, datetime):
        return obj.isoformat()
    # 그 외 처리하지 못한 타입은 에러 발생
    raise TypeError(f'Object of type {obj.__class__.__name__} is not JSON serializable')

if __name__ == '__main__':
    # --- Input 데이터 ---
    test_input = {
        'total_sessions': 5
    }
    
    # --- 객체 생성 ---
    config = Config()
    
    try:
        book_db = pd.read_csv('biblio_data.csv')
        print("✅ 서적 DB ('biblio_data.csv') 로딩 성공!")
    except FileNotFoundError:
        print("⚠️ 'biblio_data.csv'을 찾을 수 없습니다.")
        book_db = pd.DataFrame() 
        
    # --- 생성기 실행 ---
    # 변수명이 정확하게 config, book_db, test_input으로 전달되고 있음.
    generator = SyntheticDataGenerator(config, book_db, test_input, user_pool_path='user_pool.csv') 
    generated_data = generator.generate_sessions()

    # --- 결과 출력 및 저장 (유저 기준으로 정리) ---
    print("\n--- 생성된 전체 세션 데이터 ---")
    
    # 1. generated_data를 DataFrame으로 변환
    log_df = pd.DataFrame(generated_data)

    # 2. 'properties' 딕셔너리를 별도 컬럼으로 분리 (로그인 상태 등)
    if 'properties' in log_df.columns and not log_df['properties'].isnull().all():
        properties_df = pd.json_normalize(log_df['properties'])
        log_df = pd.concat([log_df.drop('properties', axis=1), properties_df], axis=1)

    # 3. user_id와 timestamp를 기준으로 정렬
    log_df_sorted = log_df.sort_values(by=['user_id', 'timestamp'])

    # 4. DataFrame을 XLSX 파일로 저장하도록 변경
    # --- pip install openpyxl 명령어로 라이브러리가 설치되어 있어야 합니다. ---
    OUTPUT_LOG_FILE = 'synthetic_event_logs_by_user.xlsx'
    
    try:
        # 엑셀 파일로 저장 (Sheet 이름을 지정할 수 있음)
        log_df_sorted.to_excel(
            OUTPUT_LOG_FILE, 
            sheet_name='User_Event_Logs', 
            index=False # <--- 이 뒤의 'encoding'만 제거하면 돼!
        )

        print(f"✅ 유저별로 정리된 이벤트 로그가 '{OUTPUT_LOG_FILE}' (XLSX) 파일로 저장되었습니다. (총 {len(log_df_sorted)}개)")
    
    except ImportError:
        print("\n❌ 에러: XLSX 파일 저장을 위해 'openpyxl' 라이브러리가 필요합니다.")
        print("    터미널에서 'pip install openpyxl' 명령어를 실행해주세요.")
        
    # 5. 콘솔에 JSON 형식으로 출력 (디버깅 용이)
    print("\n--- 콘솔 JSON 출력 (상위 5개 이벤트) ---")
    print(json.dumps(generated_data[:5], indent=2, ensure_ascii=False, default=convert_to_python_native))