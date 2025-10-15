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
# 2. 사용자 생성 함수
# ----------------------------------------------------
def create_new_user(config, user_sequence):
    profile = random.choices(list(config.USER_PROFILES.keys()), weights=list(config.USER_PROFILES.values()), k=1)[0]
    gender = random.choices(list(config.GENDER_RATIO.keys()), weights=list(config.GENDER_RATIO.values()), k=1)[0]
    min_age, max_age = config.PROFILE_TO_AGE_RANGE[profile]
    age = random.randint(min_age, max_age)
    user_id = f"{user_sequence:08d}"
    login_type = random.choices(list(config.USER_INITIAL_LOGIN_RATIO.keys()), weights=list(config.USER_INITIAL_LOGIN_RATIO.values()), k=1)[0]
    
    return {
        'user_id': user_id,
        'gender': gender,
        'age': age,
        'profile': profile,
        'join_date': datetime.now().strftime('%Y-%m-%d'),
        'initial_login_status': (login_type == 'login')
    }

# ----------------------------------------------------
# 3. 메인 데이터 생성기 클래스
# ----------------------------------------------------
class SyntheticDataGenerator:
    def __init__(self, config, book_db, input_data):
        self.config = config
        self.book_db = book_db
        self.input_data = input_data
        self.user_counter = 1

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
        user = create_new_user(self.config, self.user_counter)
        self.user_counter += 1
        
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

            # 다음 행동 분기점을 결정하는 로직
            if chosen_action == 'login':
                current_rule_name = 'PROB_ON_LOGIN_ATTEMPT'
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
# 4. 테스트 코드
# ----------------------------------------------------
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
        exit(0);
    # --- 생성기 실행 ---
    generator = SyntheticDataGenerator(config, book_db, test_input)
    generated_data = generator.generate_sessions()

    # --- 결과 출력 ---
    print("\n--- 생성된 전체 세션 데이터 ---")
    print(json.dumps(generated_data, indent=2, ensure_ascii=False))