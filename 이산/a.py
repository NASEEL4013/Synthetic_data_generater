import pandas as pd
import random
import json
import sys
from datetime import datetime, timedelta 

# ----------------------------------------------------
# 1. Config 클래스: 규칙 및 확률 정의
# ----------------------------------------------------
class Config:
    """
    데이터 생성에 필요한 모든 기본 설정(고정값, 확률)을 관리하는 클래스.
    """
    GENDER_RATIO = {'여성': 0.7, '남성': 0.3}
    USER_INITIAL_LOGIN_RATIO = {'login': 0.95, 'not_login': 0.05}

    # 사용자 활동 빈도 티어 (세션 할당 가중치)
    SESSION_FREQUENCY_TIERS = {'High': 0.6, 'Medium': 0.3, 'Low': 0.1}

    # 행동 시나리오 확률
    PROB_ON_LOGIN_ATTEMPT = {'login_success': 0.9, 'drop-off': 0.1}
    
    PROB_MAINPAGE_NOT_LOGIN = {
        'search': 0.5, 'recommand': 0.1, 'promotion': 0.35, 'login': 0.05
    }
    PROB_MAINPAGE_LOGIN = {
        'search': 0.5, 'recommand': 0.1, 'promotion': 0.35, 'mypage': 0.05
    }
    
    PROB_MYPAGE_LOGIN = {'order_detail': 0.8, 'mainpage' : 0.2}
    PROB_MYPAGE_NOT_LOGIN = {'login': 0.9, 'mainpage' : 0.1}
    
    PROB_SEARCH = {
        'search_text': 0.3, 'view_recommended_item': 0.5, 'return_mainpage': 0.2
    }
    PROB_ORDER_DETAIL = {'mainpage': 0.1, 'drop-off': 0.9}
    
    PROB_ACTION_AFTER_PROMOTION = {'mainpage': 0.9, 'drop-off' : 0.1}
    
    PROB_RECOMMANDED_ITEM = {'item': 0.3, 'mainpage': 0.6, 'drop-off': 0.1}
    
    PROB_VIEW_ITEM_LIST = {'click_item': 0.95, 'drop-off': 0.05}
    
    PROB_VIEW_ITEM_NOT_LOGIN = {'login': 0.5, 'drop-off': 0.5}
    PROB_VIEW_ITEM_LOGIN = {
        'add_to_cart': 0.2, 'drop-off': 0.2, 'buy_baro': 0.1, 
        'purchase': 0.1, 'return_item_list' : 0.4
    }
    
    PROB_ACTION_AFTER_ADD_TO_CART = {
        'view_cart': 0.6, 'return_mainpage': 0.05, 'return_item_list': 0.35
    }
    PROB_ACTION_AFTER_VIEW_CART = {
        'purchase': 0.3, 'abandon': 0.35, 'return_mainpage' : 0.3, 'drop-off': 0.05
    }
    
    PROB_BARO_SHOP = {'choose_shop': 0.7, 'drop-off': 0.1, 'return_item_list' : 0.2}
    PROB_BARO_VISIT = {'choose_visit': 1}
    PROB_BARO_PURCHASE = {'purchase': 0.95, 'drop-off': 0.05}
    
    PROB_PURCHASE = {'purchase': 0.95, 'drop-off': 0.05}
    PROB_PURCHASE_CLEAR = {'return_mainpage': 0.15, 'order_detail': 0.6, 'drop-off': 0.25}

    # 이벤트/페이지별 체류 시간 (초 단위)
    TIME_DELAY_SECONDS = {
        'default': (1, 3), 
        'PROB_MAINPAGE_LOGIN': (3, 7),
        'PROB_MAINPAGE_NOT_LOGIN': (2, 5),
        'PROB_SEARCH': (5, 12),
        'PROB_VIEW_ITEM_LIST': (8, 15),
        'PROB_VIEW_ITEM_LOGIN': (15, 30),
        'PROB_RECOMMANDED_ITEM': (4, 10),
        'PROB_MYPAGE_LOGIN': (7, 15),
        'PROB_ORDER_DETAIL': (10, 20),
        'PROB_ACTION_AFTER_VIEW_CART': (10, 25),
        'PROB_PURCHASE_CLEAR': (5, 10),
        'PROB_ACTION_AFTER_PROMOTION': (3, 8) 
    }

# ----------------------------------------------------
# 2. 메인 데이터 생성기 클래스
# ----------------------------------------------------
class SyntheticDataGenerator:
    def __init__(self, config, book_db, input_data, user_pool_path='user_pool.csv'):
        self.config = config
        
        # 서적 DB 로드 및 검증
        self.book_db = book_db
        if not self.book_db.empty:
            if 'purchase_weight' not in self.book_db.columns:
                print("⚠️ 경고: book_db에 'purchase_weight' 컬럼이 없습니다. 가중치를 1로 설정합니다.")
                self.book_db['purchase_weight'] = 1 
        
        # 설정 데이터 로드
        self.total_sessions = input_data.get('total_sessions', 100)
        self.start_date = datetime.strptime(input_data['start_date'], '%Y-%m-%d')
        self.end_date = datetime.strptime(input_data['end_date'], '%Y-%m-%d')
        
        # User Pool 로드
        try:
            self.user_pool = pd.read_csv(user_pool_path)
            print(f"✅ 사용자 풀 ('{user_pool_path}') 로딩 성공!")
        except FileNotFoundError:
            print(f"⚠️ 사용자 풀 ('{user_pool_path}')을 찾을 수 없습니다. 종료합니다.")
            sys.exit(0)

        # 유저 샘플링
        self.users_to_sample = input_data.get('users_to_sample', len(self.user_pool))
        if self.users_to_sample < len(self.user_pool):
            self.sampled_user_pool = self.user_pool.sample(n=self.users_to_sample, replace=False) 
        else:
            self.sampled_user_pool = self.user_pool
            
        # 활동 빈도 티어 할당
        tiers = list(self.config.SESSION_FREQUENCY_TIERS.keys())
        tier_weights = list(self.config.SESSION_FREQUENCY_TIERS.values())
        
        assigned_tiers = random.choices(tiers, weights=tier_weights, k=len(self.sampled_user_pool))
        self.sampled_user_pool['frequency_tier'] = assigned_tiers
        
        # 세션 할당 가중치 계산
        frequency_map = self.config.SESSION_FREQUENCY_TIERS
        self.session_weights = [
            frequency_map[self.sampled_user_pool.iloc[i]['frequency_tier']]
            for i in range(len(self.sampled_user_pool))
        ]
        
        # 가중치 정규화
        weight_sum = sum(self.session_weights)
        if weight_sum > 0:
            self.session_weights = [w / weight_sum for w in self.session_weights]
        else:
            self.session_weights = [1.0 / len(self.sampled_user_pool)] * len(self.sampled_user_pool)
            

    def _get_random_user(self):
        """가중치에 따라 유저 1명 선택"""
        selected_user_row = self.sampled_user_pool.sample(n=1, weights=self.session_weights).iloc[0]
        
        login_type = random.choices(
            list(self.config.USER_INITIAL_LOGIN_RATIO.keys()), 
            weights=list(self.config.USER_INITIAL_LOGIN_RATIO.values()), k=1
        )[0]
        
        return {
            'user_id': selected_user_row['user_id'],
            'gender': selected_user_row['gender'],
            'age': selected_user_row['age'],
            'initial_login_status': (login_type == 'login')
        }

    def _get_next_action(self, prob_dict):
        return random.choices(list(prob_dict.keys()), weights=list(prob_dict.values()), k=1)[0]

    def _generate_event(self, event_name, session_id, user_id, current_time, event_sequence, properties={}):
        return {
            'event_name': event_name,
            'session_id': session_id,
            'user_id': user_id,
            'timestamp': current_time.isoformat(),
            'event_sequence': event_sequence,
            'properties': properties
        }

    def generate_sessions(self):
        all_event_logs = []
        print(f"총 {self.total_sessions}개의 세션을 {self.start_date.date()} ~ {self.end_date.date()} 기간 동안 생성합니다.")

        time_span = self.end_date - self.start_date
        time_step = time_span / self.total_sessions if self.total_sessions > 0 else timedelta(0)
        
        for i in range(self.total_sessions):
            max_noise_sec = int(time_step.total_seconds() * 0.1) if time_step.total_seconds() > 0 else 0
            session_start_offset = time_step * i + timedelta(seconds=random.randint(0, max(0, max_noise_sec)))
            session_start_time = self.start_date + session_start_offset
            
            session_events = self._create_one_session(session_start_time)
            all_event_logs.extend(session_events)
            
        print(f"총 {len(all_event_logs)}개의 이벤트 로그가 생성되었습니다.")
        return all_event_logs

    def _create_one_session(self, session_start_time):
        user = self._get_random_user()
        
        # 세션 ID 생성 (sYYYYMMDD_8자리)
        date_str = session_start_time.strftime('%Y%m%d')
        random_part = f"{random.randint(0, 99999999):08d}"
        session_id = f"s{date_str}_{random_part}"
        
        event_logs = []
        is_logged_in = user['initial_login_status']
        current_time = session_start_time
        session_context = {}
        event_sequence = 1
        
        # 1. App Launch
        event_logs.append(self._generate_event('App Launch', session_id, user['user_id'], current_time, event_sequence))
        event_sequence += 1
        
        # 2. View Main Page
        min_sec, max_sec = self.config.TIME_DELAY_SECONDS.get('default')
        current_time += timedelta(seconds=random.uniform(min_sec, max_sec))
        event_logs.append(self._generate_event('View Main Page', session_id, user['user_id'], current_time, event_sequence, {'is_logged_in': is_logged_in}))
        event_sequence += 1
        
        current_rule_name = 'PROB_MAINPAGE_LOGIN' if is_logged_in else 'PROB_MAINPAGE_NOT_LOGIN'
        
        while True:
            prob_dict = getattr(self.config, current_rule_name)
            chosen_action = self._get_next_action(prob_dict)
            
            delay_range = self.config.TIME_DELAY_SECONDS.get(current_rule_name, self.config.TIME_DELAY_SECONDS['default'])
            delay_seconds = random.uniform(*delay_range)
            current_time += timedelta(seconds=delay_seconds)
            
            event_properties = {'time_spent_sec': round(delay_seconds, 2)}

            # 선택된 책 정보가 있으면 Properties에 추가
            if 'current_book' in session_context:
                # 책 정보가 유지되어야 하는 페이지들
                if current_rule_name in [
                    'PROB_VIEW_ITEM_LOGIN', 'PROB_ACTION_AFTER_ADD_TO_CART', 
                    'PROB_ACTION_AFTER_VIEW_CART', 'PROB_PURCHASE_CLEAR',
                    'PROB_BARO_SHOP', 'PROB_BARO_VISIT', 'PROB_BARO_PURCHASE'
                ]:
                    book = session_context['current_book']
                    event_properties['item_id'] = book.get('ID', book.get('Id', None))
                    event_properties['item_title'] = book.get('제목', None)
                    event_properties['item_price'] = book.get('가격', None)
                    event_properties['item_category'] = book.get('카테고리', None)

                # 책 정보 컨텍스트 해제 (메인이나 리스트로 돌아갈 때)
                if current_rule_name in ['PROB_MAINPAGE_LOGIN', 'PROB_MAINPAGE_NOT_LOGIN', 'PROB_VIEW_ITEM_LIST']:
                    del session_context['current_book']

            # 현재 페이지 로그 기록
            event_logs.append(self._generate_event(current_rule_name, session_id, user['user_id'], current_time, event_sequence, event_properties))
            event_sequence += 1 
            
            # Drop-off 처리
            if chosen_action == 'drop-off':
                current_time += timedelta(seconds=1) 
                event_logs.append(self._generate_event('drop-off', session_id, user['user_id'], current_time, event_sequence, {}))
                event_sequence += 1 
                
                if random.random() < 0.5: # 50% 확률 재접속
                    reconnect_delay_range = self.config.TIME_DELAY_SECONDS.get('default')
                    reconnect_delay_sec = random.uniform(*reconnect_delay_range) + 5.0
                    current_time += timedelta(seconds=reconnect_delay_sec)
                    event_logs.append(self._generate_event('Reconnect_Session', session_id, user['user_id'], current_time, event_sequence, {'is_logged_in': is_logged_in}))
                    event_sequence += 1 
                    continue 
                else:
                    break 

            # 다음 상태 결정 로직
            if chosen_action in ['click_item', 'item']: # 검색결과 클릭 or 추천상품 클릭
                if not self.book_db.empty and 'purchase_weight' in self.book_db.columns:
                    selected_book_row = self.book_db.sample(n=1, weights='purchase_weight').iloc[0]
                    session_context['current_book'] = selected_book_row.to_dict()
                current_rule_name = 'PROB_VIEW_ITEM_LOGIN'

            elif chosen_action == 'login_success':
                is_logged_in = True
                current_rule_name = 'PROB_MAINPAGE_LOGIN'
            elif chosen_action == 'login':
                current_rule_name = 'PROB_ON_LOGIN_ATTEMPT'
            elif chosen_action == 'mypage':
                current_rule_name = 'PROB_MYPAGE_LOGIN'
            elif chosen_action in ['search', 'search_text', 'view_recommended_item']:
                current_rule_name = 'PROB_VIEW_ITEM_LIST'
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
            elif chosen_action == 'order_detail':
                current_rule_name = 'PROB_ORDER_DETAIL'
            elif chosen_action == 'promotion':
                current_rule_name = 'PROB_ACTION_AFTER_PROMOTION' 
            elif chosen_action in ['mainpage', 'return_mainpage', 'return_item_list', 'abandon', 'recommand']:
                if 'current_book' in session_context and chosen_action in ['return_item_list', 'return_mainpage']:
                    del session_context['current_book']
                current_rule_name = 'PROB_MAINPAGE_LOGIN' if is_logged_in else 'PROB_MAINPAGE_NOT_LOGIN'
            else:
                print(f"⚠️ 경고: 알 수 없는 chosen_action '{chosen_action}'. 세션을 종료합니다.")
                break
                
        return event_logs

# ----------------------------------------------------
# 3. 메인 실행 코드
# ----------------------------------------------------
def convert_to_python_native(obj):
    if obj.__class__.__name__ in ['int64', 'int32', 'int16']: return int(obj)
    if isinstance(obj, datetime): return obj.isoformat()
    raise TypeError(f'Object of type {obj.__class__.__name__} is not JSON serializable')

if __name__ == '__main__':
    print("--- 📊 합성 데이터 생성기 시작 ---")
    
    # 사용자 입력
    try:
        total_sessions = int(input("1. 총 생성할 세션 수 (Total Sessions): "))
        users_to_sample = int(input("2. 세션에 참여시킬 유저 수 (Users to Sample): "))
        start_date_str = input("3. 생성 시작 날짜 (YYYY-MM-DD): ")
        end_date_str = input("4. 생성 종료 날짜 (YYYY-MM-DD): ")
        
        start_date_check = datetime.strptime(start_date_str, '%Y-%m-%d')
        end_date_check = datetime.strptime(end_date_str, '%Y-%m-%d')
        if end_date_check <= start_date_check:
            print("⚠️ 오류: 종료 날짜는 시작 날짜보다 늦어야 합니다.")
            sys.exit()
    except ValueError:
        print("⚠️ 오류: 입력 형식을 확인해주세요.")
        sys.exit()
        
    test_input = {
        'total_sessions': total_sessions,
        'users_to_sample': users_to_sample,
        'start_date': start_date_str,
        'end_date': end_date_str,
    }

    config = Config()
    
    # 서적 DB 로드 (가중치 파일)
    try:
        book_db = pd.read_csv('BDB/biblio_data_with_weights.csv')
        print("✅ 서적 DB ('BDB/biblio_data_with_weights.csv') 로딩 성공!")
    except FileNotFoundError:
        print("⚠️ 'BDB/biblio_data_with_weights.csv'을 찾을 수 없습니다.")
        book_db = pd.DataFrame() 
        
    # 생성기 실행
    generator = SyntheticDataGenerator(config, book_db, test_input, user_pool_path='user_pool.csv') 
    generated_data = generator.generate_sessions()

    # 결과 저장
    print("\n--- 생성된 전체 세션 데이터 처리 중 ---")
    
    log_df = pd.DataFrame(generated_data)
    if 'properties' in log_df.columns and not log_df['properties'].isnull().all():
        properties_df = pd.json_normalize(log_df['properties'])
        log_df = pd.concat([log_df.drop('properties', axis=1), properties_df], axis=1)

    log_df_sorted = log_df.sort_values(by=['user_id', 'timestamp'])
    OUTPUT_LOG_FILE = 'synthetic_event_logs_by_user.xlsx'
    
    try:
        log_df_sorted.to_excel(OUTPUT_LOG_FILE, sheet_name='User_Event_Logs', index=False)
        print(f"✅ 저장 완료! '{OUTPUT_LOG_FILE}' (총 {len(log_df_sorted)}개 로그)")
    except Exception as e:
        print(f"\n❌ 저장 중 오류 발생: {e} (openpyxl 설치 여부를 확인하세요)")
        
    print("\n--- 콘솔 JSON 출력 (상위 5개) ---")
    print(json.dumps(generated_data[:5], indent=2, ensure_ascii=False, default=convert_to_python_native))