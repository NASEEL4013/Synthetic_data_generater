import pandas as pd
import random
import uuid
import json
from datetime import datetime, timedelta 
import sys 

# ----------------------------------------------------
# 1. Config 클래스: 모든 규칙과 확률 정의
# ----------------------------------------------------
class Config:
    """
    데이터 생성에 필요한 모든 기본 설정(고정값, 확률)을 관리하는 클래스.
    """
    # --- (기존 GENDER_RATIO, USER_INITIAL_LOGIN_RATIO 등 유지) ---
    GENDER_RATIO = {
        '여성': 0.7,
        '남성': 0.3
    }
    USER_INITIAL_LOGIN_RATIO = {
        'login': 0.95,
        'not_login': 0.05
    }

    SESSION_FREQUENCY_TIERS = {
        'High': 0.6,
        'Medium': 0.3,
        'Low': 0.1
    }

    PROB_ON_LOGIN_ATTEMPT = {
        'login_success': 0.9,
        'drop-off': 0.1  # 'out' -> 'drop-off'
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
        'order_detail': 0.8,
        'mainpage' : 0.2
    }
    PROB_MYPAGE_NOT_LOGIN = {
        'login': 0.9,
        'mainpage' : 0.1
    }
    PROB_SEARCH = {
        'search_text': 0.3,
        'view_recommended_item': 0.7
    }
    PROB_ORDER_DETAIL = {
        'mainpage': 0.1,
        'drop-off': 0.9  
    }
    PROB_ACTION_AFTER_PROMOTION = {
        'mainpage': 0.9,
        'drop-off' : 0.1
    }
    PROB_RECOMMANDED_ITEM = {
        'item': 0.3,
        'mainpage': 0.6,
        'drop-off': 0.1  
    }
    PROB_VIEW_ITEM_LIST = {
        'click_item': 0.95,
        'drop-off': 0.05  
    }
    PROB_VIEW_ITEM_NOT_LOGIN = {
        'login': 0.5,
        'drop-off': 0.5  
    }
    PROB_VIEW_ITEM_LOGIN = {
        'add_to_cart': 0.2,
        'drop-off': 0.2,
        'buy_baro': 0.1,
        'purchase': 0.1,
        'return_item_list' : 0.4
    }
    PROB_ACTION_AFTER_ADD_TO_CART = {
        'view_cart': 0.6,
        'return_mainpage': 0.05,
        'return_item_list': 0.35
    }
    PROB_ACTION_AFTER_VIEW_CART = {
        'purchase': 0.3,
        'abandon': 0.35,
        'return_mainpage' : 0.3,
        'drop-off': 0.05  

    }
    PROB_BARO_SHOP = {
        'choose_shop': 0.7,
        'drop-off': 0.1,
        'return_item_list' : 0.2
    }
    PROB_BARO_VISIT = {
        'choose_visit': 1
    }
    PROB_BARO_PURCHASE = {
        'purchase': 0.95,
        'drop-off': 0.05  # 'out' -> 'drop-off'
    }
    PROB_PURCHASE = {
        'purchase': 0.95,
        'drop-off': 0.05  # 'out' -> 'drop-off'
    }
    PROB_PURCHASE_CLEAR = {
        'return_mainpage': 0.15,
        'order_detail': 0.6,
        'drop-off': 0.25  # 'out' -> 'drop-off'
    }

    # --- (기존 TIME_DELAY_SECONDS 유지) ---
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
        'PROB_PURCHASE_CLEAR': (5, 10)
    }

# ----------------------------------------------------
# 2. 메인 데이터 생성기 클래스
# ----------------------------------------------------
class SyntheticDataGenerator:
    def __init__(self, config, book_db, input_data, user_pool_path='user_pool.csv'):
        self.config = config
        self.book_db = book_db
        
        # 기간 및 세션 수 데이터 로드
        self.total_sessions = input_data.get('total_sessions', 100)
        self.start_date = datetime.strptime(input_data['start_date'], '%Y-%m-%d')
        self.end_date = datetime.strptime(input_data['end_date'], '%Y-%m-%d')
        
        # user_pool 로드
        try:
            self.user_pool = pd.read_csv(user_pool_path)
            print(f"✅ 사용자 풀 ('{user_pool_path}') 로딩 성공!")
        except FileNotFoundError:
            print(f"⚠️ 사용자 풀 ('{user_pool_path}')을 찾을 수 없습니다. 프로그램을 종료합니다.")
            sys.exit(0)

        # 샘플링된 유저 풀 생성
        self.users_to_sample = input_data.get('users_to_sample', len(self.user_pool))
        if self.users_to_sample < len(self.user_pool):
            self.sampled_user_pool = self.user_pool.sample(n=self.users_to_sample, replace=False) 
        else:
            self.sampled_user_pool = self.user_pool
            
        # --- [삭제] 1. 프로필 가중치 계산 (profile 컬럼 없음) ---
        # profile_weights = [] ... (관련 로직 전체 삭제)
        
        # --- [수정] 2. 활동 빈도 티어 할당 및 최종 세션 가중치 계산 ---
        # (프로필 가중치 없이 활동 빈도만으로 계산)
        tiers = list(self.config.SESSION_FREQUENCY_TIERS.keys())
        tier_weights = list(self.config.SESSION_FREQUENCY_TIERS.values())
        
        assigned_tiers = random.choices(tiers, weights=tier_weights, k=len(self.sampled_user_pool))
        self.sampled_user_pool['frequency_tier'] = assigned_tiers
        
        frequency_map = self.config.SESSION_FREQUENCY_TIERS
        
        # [수정] profile_weights 없이 frequency_map만으로 가중치 리스트 생성
        self.session_weights = [
            frequency_map[self.sampled_user_pool.iloc[i]['frequency_tier']]
            for i in range(len(self.sampled_user_pool))
        ]
        
        # 최종 가중치 정규화
        weight_sum = sum(self.session_weights)
        if weight_sum > 0:
            self.session_weights = [w / weight_sum for w in self.session_weights]
        else:
            self.session_weights = [1.0 / len(self.sampled_user_pool)] * len(self.sampled_user_pool)
            

    def _get_random_user(self):
        """
        활동 빈도에 맞춰 sampled_user_pool에서 사용자 1명을 선택합니다.
        """
        selected_user_row = self.sampled_user_pool.sample(n=1, weights=self.session_weights).iloc[0]
        
        login_type = random.choices(
            list(self.config.USER_INITIAL_LOGIN_RATIO.keys()), 
            weights=list(self.config.USER_INITIAL_LOGIN_RATIO.values()), k=1
        )[0]
        
        return {
            'user_id': selected_user_row['user_id'],
            'gender': selected_user_row['gender'],
            'age': selected_user_row['age'],
            # 'profile': selected_user_row['profile'], # <- [삭제] profile 컬럼 없음
            'initial_login_status': (login_type == 'login')
        }

    # _get_next_action (변경 없음)
    def _get_next_action(self, prob_dict):
        return random.choices(list(prob_dict.keys()), weights=list(prob_dict.values()), k=1)[0]

    # _generate_event (변경 없음)
    def _generate_event(self, event_name, session_id, user_id, current_time, properties={}):
        return {
            'event_name': event_name,
            'session_id': session_id,
            'user_id': user_id,
            'timestamp': current_time.isoformat(),
            'properties': properties
        }

    # generate_sessions (변경 없음)
    def generate_sessions(self):
        all_event_logs = []
        
        print(f"총 {self.total_sessions}개의 세션을 {self.start_date.date()}부터 {self.end_date.date()}까지 생성합니다...")

        time_span = self.end_date - self.start_date
        if self.total_sessions > 0:
            time_step = time_span / self.total_sessions
        else:
            time_step = timedelta(0)
        
        for i in range(self.total_sessions):
            max_noise_sec = int(time_step.total_seconds() * 0.1) if time_step.total_seconds() > 0 else 0
            session_start_offset = time_step * i + timedelta(seconds=random.randint(0, max(0, max_noise_sec)))
            session_start_time = self.start_date + session_start_offset
            
            session_events = self._create_one_session(session_start_time)
            all_event_logs.extend(session_events)
            
        print(f"총 {len(all_event_logs)}개의 이벤트 로그가 생성되었습니다.")
        return all_event_logs

    # _create_one_session (버그 수정된 최종본 유지 - 변경 없음)
    def _create_one_session(self, session_start_time):
        user = self._get_random_user()
        
        # --- [수정] 세션 ID 생성 방식 ---
        date_str = session_start_time.strftime('%Y%m%d')
        random_part = f"{random.randint(0, 99999999):08d}"
        session_id = f"s{date_str}_{random_part}"
        # --- [수정 끝] ---
        
        event_logs = []
        is_logged_in = user['initial_login_status']
        
        current_time = session_start_time
        
        # 1. App Launch 이벤트
        event_logs.append(self._generate_event('App Launch', session_id, user['user_id'], current_time))
        
        # 2. View Main Page 이벤트
        min_sec, max_sec = self.config.TIME_DELAY_SECONDS.get('default')
        current_time += timedelta(seconds=random.uniform(min_sec, max_sec))
        event_logs.append(self._generate_event('View Main Page', session_id, user['user_id'], current_time, {'is_logged_in': is_logged_in}))
        
        current_rule_name = 'PROB_MAINPAGE_LOGIN' if is_logged_in else 'PROB_MAINPAGE_NOT_LOGIN'
        
        while True:
            # 1. 현재 페이지(상태)의 확률 사전을 가져옴
            prob_dict = getattr(self.config, current_rule_name)
            
            # 2. 해당 페이지에서 할 행동(Action)을 선택
            chosen_action = self._get_next_action(prob_dict)

            # 3. 지연 시간 계산 (현재 페이지 기준)
            delay_range = self.config.TIME_DELAY_SECONDS.get(current_rule_name, self.config.TIME_DELAY_SECONDS['default'])
            delay_seconds = random.uniform(*delay_range)
            current_time += timedelta(seconds=delay_seconds)
            event_properties = {
                'time_spent_sec': round(delay_seconds, 2) 
            }

            # 4. "현재 페이지(current_rule_name)"를 먼저 로그로 기록
            event_logs.append(self._generate_event(current_rule_name, session_id, user['user_id'], current_time, event_properties))
            
            # 5. [수정] 'drop-off' 처리 (재접속 또는 종료)
            if chosen_action == 'drop-off':
                # 5a. 'drop-off' 이벤트 기록
                current_time += timedelta(seconds=1) # 1초 추가
                event_logs.append(self._generate_event('drop-off', session_id, user['user_id'], current_time, {}))
                
                if random.random() < 0.5: # 50% 확률로 재접속
                    # 5b. 재접속 이벤트 기록
                    reconnect_delay_range = self.config.TIME_DELAY_SECONDS.get('default')
                    reconnect_delay_sec = random.uniform(*reconnect_delay_range) + 5.0
                    current_time += timedelta(seconds=reconnect_delay_sec)
                    event_logs.append(self._generate_event('Reconnect_Session', session_id, user['user_id'], current_time, {'is_logged_in': is_logged_in}))
                    
                    # 5c. current_rule_name을 변경하지 않고 continue
                    continue 
                else:
                    break # 세션 종료

            # 6. 'drop-off'가 아닐 때: "다음 루프의 페이지(상태)"를 결정
            if chosen_action == 'login_success':
                is_logged_in = True
                current_rule_name = 'PROB_MAINPAGE_LOGIN'
            # ... (나머지 if/elif 블록은 동일하게 유지) ...
            elif chosen_action == 'login':
                current_rule_name = 'PROB_ON_LOGIN_ATTEMPT'
            elif chosen_action == 'mypage':
                current_rule_name = 'PROB_MYPAGE_LOGIN'
            elif chosen_action in ['search', 'search_text', 'view_recommended_item', 'return_item_list']:
                current_rule_name = 'PROB_VIEW_ITEM_LIST'
            elif chosen_action in ['item', 'click_item']:
                current_rule_name = 'PROB_VIEW_ITEM_LOGIN'
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
            elif chosen_action in ['mainpage', 'return_mainpage', 'return_item_list', 'abandon', 'promotion', 'recommand']:
                current_rule_name = 'PROB_MAINPAGE_LOGIN' if is_logged_in else 'PROB_MAINPAGE_NOT_LOGIN'
            else:
                print(f"⚠️ 경고: 알 수 없는 chosen_action '{chosen_action}' (from {current_rule_name}). 세션을 종료합니다.")
                break
                
        return event_logs

# ----------------------------------------------------
# 4. 테스트 코드 (사용자 입력 및 XLSX 저장 로직)
# ----------------------------------------------------

# JSON 직렬화 에러 방지 함수
def convert_to_python_native(obj):
    if obj.__class__.__name__ in ['int64', 'int32', 'int16']:
        return int(obj)
    if isinstance(obj, datetime):
        return obj.isoformat()
    raise TypeError(f'Object of type {obj.__class__.__name__} is not JSON serializable')

if __name__ == '__main__':
    print("--- 📊 합성 데이터 생성기 시작 ---")
    
    # --- 사용자에게 input_data를 직접 입력받는 로직 ---
    try:
        total_sessions = int(input("1. 총 생성할 세션 수 (Total Sessions): "))
        users_to_sample = int(input("2. 세션에 참여시킬 유저 수 (Users to Sample): "))
        start_date_str = input("3. 생성 시작 날짜 (YYYY-MM-DD): ")
        end_date_str = input("4. 생성 종료 날짜 (YYYY-MM-DD): ")
        
        # 유효성 검사
        start_date_check = datetime.strptime(start_date_str, '%Y-%m-%d')
        end_date_check = datetime.strptime(end_date_str, '%Y-%m-%d')
        
        if end_date_check <= start_date_check:
            print("⚠️ 오류: 종료 날짜는 시작 날짜보다 늦어야 합니다. 프로그램을 종료합니다.")
            sys.exit()

    except ValueError:
        print("⚠️ 오류: 세션 수와 유저 수는 정수여야 하며, 날짜 형식(YYYY-MM-DD)을 확인해주세요. 프로그램을 종료합니다.")
        sys.exit()
        
    # --- Input 데이터 딕셔너리 생성 ---
    test_input = {
        'total_sessions': total_sessions,
        'users_to_sample': users_to_sample,
        'start_date': start_date_str,
        'end_date': end_date_str,
    }

    # --- 객체 생성 ---
    config = Config()
    
    try:
        # --- [수정] 파일명 변경 ---
        book_db = pd.read_csv('biblio_data_filtered.csv')
        print("✅ 서적 DB ('biblio_data_filtered.csv') 로딩 성공!")
    except FileNotFoundError:
        print("⚠️ 'biblio_data_filtered.csv'을 찾을 수 없습니다. (경고: 실행은 계속됩니다)")
        book_db = pd.DataFrame() 
        
    # --- 생성기 실행 ---
    generator = SyntheticDataGenerator(config, book_db, test_input, user_pool_path='user_pool.csv') 
    generated_data = generator.generate_sessions()

    # --- 결과 출력 및 XLSX 저장 ---
    print("\n--- 생성된 전체 세션 데이터 ---")
    
    # 1. generated_data를 DataFrame으로 변환
    log_df = pd.DataFrame(generated_data)

    # 2. 'properties' 딕셔너리를 별도 컬럼으로 분리
    if 'properties' in log_df.columns and not log_df['properties'].isnull().all():
        properties_df = pd.json_normalize(log_df['properties'])
        log_df = pd.concat([log_df.drop('properties', axis=1), properties_df], axis=1)

    # 3. user_id와 timestamp를 기준으로 정렬
    log_df_sorted = log_df.sort_values(by=['user_id', 'timestamp'])

    # 4. DataFrame을 XLSX 파일로 저장
    OUTPUT_LOG_FILE = 'synthetic_event_logs_by_user.xlsx'
    
    try:
        log_df_sorted.to_excel(
            OUTPUT_LOG_FILE, 
            sheet_name='User_Event_Logs', 
            index=False 
        )

        print(f"✅ 유저별로 정리된 이벤트 로그가 '{OUTPUT_LOG_FILE}' (XLSX) 파일로 저장되었습니다. (총 {len(log_df_sorted)}개)")
    
    except ImportError:
        print("\n❌ 에러: XLSX 파일 저장을 위해 'openpyxl' 라이브러리가 필요합니다.")
        print("    터미널에서 'pip install openpyxl' 명령어를 실행해주세요.")
    except Exception as e:
        print(f"\n❌ XLSX 파일 저장 중 예상치 못한 오류 발생: {e}")
        
    # 5. 콘솔에 JSON 형식으로 출력 (상위 5개 이벤트)
    print("\n--- 콘솔 JSON 출력 (상위 5개 이벤트) ---")
    print(json.dumps(generated_data[:5], indent=2, ensure_ascii=False, default=convert_to_python_native))