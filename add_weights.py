import pandas as pd
import random

INPUT_FILE = "biblio_data_filtered.csv"
OUTPUT_FILE = "biblio_data_with_weights.csv"
NUM_BESTSELLERS = 30    # 베스트셀러로 지정할 책의 수
BESTSELLER_WEIGHT = 50  # 베스트셀러 가중치 (일반 책보다 50배)
DEFAULT_WEIGHT = 1      # 일반 책 가중치

try:
    df = pd.read_csv(INPUT_FILE)
    print(f"'{INPUT_FILE}' 로드 성공. (총 {len(df)}권)")

    if len(df) < NUM_BESTSELLERS:
        NUM_BESTSELLERS = len(df)
        print(f"책이 30권보다 적어서 {NUM_BESTSELLERS}권을 베스트셀러로 지정합니다.")

    # 1. 모든 책에 기본 가중치 1 부여
    df['purchase_weight'] = DEFAULT_WEIGHT

    # 2. 30권 랜덤 샘플링
    bestseller_indices = df.sample(n=NUM_BESTSELLERS, replace=False).index

    # 3. 30권에 높은 가중치(50) 부여
    df.loc[bestseller_indices, 'purchase_weight'] = BESTSELLER_WEIGHT

    print(f"'{OUTPUT_FILE}' 생성 중... (베스트셀러 {NUM_BESTSELLERS}권 포함)")
    
    # 4. 새 파일로 저장
    df.to_csv(OUTPUT_FILE, index=False, encoding='utf-8-sig')

    print(f"✅ 완료! '{OUTPUT_FILE}' 파일이 저장되었습니다.")
    print("\n--- 가중치 적용 샘플 (상위 5개) ---")
    print(df.head())
    print("\n--- 베스트셀러 샘플 (가중치 50) ---")
    print(df[df['purchase_weight'] == BESTSELLER_WEIGHT].head(5))

except FileNotFoundError:
    print(f"⚠️ 오류: '{INPUT_FILE}' 파일을 찾을 수 없습니다. a.py와 같은 폴더에 있는지 확인하세요.")
except Exception as e:
    print(f"오류 발생: {e}")