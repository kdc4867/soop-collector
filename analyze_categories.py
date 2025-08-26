# analyze_categories.py
import re
import pandas as pd
from pathlib import Path

WIDE_FILE = Path("data/soop/categories_matrix.csv")
OUT_FILE  = Path("data/soop/categories/top_latest.csv")

TIME_COL_PAT = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:00:00Z$")  # ISO 정시

def analyze():
    if not WIDE_FILE.exists():
        print("categories_matrix.csv가 없습니다.")
        return

    # dtype 고정 + 인덱스 유연 처리
    wide = pd.read_csv(WIDE_FILE, dtype={"category_no": str}, encoding="utf-8-sig")

    # 멀티인덱스로 저장된 경우 대비: 인덱스가 포함돼 있으면 reset해서 컬럼화
    if "category_no" not in wide.columns:
        wide = wide.reset_index()

    # category_name이 인덱스에 있었던 경우도 대비
    if "category_name" not in wide.columns:
        # 흔히 첫 두 인덱스가 category_no, category_name인 케이스
        # 이미 category_no는 위에서 보장했으므로, 없으면 빈값 채움
        wide["category_name"] = wide.get("category_name", pd.Series([""] * len(wide)))

    # 시간 컬럼만 골라내기
    time_cols = [c for c in wide.columns if TIME_COL_PAT.match(str(c))]
    if not time_cols:
        print("시간 컬럼을 찾지 못했습니다.")
        return

    # 최신 시각 선택
    # (문자열 정렬도 가능하지만 혹시 모를 예외 대비해 datetime으로 정렬)
    time_cols_sorted = sorted(
        time_cols,
        key=lambda s: pd.to_datetime(s, utc=True, errors="coerce")
    )
    latest_col = time_cols_sorted[-1]

    # 최신 스냅샷 기준 랭킹
    # category_no를 인덱스로, 이름은 컬럼으로 둠
    view_series = pd.to_numeric(wide[latest_col], errors="coerce").fillna(0).astype("Int64")
    out_df = pd.DataFrame({
        "category_no": wide["category_no"].astype(str),
        "category_name": wide["category_name"].astype(str),
        latest_col: view_series
    })

    # 동일 category_no 중복(이론상 거의 없음) 방지: 마지막 값 유지
    out_df = (out_df
              .drop_duplicates(subset=["category_no"], keep="last")
              .set_index("category_no")
              .sort_values(latest_col, ascending=False))

    OUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    out_df.to_csv(OUT_FILE, encoding="utf-8-sig")
    print(f"Saved top_latest.csv from {latest_col}")

if __name__ == "__main__":
    analyze()