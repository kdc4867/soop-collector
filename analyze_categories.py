import pandas as pd
from pathlib import Path

WIDE_FILE = Path("data/soop/categories_matrix.csv")

def analyze():
    wide = pd.read_csv(WIDE_FILE, encoding="utf-8-sig").set_index("category_no")
    time_cols = [c for c in wide.columns if c != "category_name"]
    if not time_cols:
        print("시간 컬럼이 없습니다.")
        return
    latest_col = sorted(time_cols)[-1]
    latest = wide[["category_name", latest_col]].sort_values(latest_col, ascending=False)
    out = Path("data/soop/categories/top_latest.csv")
    out.parent.mkdir(parents=True, exist_ok=True)
    latest.to_csv(out, encoding="utf-8-sig")
    print(f"Saved top_latest.csv from {latest_col}")

if __name__ == "__main__":
    analyze()