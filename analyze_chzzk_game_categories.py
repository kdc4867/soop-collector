# analyze_chzzk_game_categories.py
import re
import pandas as pd
from pathlib import Path

WIDE_FILE = Path("data/chzzk/game_categories_matrix.csv")
OUT_DIR   = Path("data/chzzk/game_categories")
OUT_FILE  = OUT_DIR / "top_latest.csv"

TIME_COL_PAT = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:00:00Z$")

def analyze():
    if not WIDE_FILE.exists():
        print("game_categories_matrix.csv가 없습니다."); return

    wide = pd.read_csv(WIDE_FILE, dtype=str, encoding="utf-8-sig")

    time_cols = [c for c in wide.columns if TIME_COL_PAT.match(str(c))]
    if not time_cols:
        print("시간 컬럼이 없습니다."); return
    latest_col = sorted(time_cols, key=lambda s: pd.to_datetime(s, utc=True))[-1]

    wide[latest_col] = pd.to_numeric(wide[latest_col], errors="coerce").fillna(0).astype("Int64")
    out = (wide[["categoryType","categoryId","categoryValue", latest_col]]
           .sort_values(latest_col, ascending=False)
           .reset_index(drop=True))

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    out.to_csv(OUT_FILE, index=False, encoding="utf-8-sig")
    print(f"Saved {OUT_FILE} (latest={latest_col})")

if __name__ == "__main__":
    analyze()
