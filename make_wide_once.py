# make_wide_once.py
import pandas as pd
from pathlib import Path

ts_csv = Path("data/soop/categories_timeseries.csv")
out_csv = Path("data/soop/categories_matrix.csv")

if not ts_csv.exists():
    raise SystemExit("timeseries csv가 없습니다.")

df = pd.read_csv(ts_csv, encoding="utf-8-sig")
df["captured_at_utc"] = pd.to_datetime(df["captured_at_utc"], utc=True).dt.floor("H").astype(str)
df["view_cnt"] = pd.to_numeric(df["view_cnt"], errors="coerce").fillna(0).astype(int)

wide = df.pivot_table(index="category_no",
                      columns="captured_at_utc",
                      values="view_cnt",
                      aggfunc="last").sort_index()

name_map = (
    df.sort_values("captured_at_utc")
      .drop_duplicates(["category_no"], keep="last")
      .set_index("category_no")["category_name"]
)
wide.insert(0, "category_name", name_map.reindex(wide.index).fillna(""))

out_csv.parent.mkdir(parents=True, exist_ok=True)
wide.to_csv(out_csv, encoding="utf-8-sig")
print("written ->", out_csv)