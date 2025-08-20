import pandas as pd
from pathlib import Path

DATA_FILE = Path("data/soop/categories/categories_master.csv")

def load_data():
    return pd.read_csv(DATA_FILE)

def analyze():
    df = load_data()
    # 기본 타입 캐스팅
    df["view_cnt"] = pd.to_numeric(df["view_cnt"], errors="coerce").fillna(0).astype(int)

    # 최근 시각별 피벗 (카테고리별 시청자 합)
    pivot = df.pivot_table(index="captured_at_utc",
                           columns="category_name",
                           values="view_cnt",
                           aggfunc="sum")
    pivot.to_csv("data/soop/categories/pivot_timeseries.csv", encoding="utf-8-sig")

    # 최근 스냅샷 상위 카테고리
    latest_time = df["captured_at_utc"].max()
    latest = df[df["captured_at_utc"] == latest_time].sort_values("view_cnt", ascending=False)
    latest.to_csv("data/soop/categories/top_latest.csv", index=False, encoding="utf-8-sig")

    print("Saved pivot_timeseries.csv and top_latest.csv")

if __name__ == "__main__":
    analyze()