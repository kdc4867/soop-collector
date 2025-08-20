# collect_categories.py
# -*- coding: utf-8 -*-
"""
SOOP 카테고리 스냅샷 수집 (CSV 저장, 라벨링/가공 없음)
- categoryList API를 페이징 끝까지 수집
- 원본 필드만 저장: category_no, category_name, view_cnt, fixed_tags, cate_img, captured_at_utc, platform
- 저장:
  1) 시각별 스냅샷: data/soop/categories/YYYY/MM/DD/HH.csv
  2) 마스터 누적:   data/soop/categories/categories_master.csv
  3) 시계열 누적:   data/soop/categories_timeseries.csv  ← 이번에 추가
"""

import time
import pathlib
from datetime import datetime, timezone
from typing import List, Tuple, Dict, Any

import requests
import pandas as pd

# ======================
# 설정
# ======================
BASE = "https://sch.sooplive.co.kr/api.php"
HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Accept": "application/json, text/plain, */*",
    "Referer": "https://www.sooplive.co.kr/",
}
PAGE_SIZE = 120
ORDER = "view_cnt"
SLEEP_BETWEEN_PAGES = 0.35

OUT_ROOT = pathlib.Path("data/soop/categories")
OUT_ROOT.mkdir(parents=True, exist_ok=True)
MASTER_CSV = OUT_ROOT / "categories_master.csv"
TIMESERIES_CSV = pathlib.Path("data/soop/categories_timeseries.csv")

# ======================
# 요청/수집
# ======================
def fetch_category_page(page_no: int, n_per: int = PAGE_SIZE, order: str = ORDER) -> Tuple[List[Dict[str, Any]], bool]:
    params = {
        "m": "categoryList",
        "szKeyword": "",
        "szOrder": order,
        "nPageNo": page_no,
        "nListCnt": n_per,
        "nOffset": 0,
        "szPlatform": "pc",
    }
    backoff = 1.0
    for attempt in range(5):
        try:
            r = requests.get(BASE, params=params, headers=HEADERS, timeout=15)
            r.raise_for_status()
            j = r.json()
            data = j.get("data", {})
            items = data.get("list", []) or []
            is_more = bool(data.get("is_more", False))
            return items, is_more
        except Exception:
            if attempt == 4:
                raise
            time.sleep(backoff)
            backoff = min(backoff * 2, 10)
    return [], False

def fetch_all_categories() -> pd.DataFrame:
    rows: List[Dict[str, Any]] = []
    page = 1
    while True:
        items, is_more = fetch_category_page(page)
        rows += items
        if not is_more:
            break
        page += 1
        time.sleep(SLEEP_BETWEEN_PAGES)
    if not rows:
        return pd.DataFrame(columns=["category_no","category_name","view_cnt","fixed_tags","cate_img"])
    df = pd.DataFrame(rows)
    # 필요한 컬럼만 (원본 유지)
    keep = [c for c in ["category_no","category_name","view_cnt","fixed_tags","cate_img"] if c in df.columns]
    df = df[keep].copy()
    # 메타
    ts = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
    df["captured_at_utc"] = ts
    df["platform"] = "soop"
    return df

def save_snapshot_csv(df: pd.DataFrame) -> pathlib.Path:
    ts = pd.to_datetime(df["captured_at_utc"].iloc[0])
    outdir = OUT_ROOT / f"{ts.year:04d}" / f"{ts.month:02d}" / f"{ts.day:02d}"
    outdir.mkdir(parents=True, exist_ok=True)
    outpath = outdir / f"{ts.hour:02d}.csv"
    df.to_csv(outpath, index=False, encoding="utf-8-sig")
    return outpath

def append_master_csv(df: pd.DataFrame) -> pathlib.Path:
    header = not MASTER_CSV.exists()
    df.to_csv(MASTER_CSV, index=False, mode="a", header=header, encoding="utf-8-sig")
    return MASTER_CSV

def upsert_timeseries_csv(df: pd.DataFrame) -> pathlib.Path:
    """시계열 누적 파일 갱신 (append 후 같은 시각/카테고리 중복 제거)"""
    # 필요한 컬럼만 축소
    ts_part = df[["captured_at_utc","category_no","category_name","view_cnt"]].copy()
    # 정시 버킷 보정(안전)
    ts_part["captured_at_utc"] = pd.to_datetime(ts_part["captured_at_utc"], utc=True).dt.floor("H")

    if TIMESERIES_CSV.exists():
        old = pd.read_csv(TIMESERIES_CSV, encoding="utf-8-sig")
        # 타입 보정
        if "captured_at_utc" in old.columns:
            old["captured_at_utc"] = pd.to_datetime(old["captured_at_utc"], utc=True).dt.floor("H")
        ts_all = pd.concat([old, ts_part], ignore_index=True)
    else:
        TIMESERIES_CSV.parent.mkdir(parents=True, exist_ok=True)
        ts_all = ts_part

    # 같은 시각(captured_at_utc) + 같은 카테고리(category_no) 중복 제거 (마지막 값 유지)
    ts_all = ts_all.drop_duplicates(["captured_at_utc","category_no"], keep="last")
    ts_all.sort_values(["captured_at_utc","category_no"], inplace=True)
    ts_all.to_csv(TIMESERIES_CSV, index=False, encoding="utf-8-sig")
    return TIMESERIES_CSV

def main():
    df_all = fetch_all_categories()
    if df_all.empty:
        print("빈 응답. 잠시 후 재시도 바람.")
        return

    # 콘솔 확인(상위 25)
    if "view_cnt" in df_all.columns:
        df_sorted = df_all.sort_values("view_cnt", ascending=False).copy()
        cols_show = [c for c in ["category_no","category_name","view_cnt","fixed_tags"] if c in df_sorted.columns]
        print("\n=== Snapshot (top 25 by view_cnt) ===")
        print(df_sorted[cols_show].head(25).to_string(index=False))
        print(f"\nrows_total={len(df_all)}")

    snap = save_snapshot_csv(df_all)
    master = append_master_csv(df_all)
    tsfile = upsert_timeseries_csv(df_all)
    print(f"\nsaved snapshot -> {snap}")
    print(f"appended master -> {master}")
    print(f"updated timeseries -> {tsfile}")

if __name__ == "__main__":
    main()