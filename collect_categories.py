import requests
import pandas as pd
from datetime import datetime, timezone
import os
import pathlib

BASE = "https://sch.sooplive.co.kr/api.php"
HEADERS = {"User-Agent": "Mozilla/5.0"}
DATA_DIR = pathlib.Path("data/soop/categories")
DATA_DIR.mkdir(parents=True, exist_ok=True)

def fetch_category_page(page: int = 1, nListCnt: int = 120):
    params = {
        "m": "categoryList",
        "szKeyword": "",
        "szOrder": "view_cnt",
        "nPageNo": page,
        "nListCnt": nListCnt,
        "nOffset": (page - 1) * nListCnt,
        "szPlatform": "pc",
    }
    r = requests.get(BASE, params=params, headers=HEADERS, timeout=15)
    r.raise_for_status()
    js = r.json()
    return js["data"]["list"], js["data"]["is_more"]

def fetch_all_categories():
    all_items = []
    page = 1
    while True:
        items, is_more = fetch_category_page(page)
        all_items.extend(items)
        if not is_more:
            break
        page += 1
    return all_items

def save_snapshot(df: pd.DataFrame):
    now = datetime.now(timezone.utc)
    df["captured_at_utc"] = now.isoformat()
    df["platform"] = "soop"

    # 시각별 스냅샷
    snapshot_dir = DATA_DIR / now.strftime("%Y/%m/%d")
    snapshot_dir.mkdir(parents=True, exist_ok=True)
    snapshot_file = snapshot_dir / f"{now.strftime('%H')}.csv"
    df.to_csv(snapshot_file, index=False, encoding="utf-8-sig")

    # 누적 마스터
    master_file = DATA_DIR / "categories_master.csv"
    if master_file.exists():
        old = pd.read_csv(master_file)
        df = pd.concat([old, df], ignore_index=True)
    df.to_csv(master_file, index=False, encoding="utf-8-sig")
    print(f"Saved snapshot to {snapshot_file} and updated master")

def main():
    items = fetch_all_categories()
    df = pd.DataFrame(items)
    cols = ["category_no", "category_name", "view_cnt",
            "fixed_tags", "cate_img"]
    df = df[cols]
    save_snapshot(df)

if __name__ == "__main__":
    main()

# # -*- coding: utf-8 -*-
# """
# SOOP 카테고리 스냅샷 수집 (CSV 저장 버전)
# - categoryList API를 페이징 끝까지 긁어서 DataFrame으로 정리
# - view_cnt >= MIN_VIEWERS 필터를 콘솔 표시용으로 적용 (저장 파일은 원본 전체 저장)
# - CSV 저장:
#   1) 시각별 스냅샷 파일: data/soop/categories/YYYY/MM/DD/HH.csv
#   2) 마스터 누적 파일:    data/soop/categories/categories_master.csv (헤더 1회)
# """

# import os
# import csv
# import time
# import json
# import pathlib
# from datetime import datetime, timezone
# from typing import List, Tuple, Dict, Any

# import requests
# import pandas as pd

# # ======================
# # 설정
# # ======================
# BASE = "https://sch.sooplive.co.kr/api.php"
# HEADERS = {
#     "User-Agent": "Mozilla/5.0",
#     "Accept": "application/json, text/plain, */*",
#     "Referer": "https://www.sooplive.co.kr/",
# }
# PAGE_SIZE = 120
# ORDER = "view_cnt"
# SLEEP_BETWEEN_PAGES = 0.35
# MIN_VIEWERS = 1

# OUT_ROOT = pathlib.Path("data/soop/categories")
# MASTER_CSV = OUT_ROOT / "categories_master.csv"

# # ======================
# # 요청/수집 함수
# # ======================
# def fetch_category_page(page_no: int, n_per: int = PAGE_SIZE, order: str = ORDER) -> Tuple[List[Dict[str, Any]], bool]:
#     """카테고리 리스트 한 페이지 요청"""
#     params = {
#         "m": "categoryList",
#         "szKeyword": "",
#         "szOrder": order,
#         "nPageNo": page_no,
#         "nListCnt": n_per,
#         "nOffset": 0,
#         "szPlatform": "pc",
#     }
#     backoff = 1.0
#     for attempt in range(5):
#         try:
#             r = requests.get(BASE, params=params, headers=HEADERS, timeout=15)
#             r.raise_for_status()
#             j = r.json()
#             data = j.get("data", {})
#             items = data.get("list", []) or []
#             is_more = bool(data.get("is_more", False))
#             return items, is_more
#         except Exception:
#             if attempt == 4:
#                 raise
#             time.sleep(backoff)
#             backoff = min(backoff * 2, 10)
#     return [], False

# def fetch_all_categories() -> pd.DataFrame:
#     """모든 페이지 수집"""
#     all_rows: List[Dict[str, Any]] = []
#     page = 1
#     while True:
#         items, is_more = fetch_category_page(page)
#         all_rows.extend(items)
#         if not is_more:
#             break
#         page += 1
#         time.sleep(SLEEP_BETWEEN_PAGES)
#     if not all_rows:
#         return pd.DataFrame(columns=["category_no","category_name","view_cnt","fixed_tags","cate_img"])
#     df = pd.DataFrame(all_rows)

#     # 필요한 컬럼만 유지
#     keep = [c for c in ["category_no","category_name","view_cnt","fixed_tags","cate_img"] if c in df.columns]
#     df = df[keep].copy()

#     # fixed_tags -> JSON 문자열(항상 일관되게)
#     def _to_json_safe(v):
#         try:
#             return json.dumps(v, ensure_ascii=False)
#         except Exception:
#             return json.dumps([], ensure_ascii=False)
        
#     df["fixed_tags"] = df["fixed_tags"].apply(_to_json_safe)


#     # 메타 추가
#     ts = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
#     df["captured_at_utc"] = ts
#     df["platform"] = "soop"
#     return df

# def save_snapshot_csv(df: pd.DataFrame) -> pathlib.Path:
#     """시각별 스냅샷 CSV 저장"""
#     ts = pd.to_datetime(df["captured_at_utc"].iloc[0])
#     outdir = OUT_ROOT / f"{ts.year:04d}" / f"{ts.month:02d}" / f"{ts.day:02d}"
#     outdir.mkdir(parents=True, exist_ok=True)
#     outpath = outdir / f"{ts.hour:02d}.csv"
#     df.to_csv(outpath, index=False, encoding="utf-8")
#     return outpath

# def append_master_csv(df: pd.DataFrame) -> pathlib.Path:
#     """마스터 누적 CSV 저장(헤더 1회)"""
#     OUT_ROOT.mkdir(parents=True, exist_ok=True)
#     header = not MASTER_CSV.exists()
#     df.to_csv(MASTER_CSV, index=False, mode="a", header=header, encoding="utf-8")
#     return MASTER_CSV

# def main():
#     df_all = fetch_all_categories()

#     # 콘솔 확인: 활성만 정렬해서 상위 25개 출력
#     if not df_all.empty and "view_cnt" in df_all.columns:
#         df_alive = df_all[df_all["view_cnt"] >= MIN_VIEWERS].copy()
#         df_alive.sort_values("view_cnt", ascending=False, inplace=True)
#         cols_show = [c for c in ["category_no","category_name","view_cnt","fixed_tags"] if c in df_alive.columns]
#         print("\n=== Snapshot (alive categories) ===")
#         print(df_alive[cols_show].head(25).to_string(index=False))
#         print(f"\nrows_alive={len(df_alive)}  rows_total={len(df_all)}")

#     # 저장
#     if not df_all.empty:
#         snap = save_snapshot_csv(df_all)
#         master = append_master_csv(df_all)
#         snap_kb = snap.stat().st_size/1024 if snap.exists() else 0
#         mast_kb = master.stat().st_size/1024 if master.exists() else 0
#         print(f"\nsaved snapshot -> {snap} ({snap_kb:.1f} KB)")
#         print(f"appended master -> {master} ({mast_kb:.1f} KB total)")

# if __name__ == "__main__":
#     main()
