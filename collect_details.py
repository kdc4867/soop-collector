import requests
import pandas as pd
from datetime import datetime, timezone
import pathlib

BASE = "https://sch.sooplive.co.kr/api.php"
HEADERS = {"User-Agent": "Mozilla/5.0"}
DATA_DIR = pathlib.Path("data/soop/details")
DATA_DIR.mkdir(parents=True, exist_ok=True)

# 원하는 category_no 를 리스트로 지정
category_nos = ["00810000", "00040070"]  #버추얼, FC온라인

def fetch_category_contents(cate_no, page=1, nListCnt=60):
    params = {
        "m": "categoryContentsList",
        "szType": "live",
        "nPageNo": page,
        "nListCnt": nListCnt,
        "szPlatform": "pc",
        "szOrder": "view_cnt_desc",
        "szCateNo": cate_no,
    }
    r = requests.get(BASE, params=params, headers=HEADERS, timeout=15)
    r.raise_for_status()
    js = r.json()
    return js["data"]["list"], js["data"]["is_more"]

def fetch_all_for_category(cate_no):
    all_items = []
    page = 1
    while True:
        items, is_more = fetch_category_contents(cate_no, page)
        all_items.extend(items)
        if not is_more:
            break
        page += 1
    return all_items

def save_snapshot(df: pd.DataFrame, cate_no: str):
    now = datetime.now(timezone.utc)
    df["captured_at_utc"] = now.isoformat()
    df["platform"] = "soop"
    df["category_no"] = cate_no

    # 시각별 스냅샷
    snapshot_dir = DATA_DIR / cate_no / now.strftime("%Y/%m/%d")
    snapshot_dir.mkdir(parents=True, exist_ok=True)
    snapshot_file = snapshot_dir / f"{now.strftime('%H')}.csv"
    df.to_csv(snapshot_file, index=False, encoding="utf-8-sig")

    # 누적 마스터
    master_file = DATA_DIR / "details_master.csv"
    if master_file.exists():
        old = pd.read_csv(master_file)
        df = pd.concat([old, df], ignore_index=True)
    df.to_csv(master_file, index=False, encoding="utf-8-sig")
    print(f"Saved {len(df)} rows for category {cate_no} to {snapshot_file}")

def main():
    for cate_no in category_nos:
        items = fetch_all_for_category(cate_no)
        if not items:
            continue
        df = pd.DataFrame(items)
        cols = ["broad_no", "broad_title", "user_id", "user_nick",
                "view_cnt", "broad_start", "hash_tags"]
        df = df[cols]
        save_snapshot(df, cate_no)

if __name__ == "__main__":
    main()

# # -*- coding: utf-8 -*-
# """
# SOOP 카테고리 상세(방송 단위) 수집 (CSV 저장)
# - 입력: 대상 카테고리 번호 목록(category_nos)
# - 동작: 각 cate_no에 대해 categoryContentsList를 페이징 끝까지 수집
# - 출력:
#   1) 시각별 상세 스냅샷 CSV: data/soop/details/YYYY/MM/DD/HH.csv
#   2) 마스터 누적 CSV:        data/soop/details/details_master.csv
# """

# import os
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
# PAGE_SIZE = 60
# ORDER = "view_cnt_desc"
# SLEEP_BETWEEN_PAGES = 0.3
# SLEEP_BETWEEN_CATS = 0.8

# OUT_ROOT = pathlib.Path("data/soop/details")
# MASTER_CSV = OUT_ROOT / "details_master.csv"

# # 수집 대상 카테고리 (예시: 로스트아크, 버추얼, 롤, 롤토체스 등)
# category_nos = [
#     # "00040067",  # 로스트아크
#     # "00810000",  # 버추얼
#     # "00040019",  # 리그 오브 레전드
#     # "00040075",  # 전략적 팀 전투(TFT)
# ]

# # ======================
# # 요청/수집 함수
# # ======================
# def fetch_contents_page(cate_no: str, page_no: int, n_per: int = PAGE_SIZE, order: str = ORDER) -> Tuple[List[Dict[str, Any]], bool]:
#     params = {
#         "m": "categoryContentsList",
#         "szType": "live",
#         "nPageNo": page_no,
#         "nListCnt": n_per,
#         "szPlatform": "pc",
#         "szOrder": order,
#         "szCateNo": cate_no,
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

# def fetch_category_details(cate_no: str) -> pd.DataFrame:
#     """특정 카테고리의 방송 리스트 전체 수집"""
#     rows: List[Dict[str, Any]] = []
#     page = 1
#     while True:
#         items, is_more = fetch_contents_page(cate_no, page)
#         rows += items
#         if not is_more:
#             break
#         page += 1
#         time.sleep(SLEEP_BETWEEN_PAGES)
#     if not rows:
#         return pd.DataFrame(columns=[
#             "broad_no","broad_title","user_id","user_nick","view_cnt",
#             "broad_start","hash_tags","thumbnail","user_profile_img",
#             "category_no","captured_at_utc","platform"
#         ])
#     df = pd.DataFrame(rows)

#     # 필요한 컬럼만 유지
#     keep = [c for c in [
#         "broad_no","broad_title","user_id","user_nick","view_cnt",
#         "broad_start","hash_tags","thumbnail","user_profile_img"
#     ] if c in df.columns]
#     df = df[keep].copy()

#     # 메타 추가
#     ts = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
#     df["captured_at_utc"] = ts
#     df["platform"] = "soop"
#     df["category_no"] = cate_no
#     return df

# def fetch_multi_categories(cate_list: List[str]) -> pd.DataFrame:
#     frames = []
#     for cate in cate_list:
#         df = fetch_category_details(cate)
#         if not df.empty:
#             frames.append(df)
#         time.sleep(SLEEP_BETWEEN_CATS)
#     return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()

# def save_snapshot_csv(df: pd.DataFrame) -> pathlib.Path:
#     ts = pd.to_datetime(df["captured_at_utc"].iloc[0])
#     outdir = OUT_ROOT / f"{ts.year:04d}" / f"{ts.month:02d}" / f"{ts.day:02d}"
#     outdir.mkdir(parents=True, exist_ok=True)
#     outpath = outdir / f"{ts.hour:02d}.csv"
#     df.to_csv(outpath, index=False, encoding="utf-8")
#     return outpath

# def append_master_csv(df: pd.DataFrame) -> pathlib.Path:
#     OUT_ROOT.mkdir(parents=True, exist_ok=True)
#     header = not MASTER_CSV.exists()
#     df.to_csv(MASTER_CSV, index=False, mode="a", header=header, encoding="utf-8")
#     return MASTER_CSV

# def main():
#     if not category_nos:
#         print("⚠️ category_nos 리스트에 수집할 cate_no를 하나 이상 넣어주세요.")
#         return

#     df_all = fetch_multi_categories(category_nos)
#     if df_all.empty:
#         print("수집 결과가 비어 있습니다.")
#         return

#     # 콘솔에 상위 20개 출력
#     cols_show = [c for c in ["category_no","broad_no","user_id","user_nick","view_cnt","broad_title"] if c in df_all.columns]
#     print("\n=== Details snapshot (top 20 by view_cnt) ===")
#     print(df_all.sort_values("view_cnt", ascending=False)[cols_show].head(20).to_string(index=False))

#     # 저장
#     snap = save_snapshot_csv(df_all)
#     master = append_master_csv(df_all)
#     snap_kb = snap.stat().st_size/1024 if snap.exists() else 0
#     mast_kb = master.stat().st_size/1024 if master.exists() else 0
#     print(f"\nsaved snapshot -> {snap} ({snap_kb:.1f} KB)")
#     print(f"appended master -> {master} ({mast_kb:.1f} KB total)")

# if __name__ == "__main__":
#     main()
