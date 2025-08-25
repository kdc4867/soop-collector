# collect_details.py
# -*- coding: utf-8 -*-
"""
SOOP 카테고리 '개별 방송' 수집
- 시각별 스냅샷: data/soop/details/<cate_no>/YYYY/MM/DD/HH.csv
- 와이드 매트릭스: data/soop/details_matrix.csv
    행 = (category_no, broad_no)
    열 = 각 시각(UTC 정시) view_cnt
    메타컬럼: category_no, broad_no, user_id, user_nick, broad_title(마지막 값 유지)
- long 누적(details_master.csv)은 기본 비활성화(환경변수 WRITE_DETAILS_MASTER="true"로 활성화)
- 대상 카테고리: category_nos 리스트. 전수 탐색은 데이터 폭증 위험.
"""

import os
import pathlib
from datetime import datetime, timezone
from typing import List, Tuple, Dict, Any

import requests
import pandas as pd

BASE = "https://sch.sooplive.co.kr/api.php"
HEADERS = {"User-Agent": "Mozilla/5.0"}

# === 수집 대상 카테고리 (필요한 것만!) ===
category_nos = ["00810000", "00040070"]  # 버추얼, FC온라인

DATA_DIR = pathlib.Path("data/soop/details")
DATA_DIR.mkdir(parents=True, exist_ok=True)

WIDE_CSV = pathlib.Path("data/soop/details_matrix.csv")
MASTER_CSV = DATA_DIR / "details_master.csv"
WRITE_DETAILS_MASTER = os.getenv("WRITE_DETAILS_MASTER", "false").lower() == "true"

PAGE_SIZE = 60
ORDER = "view_cnt_desc"


def fetch_category_contents(cate_no: str, page: int = 1, nListCnt: int = PAGE_SIZE) -> Tuple[list, bool]:
    params = {
        "m": "categoryContentsList",
        "szType": "live",
        "nPageNo": page,
        "nListCnt": nListCnt,
        "szPlatform": "pc",
        "szOrder": ORDER,
        "szCateNo": cate_no,
    }
    r = requests.get(BASE, params=params, headers=HEADERS, timeout=15)
    r.raise_for_status()
    js = r.json()
    data = js.get("data", {})
    return data.get("list", []) or [], bool(data.get("is_more", False))


def fetch_all_for_category(cate_no: str) -> list:
    all_items = []
    page = 1
    while True:
        items, is_more = fetch_category_contents(cate_no, page)
        all_items.extend(items)
        if not is_more:
            break
        page += 1
    return all_items


def save_snapshot(df: pd.DataFrame, cate_no: str) -> pathlib.Path:
    now = datetime.now(timezone.utc)
    df = df.copy()
    df["captured_at_utc"] = now.replace(microsecond=0).isoformat()
    df["platform"] = "soop"
    df["category_no"] = cate_no

    snapshot_dir = DATA_DIR / cate_no / now.strftime("%Y/%m/%d")
    snapshot_dir.mkdir(parents=True, exist_ok=True)
    snapshot_file = snapshot_dir / f"{now.strftime('%H')}.csv"
    df.to_csv(snapshot_file, index=False, encoding="utf-8-sig")
    return snapshot_file


def maybe_append_master(df: pd.DataFrame) -> None:
    if not WRITE_DETAILS_MASTER or df.empty:
        return
    header = not MASTER_CSV.exists()
    df.to_csv(MASTER_CSV, index=False, mode="a", header=header, encoding="utf-8-sig")


def upsert_wide(df: pd.DataFrame) -> pathlib.Path:
    """
    details 와이드:
    인덱스 키 = (category_no, broad_no)
    메타컬럼 유지: user_id, user_nick, broad_title
    새 시간열(UTC 정시 ISO)을 upsert (덮어쓰기)
    """
    if df.empty:
        return WIDE_CSV

    # 필요한 컬럼만
    cols_keep = ["category_no", "broad_no", "user_id", "user_nick", "broad_title", "view_cnt", "captured_at_utc"]
    df = df[[c for c in cols_keep if c in df.columns]].copy()

    df["view_cnt"] = pd.to_numeric(df["view_cnt"], errors="coerce").fillna(0).astype(int)
    ts = pd.to_datetime(df["captured_at_utc"].iloc[0], utc=True).floor("H")
    col_name = ts.isoformat()

    # 스냅샷 기준 행(중복 방송 제거)
    snap = df.drop_duplicates(["category_no", "broad_no"]).set_index(["category_no", "broad_no"])

    if WIDE_CSV.exists():
        wide = pd.read_csv(WIDE_CSV, encoding="utf-8-sig")
        wide.set_index(["category_no", "broad_no"], inplace=True)

        # 신규 키 upsert 위한 빈 행 생성
        missing_idx = snap.index.difference(wide.index)
        if len(missing_idx) > 0:
            add = pd.DataFrame(index=missing_idx)
            wide = pd.concat([wide, add], axis=0)

        # 메타 최신화(이름/닉/제목 등)
        for meta in ["user_id", "user_nick", "broad_title"]:
            wide.loc[snap.index, meta] = snap[meta]
    else:
        WIDE_CSV.parent.mkdir(parents=True, exist_ok=True)
        wide = pd.DataFrame(index=snap.index)
        for meta in ["user_id", "user_nick", "broad_title"]:
            wide[meta] = snap[meta] if meta in snap.columns else ""

    # 새로운 시간 컬럼 생성/덮어쓰기
    wide[col_name] = 0
    wide.loc[snap.index, col_name] = snap["view_cnt"]

    # 정렬 및 저장
    wide = wide.sort_index()
    wide.reset_index().to_csv(WIDE_CSV, index=False, encoding="utf-8-sig")
    return WIDE_CSV


def main():
    all_rows = []
    for cate_no in category_nos:
        items = fetch_all_for_category(cate_no)
        if not items:
            continue
        df = pd.DataFrame(items)

        # 컬럼 정리(없으면 건너뜀)
        cols = [c for c in ["broad_no", "broad_title", "user_id", "user_nick", "view_cnt", "broad_start", "hash_tags"] if c in df.columns]
        df = df[cols].copy()

        snap_path = save_snapshot(df, cate_no)

        # upsert 준비용으로 최소 컬럼 보강
        df["category_no"] = cate_no
        df["captured_at_utc"] = datetime.now(timezone.utc).replace(microsecond=0).isoformat()

        all_rows.append(df)

        print(f"Saved snapshot for {cate_no} -> {snap_path} (rows={len(df)})")

    if not all_rows:
        print("no details to save")
        return

    big = pd.concat(all_rows, ignore_index=True)
    maybe_append_master(big)  # 기본 꺼짐
    wide_path = upsert_wide(big)
    print(f"updated details wide -> {wide_path} | WRITE_DETAILS_MASTER={WRITE_DETAILS_MASTER}")


if __name__ == "__main__":
    main()