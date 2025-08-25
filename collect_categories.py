# collect_categories.py
# -*- coding: utf-8 -*-
"""
SOOP 카테고리 스냅샷 수집 + 와이드 매트릭스(행=카테고리, 열=시간) 저장
- 스냅샷 CSV: data/soop/categories/YYYY/MM/DD/HH.csv (원본 시각별 전체 448행 보존)
- 와이드 매트릭스: data/soop/categories_matrix.csv (행=카테고리, 열=각 시각의 view_cnt)
- long 포맷 파일(categories_master.csv, categories_timeseries.csv)은 기본 비활성화
  (환경변수 WRITE_LONG_MASTER/WRITE_LONG_TS 를 "true"로 주면 활성화)
"""

import os
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
WIDE_CSV = pathlib.Path("data/soop/categories_matrix.csv")  # ★ 핵심 산출물

# long 파일 쓰기 옵션(기본 꺼짐)
WRITE_LONG_MASTER = os.getenv("WRITE_LONG_MASTER", "false").lower() == "true"
WRITE_LONG_TS     = os.getenv("WRITE_LONG_TS", "false").lower() == "true"


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
    keep = [c for c in ["category_no","category_name","view_cnt","fixed_tags","cate_img"] if c in df.columns]
    df = df[keep].copy()
    ts = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
    df["captured_at_utc"] = ts
    df["platform"] = "soop"
    return df


# ======================
# 저장/갱신
# ======================
def save_snapshot_csv(df: pd.DataFrame) -> pathlib.Path:
    ts = pd.to_datetime(df["captured_at_utc"].iloc[0])
    outdir = OUT_ROOT / f"{ts.year:04d}" / f"{ts.month:02d}" / f"{ts.day:02d}"
    outdir.mkdir(parents=True, exist_ok=True)
    outpath = outdir / f"{ts.hour:02d}.csv"
    df.to_csv(outpath, index=False, encoding="utf-8-sig")
    return outpath


def append_master_csv(df: pd.DataFrame) -> pathlib.Path:
    if WRITE_LONG_MASTER:
        header = not MASTER_CSV.exists()
        df.to_csv(MASTER_CSV, index=False, mode="a", header=header, encoding="utf-8-sig")
    return MASTER_CSV  # noop 또는 쓰기


def upsert_timeseries_csv(df: pd.DataFrame) -> pathlib.Path:
    """(옵션) long 포맷 timeseries (원하면 켬)"""
    if not WRITE_LONG_TS:
        return TIMESERIES_CSV  # noop
    ts_part = df[["captured_at_utc","category_no","category_name","view_cnt"]].copy()
    ts_part["captured_at_utc"] = pd.to_datetime(ts_part["captured_at_utc"], utc=True).dt.floor("H")
    if TIMESERIES_CSV.exists():
        old = pd.read_csv(TIMESERIES_CSV, encoding="utf-8-sig")
        if "captured_at_utc" in old.columns:
            old["captured_at_utc"] = pd.to_datetime(old["captured_at_utc"], utc=True).dt.floor("H")
        ts_all = pd.concat([old, ts_part], ignore_index=True)
    else:
        TIMESERIES_CSV.parent.mkdir(parents=True, exist_ok=True)
        ts_all = ts_part
    ts_all = ts_all.drop_duplicates(["captured_at_utc","category_no"], keep="last")
    ts_all.sort_values(["captured_at_utc","category_no"], inplace=True)
    ts_all.to_csv(TIMESERIES_CSV, index=False, encoding="utf-8-sig")
    return TIMESERIES_CSV


def upsert_wide_csv(df: pd.DataFrame) -> pathlib.Path:
    """
    카테고리별(view_cnt) 와이드 매트릭스 누적 갱신:
    - 행: category_no (문자열로 고정)
    - 열: captured_hour (UTC, ISO 'YYYY-MM-DDTHH:00:00Z')
    - 값: view_cnt (Int64; 결측은 NA)
    - 같은 시간/카테고리는 새 스냅샷으로 덮어씀(최근값 우선)
    """
    df = df.copy()

    # ★ 타입 고정 (핵심)
    df["category_no"] = df["category_no"].astype(str).str.zfill(8)
    df["view_cnt"] = pd.to_numeric(df["view_cnt"], errors="coerce").fillna(0).astype("Int64")

    # ★ 시각을 "정시 UTC"로 맞추고 문자열 컬럼 생성
    df["captured_at_utc"] = pd.to_datetime(df["captured_at_utc"], utc=True).dt.floor("h")
    df["captured_hour"] = df["captured_at_utc"].dt.strftime("%Y-%m-%dT%H:00:00Z")

    # 현재 스냅샷을 와이드로 피벗
    cur = (
        df.pivot_table(
            index="category_no",
            columns="captured_hour",
            values="view_cnt",
            aggfunc="max",
        )
        .astype("Int64")
    )

    if WIDE_CSV.exists():
        # 기존 파일 읽기: category_no를 인덱스로, 반드시 문자열로
        old = pd.read_csv(WIDE_CSV, dtype={"category_no": str}, index_col="category_no")
        old.index = old.index.astype(str).str.zfill(8)
        old.columns = [str(c) for c in old.columns]  # 열 라벨 문자열화

        # 인덱스(카테고리) 풀 합집합으로 맞추기
        wide = old.reindex(index=old.index.union(cur.index))

        # 열(시간) 추가/덮어쓰기: 같은 열이 있으면 새값으로 교체됨
        for col in cur.columns:
            wide[col] = cur[col]

    else:
        wide = cur

    # ★ 정렬: 인덱스는 문자열 기준, 열은 시간 순서
    wide.index = wide.index.astype(str)  # 혼합 타입 방지
    # 열을 시간으로 파싱해 정렬 (파싱 실패시 맨 뒤)
    cols = list(wide.columns)
    cols_sorted = sorted(
        cols,
        key=lambda s: pd.to_datetime(s, utc=True, errors="coerce").to_datetime64()
                        if pd.to_datetime(s, utc=True, errors="coerce") is not pd.NaT else pd.NaT
    )
    # NaT(파싱 실패)가 있으면 뒤로 보내기
    cols_sorted = [c for c in cols_sorted if str(c) != "NaT"] + [c for c in cols if pd.isna(pd.to_datetime(c, utc=True, errors="coerce"))]

    # 판다스 1.x/2.x 호환: 인덱스 정렬은 key로 문자열 캐스팅
    try:
        wide = wide.sort_index(key=lambda idx: idx.astype(str))
    except TypeError:
        wide.index = wide.index.astype(str)
        wide = wide.sort_index()

    wide = wide.reindex(columns=cols_sorted)

    # 저장 (엑셀/윈도에서 한글 깨짐 방지하려면 utf-8-sig)
    WIDE_CSV.parent.mkdir(parents=True, exist_ok=True)
    wide.to_csv(WIDE_CSV, encoding="utf-8-sig")

    return WIDE_CSV


def main():
    df_all = fetch_all_categories()
    if df_all.empty:
        print("빈 응답. 잠시 후 재시도 바람.")
        return

    if "view_cnt" in df_all.columns:
        df_sorted = df_all.sort_values("view_cnt", ascending=False).copy()
        cols_show = [c for c in ["category_no","category_name","view_cnt","fixed_tags"] if c in df_sorted.columns]
        print("\n=== Snapshot (top 25 by view_cnt) ===")
        print(df_sorted[cols_show].head(25).to_string(index=False))
        print(f"\nrows_total={len(df_all)}")

    snap = save_snapshot_csv(df_all)
    master = append_master_csv(df_all)      # 기본은 noop
    tsfile = upsert_timeseries_csv(df_all)  # 기본은 noop
    wide   = upsert_wide_csv(df_all)        # ★ 와이드 파일 갱신

    print(f"\nsaved snapshot -> {snap}")
    print(f"appended master -> {master} (WRITE_LONG_MASTER={WRITE_LONG_MASTER})")
    print(f"updated timeseries(long) -> {tsfile} (WRITE_LONG_TS={WRITE_LONG_TS})")
    print(f"updated wide -> {wide}")


if __name__ == "__main__":
    main()