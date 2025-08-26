# collect_details.py
# -*- coding: utf-8 -*-
"""
SOOP 디테일(방송 목록) 수집
- 카테고리별로 '스냅샷 / 마스터 / 와이드 매트릭스 / BJ 마스터'를 각각 분리 저장
- 와이드 매트릭스 열 라벨: "user_id|user_nick" (사람이 바로 식별 가능)
- 동일 시간대에 같은 BJ가 여러 레코드면 '마지막 값' 기준으로 반영(최근 스냅샷 우선)

폴더 구조:
data/soop/details/
  ├─ 00040070_버추얼/
  │    ├─ snapshots/YYYY/MM/DD/HH.csv
  │    ├─ details_master.csv
  │    ├─ details_matrix.csv      # 행: captured_hour(UTC), 열: user_id|user_nick, 값: view_cnt(Int64)
  │    └─ bj_master.csv           # user_id, nickname, first_seen, last_seen
  └─ 00810000_FC온라인/
       ├─ snapshots/...
       ├─ details_master.csv
       ├─ details_matrix.csv
       └─ bj_master.csv
"""

from __future__ import annotations
import requests
import pandas as pd
from datetime import datetime, timezone
from pathlib import Path
import time
import re
from typing import Dict, Tuple, Any, List

# ───────────────────────────────── 기본 설정 ─────────────────────────────────
BASE = "https://sch.sooplive.co.kr/api.php"
HEADERS = {"User-Agent": "Mozilla/5.0", "Accept": "application/json, text/plain, */*"}
DATA_ROOT = Path("data/soop/details")
DATA_ROOT.mkdir(parents=True, exist_ok=True)

# 수집 대상 카테고리 (cate_no → 한글명)
CATEGORY_MAP: Dict[str, str] = {
    "00040070": "버추얼",
    "00810000": "FC온라인",
}

PAGE_SIZE = 60
ORDER = "view_cnt_desc"
SLEEP_BETWEEN_PAGES = 0.25

# ────────────────────────────── 유틸 ──────────────────────────────
_SLUG_RE = re.compile(r"[^0-9A-Za-z가-힣_()-]+")
def slug(s: str) -> str:
    return _SLUG_RE.sub("", s).strip() or "cat"

def ensure_int64(s: pd.Series) -> pd.Series:
    return pd.to_numeric(s, errors="coerce").fillna(0).astype("Int64")

def to_hour_utc_iso(ts_scalar_or_series) -> pd.Series:
    return pd.to_datetime(ts_scalar_or_series, utc=True).dt.floor("h").dt.strftime("%Y-%m-%dT%H:00:00Z")

def category_dir(cate_no: str, cate_name: str) -> Path:
    return DATA_ROOT / f"{cate_no}_{slug(cate_name)}"

# ─────────────────────────── 네트워크 수집 ───────────────────────────
def fetch_category_contents(cate_no: str, page=1, nListCnt=PAGE_SIZE) -> Tuple[List[Dict[str, Any]], bool]:
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
    data = js.get("data", {}) or {}
    return data.get("list", []) or [], bool(data.get("is_more", False))

def fetch_all_for_category(cate_no: str) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    page = 1
    while True:
        items, is_more = fetch_category_contents(cate_no, page)
        rows.extend(items)
        if not is_more:
            break
        page += 1
        time.sleep(SLEEP_BETWEEN_PAGES)
    return rows

# ─────────────────────── 저장(스냅샷/마스터) ───────────────────────
def save_snapshot_and_append_master(df: pd.DataFrame, cate_no: str, cate_name: str, ts_iso: str) -> None:
    """
    카테고리별 스냅샷을 저장하고, 같은 폴더의 details_master.csv에 append
    """
    cdir = category_dir(cate_no, cate_name)
    snap_dir = cdir / "snapshots"
    snap_dir.mkdir(parents=True, exist_ok=True)

    # 공통 메타 부여
    df = df.copy()
    df["captured_at_utc"] = ts_iso
    df["platform"] = "soop"
    df["category_no"] = cate_no

    # 시각별 스냅샷 파일
    ts = datetime.fromisoformat(ts_iso.replace("Z", "+00:00"))
    outdir = snap_dir / ts.strftime("%Y/%m/%d")
    outdir.mkdir(parents=True, exist_ok=True)
    snap_path = outdir / f"{ts.strftime('%H')}.csv"
    df.to_csv(snap_path, index=False, encoding="utf-8-sig")

    # 카테고리별 마스터 파일
    master_csv = cdir / "details_master.csv"
    header = not master_csv.exists()
    df.to_csv(master_csv, mode="a", header=header, index=False, encoding="utf-8-sig")

    # BJ 마스터 갱신
    update_bj_master(df, cate_no, cate_name)

def update_bj_master(df: pd.DataFrame, cate_no: str, cate_name: str) -> None:
    """
    user_id ↔ nickname 최신 맵과 first_seen / last_seen를 유지
    """
    cdir = category_dir(cate_no, cate_name)
    bj_csv = cdir / "bj_master.csv"
    need = ["captured_at_utc", "user_id", "user_nick"]
    if any(col not in df.columns for col in need):
        return

    hour = to_hour_utc_iso(df["captured_at_utc"]).iloc[0]
    snap = (
        df[["user_id", "user_nick"]]
        .astype(str)
        .drop_duplicates(subset=["user_id"])
        .assign(first_seen=hour, last_seen=hour)
    )

    if bj_csv.exists():
        m = pd.read_csv(bj_csv, dtype=str)
    else:
        m = pd.DataFrame(columns=["user_id", "user_nick", "first_seen", "last_seen"])

    m = pd.concat([m, snap], ignore_index=True)
    # 동일 user_id는 마지막 등장(최신 닉네임/last_seen)만 유지
    m.sort_values(["user_id", "last_seen"], inplace=True)
    m = m.drop_duplicates(subset=["user_id"], keep="last")
    m.to_csv(bj_csv, index=False, encoding="utf-8-sig")

# ────────────────────────── 와이드 매트릭스 ──────────────────────────
def update_matrix_for_category(cate_no: str, cate_name: str) -> None:
    """
    카테고리별 details_master.csv → details_matrix.csv
    행: captured_hour(UTC, ISO)
    열: "user_id|user_nick"
    값: view_cnt (Int64)
    - 동일 시간/동일 user_id는 '마지막 값' 유지
    - 기존 파일이 있으면 같은 열/시간은 덮어쓰기(최신 스냅샷 우선)
    """
    cdir = category_dir(cate_no, cate_name)
    master_csv = cdir / "details_master.csv"
    matrix_csv = cdir / "details_matrix.csv"

    if not master_csv.exists():
        print(f"[{cate_no}] no master yet; skip matrix")
        return

    df = pd.read_csv(master_csv, encoding="utf-8-sig")
    need = ["captured_at_utc", "user_id", "user_nick", "view_cnt"]
    if any(col not in df.columns for col in need):
        print(f"[{cate_no}] master columns missing; skip matrix")
        return

    df = df[need].copy()
    df["user_id"] = df["user_id"].astype(str)
    df["user_nick"] = df["user_nick"].astype(str)
    df["view_cnt"] = ensure_int64(df["view_cnt"])
    df["captured_hour"] = to_hour_utc_iso(df["captured_at_utc"])
    df["col_label"] = df["user_id"] + "|" + df["user_nick"]

    # 시간 정렬 후 중복 제거(같은 시간/같은 BJ는 마지막 값)
    df.sort_values(["captured_hour"], inplace=True)
    df = df.drop_duplicates(subset=["captured_hour", "user_id"], keep="last")

    # 현재 스냅샷 → 와이드
    cur = (
        df.pivot_table(index="captured_hour", columns="col_label", values="view_cnt", aggfunc="last")
        .astype("Int64")
    )

    # 기존 매트릭스 병합
    if matrix_csv.exists():
        old = pd.read_csv(matrix_csv, dtype=str)
        if "captured_hour" not in old.columns:
            first = old.columns[0]
            old = old.rename(columns={first: "captured_hour"})
        for c in old.columns:
            if c == "captured_hour":
                continue
            old[c] = ensure_int64(old[c])
        old = old.set_index("captured_hour")

        # 행·열 합집합, 동일 셀은 cur(최신)로 덮어쓰기
        wide = old.reindex(index=old.index.union(cur.index), columns=old.columns.union(cur.columns))
        for col in cur.columns:
            wide[col] = cur[col]
    else:
        wide = cur

    # 시간 오름차순
    wide = wide.sort_index()
    out_df = wide.reset_index()
    out_df.to_csv(matrix_csv, index=False, encoding="utf-8-sig")
    print(f"updated matrix -> {matrix_csv}")

# ────────────────────────────── 메인 ──────────────────────────────
def main():
    now_iso = datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")

    all_preview = []  # 콘솔 프린트용

    for cate_no, cate_name in CATEGORY_MAP.items():
        items = fetch_all_for_category(cate_no)
        if not items:
            print(f"[{cate_no}] empty")
            continue

        df = pd.DataFrame(items)

        # 사용 컬럼만 (있으면 사용)
        cols = ["broad_no", "broad_title", "user_id", "user_nick", "view_cnt", "broad_start", "hash_tags"]
        cols = [c for c in cols if c in df.columns]
        df = df[cols].copy()

        # 스냅샷 저장 + 카테고리 마스터 append + BJ 마스터 갱신
        save_snapshot_and_append_master(df, cate_no, cate_name, now_iso)

        # 프리뷰 수집
        df_prev = df.copy()
        df_prev["category_no"] = cate_no
        df_prev["captured_at_utc"] = now_iso
        all_preview.append(df_prev)

        # 카테고리별 와이드 매트릭스 갱신
        update_matrix_for_category(cate_no, cate_name)

    # 콘솔 프리뷰
    if all_preview:
        ap = pd.concat(all_preview, ignore_index=True)
        if "view_cnt" in ap.columns:
            show_cols = [c for c in ["category_no","broad_no","user_id","user_nick","view_cnt","broad_title"] if c in ap.columns]
            print("\n=== Details snapshot (top 20 by view_cnt) ===")
            print(ap.sort_values("view_cnt", ascending=False)[show_cols].head(20).to_string(index=False))

if __name__ == "__main__":
    main()