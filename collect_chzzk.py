# # collect_chzzk.py
# # -*- coding: utf-8 -*-
# """
# CHZZK(치지직) 수집기
# - 스냅샷: data/chzzk/lives/YYYY/MM/DD/HH.csv
# - 카테고리 와이드(전체): data/chzzk/categories_matrix.csv
# - 카테고리 와이드(게임만): data/chzzk/game_categories_matrix.csv
# - 디테일 와이드(열=스트리머 이름): data/chzzk/details_matrix.csv
# """

# import os, time, json, csv
# from pathlib import Path
# from datetime import datetime, timezone
# from typing import Dict, Any, List
# import numpy as np
# import pandas as pd
# import requests

# OPENAPI = "https://openapi.chzzk.naver.com"
# HEADERS = {
#     "Client-Id": os.environ.get("CHZZK_CLIENT_ID", "dcb94fcb-4bf9-4593-9207-b82a413b73e8"),
#     "Client-Secret": os.environ.get("CHZZK_CLIENT_SECRET", "RCuiu38XxAu69BqjRmlr5fhzxExUS4P62TkpTFCvHYU"),
#     "Content-Type": "application/json",
#     "User-Agent": "Mozilla/5.0",
# }

# # 출력 경로
# OUT_ROOT       = Path("data/chzzk")
# SNAP_ROOT      = OUT_ROOT / "lives"
# CAT_WIDE       = OUT_ROOT / "categories_matrix.csv"
# GAME_CAT_WIDE  = OUT_ROOT / "game_categories_matrix.csv"
# DET_WIDE       = OUT_ROOT / "details_matrix.csv"

# PAGE_SIZE = 20
# SLEEP_BETWEEN = 0.25


# def _utc_hour_iso(dt=None) -> str:
#     if dt is None:
#         dt = datetime.now(timezone.utc).replace(microsecond=0)
#     dt = pd.to_datetime(dt, utc=True).floor("h")
#     return dt.strftime("%Y-%m-%dT%H:00:00Z")


# def _ensure_cat_cols(df: pd.DataFrame) -> pd.DataFrame:
#     """API 응답 컬럼명 정규화: liveCategory* → category* 로 통일"""
#     d = df.copy()
#     if "categoryId" not in d.columns and "liveCategory" in d.columns:
#         d = d.rename(columns={"liveCategory": "categoryId"})
#     if "categoryValue" not in d.columns and "liveCategoryValue" in d.columns:
#         d = d.rename(columns={"liveCategoryValue": "categoryValue"})
#     return d


# def fetch_all_lives() -> pd.DataFrame:
#     """GET /open/v1/lives 전체 페이지 수집 (시청자순)"""
#     if not HEADERS["Client-Id"] or not HEADERS["Client-Secret"]:
#         raise SystemExit("CHZZK_CLIENT_ID/CHZZK_CLIENT_SECRET 환경변수가 필요합니다.")

#     url = f"{OPENAPI}/open/v1/lives"
#     params = {"size": PAGE_SIZE}
#     rows: List[Dict[str, Any]] = []
#     seen_next = set()

#     while True:
#         r = requests.get(url, headers=HEADERS, params=params, timeout=20)
#         r.raise_for_status()
#         js = r.json() or {}
#         content = js.get("content") or {}
#         data = content.get("data") or []
#         page = content.get("page") or {}
#         rows.extend(data)

#         nxt = page.get("next")
#         if not nxt or nxt in seen_next:
#             break
#         seen_next.add(nxt)
#         params["next"] = nxt
#         time.sleep(SLEEP_BETWEEN)

#     if not rows:
#         return pd.DataFrame()

#     df = pd.json_normalize(rows)

#     # 필요한 컬럼만 유지
#     keep = [c for c in [
#         "liveId","liveTitle","liveThumbnailImageUrl","concurrentUserCount",
#         "openDate","categoryType","liveCategory","liveCategoryValue",
#         "channelId","channelName","channelImageUrl","tags"
#     ] if c in df.columns]
#     df = df[keep].copy()

#     # 타입 정리
#     df["concurrentUserCount"] = pd.to_numeric(df.get("concurrentUserCount", 0), errors="coerce").fillna(0).astype("Int64")
#     for c in ["channelId","channelName","categoryType","liveCategory","liveCategoryValue"]:
#         if c in df.columns:
#             df[c] = df[c].astype(str)

#     return df


# def save_live_snapshot(df: pd.DataFrame) -> Path:
#     """스냅샷 CSV 저장 (전 컬럼 인용 + tags JSON 문자열화)"""
#     ts = _utc_hour_iso()
#     dt = datetime.fromisoformat(ts.replace("Z","+00:00"))
#     outdir = SNAP_ROOT / dt.strftime("%Y/%m/%d")
#     outdir.mkdir(parents=True, exist_ok=True)
#     outpath = outdir / f"{dt.strftime('%H')}.csv"

#     snap = df.copy()
#     if "tags" in snap.columns:
#         snap["tags"] = snap["tags"].apply(
#             lambda v: json.dumps(v, ensure_ascii=False) if isinstance(v, (list, dict))
#             else (v if isinstance(v, str) else "[]")
#         )
#     snap["captured_at_utc"] = ts

#     snap.to_csv(outpath, index=False, encoding="utf-8-sig", quoting=csv.QUOTE_ALL)
#     return outpath


# def upsert_category_matrix(df: pd.DataFrame) -> Path:
#     """
#     categories_matrix.csv
#     - 행: categoryType, categoryId, categoryValue
#     - 열: captured_hour(UTC)
#     - 값: concurrentUserCount 합계
#     """
#     df = _ensure_cat_cols(df)
#     if df.empty:
#         CAT_WIDE.parent.mkdir(parents=True, exist_ok=True)
#         if not CAT_WIDE.exists():
#             pd.DataFrame(columns=["categoryType","categoryId","categoryValue"]).to_csv(CAT_WIDE, index=False, encoding="utf-8-sig")
#         return CAT_WIDE

#     ts_col = _utc_hour_iso()
#     need = ["categoryType","categoryId","categoryValue","concurrentUserCount"]
#     cur = (df[need]
#            .assign(concurrentUserCount=pd.to_numeric(df["concurrentUserCount"], errors="coerce").fillna(0).astype("Int64"))
#            .groupby(["categoryType","categoryId","categoryValue"], dropna=False)["concurrentUserCount"]
#            .sum().astype("Int64").rename(ts_col).reset_index())

#     cur_pivot = (cur.pivot_table(index=["categoryType","categoryId","categoryValue"],
#                                  values=ts_col, aggfunc="last").astype("Int64"))

#     if CAT_WIDE.exists():
#         old = pd.read_csv(CAT_WIDE, dtype=str)
#         num_cols = [c for c in old.columns if c not in ("categoryType","categoryId","categoryValue")]
#         for c in num_cols:
#             old[c] = pd.to_numeric(old[c], errors="coerce").astype("Int64")
#         old = old.set_index(["categoryType","categoryId","categoryValue"])
#         wide = old.reindex(index=old.index.union(cur_pivot.index))
#         wide[ts_col] = cur_pivot
#     else:
#         wide = cur_pivot

#     cols = list(wide.columns)
#     order = np.argsort(pd.to_datetime(cols, utc=True, errors="coerce").values)
#     wide = wide.iloc[:, order]

#     out_df = wide.reset_index()
#     CAT_WIDE.parent.mkdir(parents=True, exist_ok=True)
#     out_df.to_csv(CAT_WIDE, index=False, encoding="utf-8-sig")
#     return CAT_WIDE


# def upsert_game_categories_matrix(df: pd.DataFrame) -> Path:
#     """
#     game_categories_matrix.csv
#     - 대상: categoryType == 'GAME'
#     - 행: categoryType(=GAME), categoryId, categoryValue
#     - 열: captured_hour(UTC)
#     - 값: concurrentUserCount 합계
#     """
#     df = _ensure_cat_cols(df)
#     if df.empty:
#         return GAME_CAT_WIDE

#     game = df[df.get("categoryType", "").astype(str) == "GAME"].copy()
#     if game.empty:
#         if not GAME_CAT_WIDE.exists():
#             pd.DataFrame(columns=["categoryType","categoryId","categoryValue"]).to_csv(GAME_CAT_WIDE, index=False, encoding="utf-8-sig")
#         return GAME_CAT_WIDE

#     ts_col = _utc_hour_iso()
#     game["concurrentUserCount"] = pd.to_numeric(game["concurrentUserCount"], errors="coerce").fillna(0).astype("Int64")

#     cur = (game.groupby(["categoryType","categoryId","categoryValue"], dropna=False)["concurrentUserCount"]
#                 .sum().astype("Int64").rename(ts_col).reset_index())

#     cur_pivot = (cur.pivot_table(index=["categoryType","categoryId","categoryValue"],
#                                  values=ts_col, aggfunc="last").astype("Int64"))

#     if GAME_CAT_WIDE.exists():
#         old = pd.read_csv(GAME_CAT_WIDE, dtype=str)
#         num_cols = [c for c in old.columns if c not in ("categoryType","categoryId","categoryValue")]
#         for c in num_cols:
#             old[c] = pd.to_numeric(old[c], errors="coerce").astype("Int64")
#         old = old.set_index(["categoryType","categoryId","categoryValue"])
#         wide = old.reindex(index=old.index.union(cur_pivot.index))
#         wide[ts_col] = cur_pivot
#     else:
#         wide = cur_pivot

#     cols = list(wide.columns)
#     order = np.argsort(pd.to_datetime(cols, utc=True, errors="coerce").values)
#     wide = wide.iloc[:, order]

#     out_df = wide.reset_index()
#     GAME_CAT_WIDE.parent.mkdir(parents=True, exist_ok=True)
#     out_df.to_csv(GAME_CAT_WIDE, index=False, encoding="utf-8-sig")
#     return GAME_CAT_WIDE


# def upsert_details_matrix(df: pd.DataFrame) -> Path:
#     """
#     details_matrix.csv
#     - 행: captured_hour(UTC)
#     - 열: channelName (스트리머 이름만)
#     - 값: concurrentUserCount (같은 시간/같은 이름은 마지막 값)
#     """
#     if df.empty:
#         return DET_WIDE

#     ts_col = _utc_hour_iso()
#     cur = df.copy()
#     cur["col"] = cur["channelName"].astype(str).str.strip()
#     cur = cur[["col","concurrentUserCount"]].dropna()
#     cur = cur.drop_duplicates(subset=["col"], keep="last").set_index("col")["concurrentUserCount"].astype("Int64")

#     if DET_WIDE.exists():
#         old = pd.read_csv(DET_WIDE, dtype=str)
#         if "captured_hour" not in old.columns:
#             old = old.rename(columns={old.columns[0]:"captured_hour"})
#         for c in old.columns:
#             if c == "captured_hour": continue
#             old[c] = pd.to_numeric(old[c], errors="coerce").astype("Int64")
#         old = old.set_index("captured_hour")
#         wide = old.reindex(columns=old.columns.union(cur.index))
#         wide.loc[ts_col, cur.index] = cur.values
#     else:
#         wide = pd.DataFrame(index=[ts_col], columns=cur.index, dtype="Int64")
#         wide.loc[ts_col] = cur.values

#     wide = wide.sort_index()
#     out_df = wide.reset_index().rename(columns={"index":"captured_hour"})
#     out_df.to_csv(DET_WIDE, index=False, encoding="utf-8-sig")
#     return DET_WIDE


# def main():
#     df = fetch_all_lives()
#     if df.empty:
#         print("빈 응답."); return

#     # 프리뷰
#     if "concurrentUserCount" in df.columns:
#         show_cols = [c for c in ["channelName","concurrentUserCount","liveTitle","categoryType","liveCategoryValue"] if c in df.columns]
#         print("\n=== CHZZK live snapshot (top20 by viewers) ===")
#         print(df.sort_values("concurrentUserCount", ascending=False)[show_cols].head(20).to_string(index=False))

#     snap = save_live_snapshot(df)
#     cat  = upsert_category_matrix(df)
#     det  = upsert_details_matrix(df)
#     game = upsert_game_categories_matrix(df)

#     print(f"\nsaved snapshot  -> {snap}")
#     print(f"updated catwide -> {cat}")
#     print(f"updated detwide -> {det}")
#     print(f"updated gamecat -> {game}")


# if __name__ == "__main__":
#     main()

# collect_chzzk.py
# -*- coding: utf-8 -*-
"""
CHZZK(치지직) 수집기 (피드백 반영판)
- 스냅샷(옵션): data/chzzk/lives/YYYY/MM/DD/HH.csv  → 기본 비활성화(용량 절감)
- 카테고리 와이드(전체): data/chzzk/categories_matrix.csv
- 카테고리 와이드(게임만): data/chzzk/game_categories_matrix.csv
- 디테일 와이드(열=스트리머 이름): data/chzzk/details_matrix.csv (수집 시점 TOP 100만)
"""

import os, time, json, csv
from pathlib import Path
from datetime import datetime, timezone
from typing import Dict, Any, List
import numpy as np
import pandas as pd
import requests

OPENAPI = "https://openapi.chzzk.naver.com"
HEADERS = {
    "Client-Id": os.environ.get("CHZZK_CLIENT_ID", "dcb94fcb-4bf9-4593-9207-b82a413b73e8"),
    "Client-Secret": os.environ.get("CHZZK_CLIENT_SECRET", "RCuiu38XxAu69BqjRmlr5fhzxExUS4P62TkpTFCvHYU"),
    "Content-Type": "application/json",
    "User-Agent": "Mozilla/5.0",
}

# 출력 경로
OUT_ROOT       = Path("data/chzzk")
SNAP_ROOT      = OUT_ROOT / "lives"
CAT_WIDE       = OUT_ROOT / "categories_matrix.csv"
GAME_CAT_WIDE  = OUT_ROOT / "game_categories_matrix.csv"
DET_WIDE       = OUT_ROOT / "details_matrix.csv"

# 스냅샷 저장 여부 (기본 False)
WRITE_LIVE_SNAPSHOTS = os.getenv("WRITE_LIVE_SNAPSHOTS", "false").lower() == "true"

PAGE_SIZE = 20
SLEEP_BETWEEN = 0.25


def _utc_hour_iso(dt=None) -> str:
    if dt is None:
        dt = datetime.now(timezone.utc).replace(microsecond=0)
    dt = pd.to_datetime(dt, utc=True).floor("h")
    return dt.strftime("%Y-%m-%dT%H:00:00Z")


def _ensure_cat_cols(df: pd.DataFrame) -> pd.DataFrame:
    """API 응답 컬럼명 정규화: liveCategory* → category* 로 통일"""
    d = df.copy()
    if "categoryId" not in d.columns and "liveCategory" in d.columns:
        d = d.rename(columns={"liveCategory": "categoryId"})
    if "categoryValue" not in d.columns and "liveCategoryValue" in d.columns:
        d = d.rename(columns={"liveCategoryValue": "categoryValue"})
    return d


def fetch_all_lives() -> pd.DataFrame:
    """GET /open/v1/lives 전체 페이지 수집 (시청자순)"""
    if not HEADERS["Client-Id"] or not HEADERS["Client-Secret"]:
        raise SystemExit("CHZZK_CLIENT_ID/CHZZK_CLIENT_SECRET 환경변수가 필요합니다.")

    url = f"{OPENAPI}/open/v1/lives"
    params = {"size": PAGE_SIZE}
    rows: List[Dict[str, Any]] = []
    seen_next = set()

    while True:
        r = requests.get(url, headers=HEADERS, params=params, timeout=20)
        r.raise_for_status()
        js = r.json() or {}
        content = js.get("content") or {}
        data = content.get("data") or []
        page = content.get("page") or {}
        rows.extend(data)

        nxt = page.get("next")
        if not nxt or nxt in seen_next:
            break
        seen_next.add(nxt)
        params["next"] = nxt
        time.sleep(SLEEP_BETWEEN)

    if not rows:
        return pd.DataFrame()

    df = pd.json_normalize(rows)

    # 필요한 컬럼만 유지
    keep = [c for c in [
        "liveId","liveTitle","liveThumbnailImageUrl","concurrentUserCount",
        "openDate","categoryType","liveCategory","liveCategoryValue",
        "channelId","channelName","channelImageUrl","tags"
    ] if c in df.columns]
    df = df[keep].copy()

    # 타입 정리
    df["concurrentUserCount"] = pd.to_numeric(df.get("concurrentUserCount", 0), errors="coerce").fillna(0).astype("Int64")
    for c in ["channelId","channelName","categoryType","liveCategory","liveCategoryValue"]:
        if c in df.columns:
            df[c] = df[c].astype(str)

    return df


def save_live_snapshot(df: pd.DataFrame) -> Path:
    """스냅샷 CSV 저장 (전 컬럼 인용 + tags JSON 문자열화) — 기본 비활성화"""
    ts = _utc_hour_iso()
    dt = datetime.fromisoformat(ts.replace("Z","+00:00"))
    outdir = SNAP_ROOT / dt.strftime("%Y/%m/%d")
    outdir.mkdir(parents=True, exist_ok=True)
    outpath = outdir / f"{dt.strftime('%H')}.csv"

    snap = df.copy()
    if "tags" in snap.columns:
        snap["tags"] = snap["tags"].apply(
            lambda v: json.dumps(v, ensure_ascii=False) if isinstance(v, (list, dict))
            else (v if isinstance(v, str) else "[]")
        )
    snap["captured_at_utc"] = ts

    snap.to_csv(outpath, index=False, encoding="utf-8-sig", quoting=csv.QUOTE_ALL)
    return outpath


def upsert_category_matrix(df: pd.DataFrame) -> Path:
    """
    categories_matrix.csv
    - 행: categoryType, categoryId, categoryValue
    - 열: captured_hour(UTC)
    - 값: concurrentUserCount 합계
    """
    df = _ensure_cat_cols(df)
    if df.empty:
        CAT_WIDE.parent.mkdir(parents=True, exist_ok=True)
        if not CAT_WIDE.exists():
            pd.DataFrame(columns=["categoryType","categoryId","categoryValue"]).to_csv(CAT_WIDE, index=False, encoding="utf-8-sig")
        return CAT_WIDE

    ts_col = _utc_hour_iso()
    need = ["categoryType","categoryId","categoryValue","concurrentUserCount"]
    cur = (df[need]
           .assign(concurrentUserCount=pd.to_numeric(df["concurrentUserCount"], errors="coerce").fillna(0).astype("Int64"))
           .groupby(["categoryType","categoryId","categoryValue"], dropna=False)["concurrentUserCount"]
           .sum().astype("Int64").rename(ts_col).reset_index())

    cur_pivot = (cur.pivot_table(index=["categoryType","categoryId","categoryValue"],
                                 values=ts_col, aggfunc="last").astype("Int64"))

    if CAT_WIDE.exists():
        old = pd.read_csv(CAT_WIDE, dtype=str)
        num_cols = [c for c in old.columns if c not in ("categoryType","categoryId","categoryValue")]
        for c in num_cols:
            old[c] = pd.to_numeric(old[c], errors="coerce").astype("Int64")
        old = old.set_index(["categoryType","categoryId","categoryValue"])
        wide = old.reindex(index=old.index.union(cur_pivot.index))
        wide[ts_col] = cur_pivot
    else:
        wide = cur_pivot

    cols = list(wide.columns)
    order = np.argsort(pd.to_datetime(cols, utc=True, errors="coerce").values)
    wide = wide.iloc[:, order]

    out_df = wide.reset_index()
    CAT_WIDE.parent.mkdir(parents=True, exist_ok=True)
    out_df.to_csv(CAT_WIDE, index=False, encoding="utf-8-sig")
    return CAT_WIDE


def upsert_game_categories_matrix(df: pd.DataFrame) -> Path:
    """
    game_categories_matrix.csv
    - 대상: categoryType == 'GAME'
    - 행: categoryType(=GAME), categoryId, categoryValue
    - 열: captured_hour(UTC)
    - 값: concurrentUserCount 합계
    """
    df = _ensure_cat_cols(df)
    if df.empty:
        return GAME_CAT_WIDE

    game = df[df.get("categoryType", "").astype(str) == "GAME"].copy()
    if game.empty:
        if not GAME_CAT_WIDE.exists():
            pd.DataFrame(columns=["categoryType","categoryId","categoryValue"]).to_csv(GAME_CAT_WIDE, index=False, encoding="utf-8-sig")
        return GAME_CAT_WIDE

    ts_col = _utc_hour_iso()
    game["concurrentUserCount"] = pd.to_numeric(game["concurrentUserCount"], errors="coerce").fillna(0).astype("Int64")

    cur = (game.groupby(["categoryType","categoryId","categoryValue"], dropna=False)["concurrentUserCount"]
                .sum().astype("Int64").rename(ts_col).reset_index())

    cur_pivot = (cur.pivot_table(index=["categoryType","categoryId","categoryValue"],
                                 values=ts_col, aggfunc="last").astype("Int64"))

    if GAME_CAT_WIDE.exists():
        old = pd.read_csv(GAME_CAT_WIDE, dtype=str)
        num_cols = [c for c in old.columns if c not in ("categoryType","categoryId","categoryValue")]
        for c in num_cols:
            old[c] = pd.to_numeric(old[c], errors="coerce").astype("Int64")
        old = old.set_index(["categoryType","categoryId","categoryValue"])
        wide = old.reindex(index=old.index.union(cur_pivot.index))
        wide[ts_col] = cur_pivot
    else:
        wide = cur_pivot

    cols = list(wide.columns)
    order = np.argsort(pd.to_datetime(cols, utc=True, errors="coerce").values)
    wide = wide.iloc[:, order]

    out_df = wide.reset_index()
    GAME_CAT_WIDE.parent.mkdir(parents=True, exist_ok=True)
    out_df.to_csv(GAME_CAT_WIDE, index=False, encoding="utf-8-sig")
    return GAME_CAT_WIDE


def upsert_details_matrix_top100(df: pd.DataFrame) -> Path:
    """
    details_matrix.csv (TOP 100)
    - 행: captured_hour(UTC)
    - 열: channelName (스트리머 이름만)
    - 값: concurrentUserCount
    - 수집 시점에서 시청자 수 상위 100명만 기록
    """
    if df.empty:
        return DET_WIDE

    ts_col = _utc_hour_iso()

    cur = df.copy()
    # TOP 100 by viewers
    cur = cur.sort_values("concurrentUserCount", ascending=False).head(100).copy()

    cur["col"] = cur["channelName"].astype(str).str.strip()
    cur = cur[["col","concurrentUserCount"]].dropna()
    cur = cur.drop_duplicates(subset=["col"], keep="last").set_index("col")["concurrentUserCount"].astype("Int64")

    if DET_WIDE.exists():
        old = pd.read_csv(DET_WIDE, dtype=str)
        if "captured_hour" not in old.columns:
            old = old.rename(columns={old.columns[0]:"captured_hour"})
        for c in old.columns:
            if c == "captured_hour": continue
            old[c] = pd.to_numeric(old[c], errors="coerce").astype("Int64")
        old = old.set_index("captured_hour")
        wide = old.reindex(columns=old.columns.union(cur.index))
        wide.loc[ts_col, cur.index] = cur.values
    else:
        wide = pd.DataFrame(index=[ts_col], columns=cur.index, dtype="Int64")
        wide.loc[ts_col] = cur.values

    wide = wide.sort_index()
    out_df = wide.reset_index().rename(columns={"index":"captured_hour"})
    out_df.to_csv(DET_WIDE, index=False, encoding="utf-8-sig")
    return DET_WIDE


def main():
    df = fetch_all_lives()
    if df.empty:
        print("빈 응답."); return

    # 콘솔 프리뷰(상위 20)
    if "concurrentUserCount" in df.columns:
        show_cols = [c for c in ["channelName","concurrentUserCount","liveTitle","categoryType","liveCategoryValue"] if c in df.columns]
        print("\n=== CHZZK live snapshot (top20 by viewers) ===")
        print(df.sort_values("concurrentUserCount", ascending=False)[show_cols].head(20).to_string(index=False))

    # lives 스냅샷: 기본 비활성화
    if WRITE_LIVE_SNAPSHOTS:
        snap = save_live_snapshot(df)
        print(f"saved snapshot  -> {snap}")
    else:
        print("snapshot       -> skipped (WRITE_LIVE_SNAPSHOTS=false)")

    cat  = upsert_category_matrix(df)
    det  = upsert_details_matrix_top100(df)
    game = upsert_game_categories_matrix(df)

    print(f"updated catwide -> {cat}")
    print(f"updated detwide -> {det}")
    print(f"updated gamecat -> {game}")


if __name__ == "__main__":
    main()
