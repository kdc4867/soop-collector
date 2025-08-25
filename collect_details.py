import requests
import pandas as pd
from datetime import datetime, timezone
import pathlib
import time

BASE = "https://sch.sooplive.co.kr/api.php"
HEADERS = {"User-Agent": "Mozilla/5.0"}
DATA_DIR = pathlib.Path("data/soop/details")
DATA_DIR.mkdir(parents=True, exist_ok=True)

# 수집 대상 카테고리 (원하면 추가/수정)
category_nos = ["00810000", "00040070"]  # 버추얼, FC온라인

PAGE_SIZE = 60
ORDER = "view_cnt_desc"

MASTER_CSV = DATA_DIR / "details_master.csv"
MATRIX_CSV = DATA_DIR / "details_matrix.csv"

def fetch_category_contents(cate_no, page=1, nListCnt=PAGE_SIZE):
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
        time.sleep(0.25)
    return all_items

def save_snapshot_and_master(df: pd.DataFrame, cate_no: str, ts_iso: str):
    # 공통 메타
    df["captured_at_utc"] = ts_iso
    df["platform"] = "soop"
    df["category_no"] = cate_no

    # 시각별 스냅샷 저장
    ts = datetime.fromisoformat(ts_iso.replace("Z", "+00:00"))
    snapshot_dir = DATA_DIR / cate_no / ts.strftime("%Y/%m/%d")
    snapshot_dir.mkdir(parents=True, exist_ok=True)
    snapshot_file = snapshot_dir / f"{ts.strftime('%H')}.csv"
    df.to_csv(snapshot_file, index=False, encoding="utf-8-sig")

    # 마스터 누적(append)
    if MASTER_CSV.exists():
        old = pd.read_csv(MASTER_CSV)
        df_all = pd.concat([old, df], ignore_index=True)
    else:
        df_all = df
    df_all.to_csv(MASTER_CSV, index=False, encoding="utf-8-sig")

def update_matrix():
    """
    details_master.csv -> details_matrix.csv
    행: captured_at_utc(정시로 내림)
    열: f"{category_no}__{broad_no}"
    값: view_cnt (동일시각/동일방송 중복 시 마지막 값)
    """
    if not MASTER_CSV.exists():
        print("no master yet; skip matrix")
        return

    df = pd.read_csv(MASTER_CSV, encoding="utf-8-sig")

    # 필요한 컬럼만
    keep = ["captured_at_utc", "category_no", "broad_no", "view_cnt"]
    keep = [c for c in keep if c in df.columns]
    if set(["captured_at_utc","category_no","broad_no","view_cnt"]) - set(keep):
        print("master columns missing; skip matrix")
        return
    df = df[keep].copy()

    # 타입 보정
    df["view_cnt"] = pd.to_numeric(df["view_cnt"], errors="coerce").fillna(0).astype(int)
    # 정시 버킷 (카테고리 스크립트와 동일하게)
    df["captured_at_utc"] = pd.to_datetime(df["captured_at_utc"], utc=True).dt.floor("H")

    # 동일 시각/동일 방송(카테고리+방송번호) 중복 마지막 값 유지
    df.sort_values(["captured_at_utc"], inplace=True)
    df = df.drop_duplicates(["captured_at_utc", "category_no", "broad_no"], keep="last")

    # 열 키 구성
    df["col_key"] = df["category_no"].astype(str) + "__" + df["broad_no"].astype(str)

    # 피벗 → 와이드
    mat = df.pivot_table(
        index="captured_at_utc",
        columns="col_key",
        values="view_cnt",
        aggfunc="last",
    ).sort_index()

    # 결측은 0 (그 시각에 방송이 없으면 0으로)
    mat = mat.fillna(0).astype(int)

    # 저장
    mat.to_csv(MATRIX_CSV, encoding="utf-8-sig")
    print(f"updated matrix -> {MATRIX_CSV}")

def main():
    now = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
    frames = []
    for cate_no in category_nos:
        items = fetch_all_for_category(cate_no)
        if not items:
            continue
        df = pd.DataFrame(items)
        # 필요한 컬럼만 슬림하게
        cols = ["broad_no", "broad_title", "user_id", "user_nick",
                "view_cnt", "broad_start", "hash_tags"]
        cols = [c for c in cols if c in df.columns]
        df = df[cols]
        save_snapshot_and_master(df, cate_no, now)
        frames.append(df.assign(category_no=cate_no, captured_at_utc=now))

    # 콘솔 확인 (합치기 후 상위 20)
    if frames:
        all_df = pd.concat(frames, ignore_index=True)
        if "view_cnt" in all_df.columns:
            show_cols = [c for c in ["category_no","broad_no","user_id","user_nick","view_cnt","broad_title"] if c in all_df.columns]
            print("\n=== Details snapshot (top 20 by view_cnt) ===")
            print(all_df.sort_values("view_cnt", ascending=False)[show_cols].head(20).to_string(index=False))

    # 매트릭스 업데이트
    update_matrix()

if __name__ == "__main__":
    main()