# cleanup_soop_files.py
from pathlib import Path

TARGETS = [
    "soop-collector/data/soop/categories_matrix.csv",
    "soop-collector/data/soop/details/details_matrix.csv",  # 기존 잘못된 합본
]

def main():
    removed = 0
    for p in TARGETS:
        path = Path(p)
        if path.exists():
            path.unlink()
            print(f"🔥 removed: {path}")
            removed += 1
        else:
            print(f"skip (not found): {path}")
    if removed == 0:
        print("Nothing to remove. You're clean!")

if __name__ == "__main__":
    main()