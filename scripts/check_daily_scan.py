# scripts/check_daily_scan.py
# -*- coding: utf-8 -*-
import os, re, glob
import pandas as pd

SOURCE_HINTS = {
    'oy_kor':    ['올리브영_랭킹','oliveyoung_kor','oy_kor'],
    'oy_global': ['올리브영글로벌','oliveyoung_global','oy_global'],
    'amazon_us': ['아마존US','amazon_us','amazonUS'],
    'qoo10_jp':  ['큐텐재팬','qoo10','Qoo10'],
    'daiso_kr':  ['다이소몰','daiso'],
}

def infer_source(path:str):
    base = os.path.basename(path).lower()
    parent = os.path.basename(os.path.dirname(path)).lower()
    for src, hints in SOURCE_HINTS.items():
        for h in hints:
            if h.lower() in base or h.lower() in parent:
                return src
    return None

def infer_date_from_filename(fn: str):
    m = re.search(r'(\d{4}-\d{2}-\d{2})', os.path.basename(fn))
    return pd.to_datetime(m.group(1)) if m else pd.NaT

def read_csv_any(path: str) -> pd.DataFrame:
    for enc in ('utf-8-sig','cp949','utf-8','euc-kr'):
        try:
            return pd.read_csv(path, encoding=enc)
        except Exception:
            pass
    return pd.read_csv(path)

def main():
    data_dir = os.getenv('DATA_DIR','./data/daily')
    paths = glob.glob(os.path.join(data_dir,'**','*.csv'), recursive=True)
    print(f"[SCAN] DATA_DIR={data_dir}, csv_files={len(paths)}")
    per_src = {k:[] for k in SOURCE_HINTS}
    for p in paths:
        src = infer_source(p)
        if src: per_src[src].append(p)
    for src, files in per_src.items():
        print(f"\n== {src} ==")
        if not files:
            print("  (발견된 파일 없음)")
            continue
        dates = []
        total_rows = 0
        sample_cols = set()
        for p in sorted(files)[-10:]:
            try:
                df = read_csv_any(p)
            except Exception as e:
                print(f"  ! 읽기 실패: {p} - {e}")
                continue
            total_rows += len(df)
            sample_cols.update(df.columns.tolist())
            if 'date' in df.columns:
                try:
                    dates.append(pd.to_datetime(df['date']).max())
                except Exception:
                    dates.append(infer_date_from_filename(p))
            else:
                dates.append(infer_date_from_filename(p))
        if dates:
            print(f"  파일수={len(files)}, 최근10개 행수합={total_rows}")
            print(f"  컬럼샘플={sorted(sample_cols)[:20]}")
            print(f"  날짜범위: {pd.Series(dates).min().date()} ~ {pd.Series(dates).max().date()}")
        else:
            print("  날짜 추출 실패(파일명에 YYYY-MM-DD 필요)")

if __name__ == "__main__":
    main()
