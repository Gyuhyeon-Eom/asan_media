"""
아산시 방송효과 분석 - 데이터 탐색 스크립트
회사 PC에서 실행: python explore_data.py
결과가 같은 폴더에 explore_result.txt로 저장됨
"""
import os
import sys
import pandas as pd
from pathlib import Path

BASE = Path(r"C:\Users\admin\Desktop\작업 폴더\01. work\02. 프로젝트\아산시\방송\01.데이터")
OUTPUT = Path(__file__).parent / "explore_result.txt"


def read_file(f, nrows=None):
    """CSV/Excel 파일 읽기 (인코딩 자동 감지)"""
    if f.suffix in ['.xlsx', '.xls']:
        return pd.read_excel(f, nrows=nrows)
    elif f.suffix == '.csv':
        for enc in ['utf-8', 'cp949', 'euc-kr', 'utf-8-sig']:
            try:
                return pd.read_csv(f, encoding=enc, nrows=nrows)
            except (UnicodeDecodeError, UnicodeError):
                continue
        return pd.read_csv(f, encoding='latin1', nrows=nrows)
    return None


def main():
    out = []
    def p(text=""):
        out.append(str(text))
        print(text)

    # 1. 폴더 구조
    p("=" * 70)
    p("[ 1. 폴더 구조 ]")
    p("=" * 70)
    for root, dirs, files in os.walk(BASE):
        depth = len(Path(root).relative_to(BASE).parts)
        indent = "  " * depth
        p(f"{indent}{Path(root).name}/")
        for f in sorted(files):
            fpath = os.path.join(root, f)
            size = os.path.getsize(fpath)
            if size > 1024 * 1024:
                size_str = f"{size / 1024 / 1024:.1f}MB"
            else:
                size_str = f"{size / 1024:.0f}KB"
            p(f"{indent}  {f} ({size_str})")

    # 2. 방송계획 폴더 상세
    p("\n" + "=" * 70)
    p("[ 2. 방송계획 데이터 ]")
    p("=" * 70)
    broadcast_dir = BASE / "방송계획"
    if broadcast_dir.exists():
        for f in sorted(broadcast_dir.iterdir()):
            if f.is_file():
                p(f"\n--- {f.name} ---")
                try:
                    if f.suffix in ['.xlsx', '.xls']:
                        xls = pd.ExcelFile(f)
                        for sheet in xls.sheet_names:
                            df = pd.read_excel(f, sheet_name=sheet)
                            p(f"[시트: {sheet}] shape={df.shape}")
                            p(f"columns: {list(df.columns)}")
                            p(df.head(15).to_string())
                    elif f.suffix == '.csv':
                        df = read_file(f)
                        p(f"shape={df.shape}")
                        p(f"columns: {list(df.columns)}")
                        p(df.head(15).to_string())
                    elif f.suffix == '.txt':
                        p(f.read_text(encoding='utf-8'))
                except Exception as e:
                    p(f"읽기 실패: {e}")
    else:
        p("방송계획 폴더 없음")

    # 3. 각 데이터 폴더 첫 파일 head + 전체 파일 목록
    p("\n" + "=" * 70)
    p("[ 3. 각 데이터 폴더 샘플 ]")
    p("=" * 70)
    for folder in sorted(BASE.iterdir()):
        if folder.is_dir() and folder.name != "방송계획":
            data_files = sorted([
                f for f in folder.rglob("*")
                if f.is_file() and f.suffix.lower() in ['.csv', '.xlsx', '.xls']
            ])
            p(f"\n{'─' * 50}")
            p(f"📁 {folder.name} ({len(data_files)}개 데이터 파일)")
            p(f"{'─' * 50}")

            # 파일 목록
            for f in data_files[:20]:
                rel = f.relative_to(folder)
                size = f.stat().st_size
                size_str = f"{size/1024/1024:.1f}MB" if size > 1024*1024 else f"{size/1024:.0f}KB"
                p(f"  {rel} ({size_str})")
            if len(data_files) > 20:
                p(f"  ... 외 {len(data_files)-20}개")

            # 첫 파일 head
            if data_files:
                f = data_files[0]
                p(f"\n  [샘플: {f.name}]")
                try:
                    df = read_file(f, nrows=5)
                    if df is not None:
                        p(f"  columns ({len(df.columns)}): {list(df.columns)}")
                        p(f"  dtypes:\n{df.dtypes.to_string()}")
                        p(df.head().to_string())
                except Exception as e:
                    p(f"  읽기 실패: {e}")

    # 4. 루트 레벨 파일 (txt 등)
    p("\n" + "=" * 70)
    p("[ 4. 루트 파일 ]")
    p("=" * 70)
    for f in sorted(BASE.iterdir()):
        if f.is_file():
            p(f"\n--- {f.name} ---")
            try:
                if f.suffix == '.txt':
                    for enc in ['utf-8', 'cp949', 'euc-kr']:
                        try:
                            p(f.read_text(encoding=enc))
                            break
                        except:
                            continue
                elif f.suffix in ['.csv', '.xlsx', '.xls']:
                    df = read_file(f, nrows=10)
                    if df is not None:
                        p(f"shape={df.shape}, columns={list(df.columns)}")
                        p(df.head(10).to_string())
            except Exception as e:
                p(f"읽기 실패: {e}")

    # 저장
    OUTPUT.write_text("\n".join(out), encoding="utf-8")
    print(f"\n결과 저장됨: {OUTPUT}")


if __name__ == "__main__":
    main()
