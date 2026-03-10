"""
勤務表 Excel → CSV 変換スクリプト
使い方: python convert.py <Excelファイル> [出力CSVファイル]
"""
import sys
import pandas as pd


# ══════════════════════════════════════════════════════════════════════════════
# パイプライン定義（ステップを順番に追加していく）
# ══════════════════════════════════════════════════════════════════════════════

def step1_unpivot(df: pd.DataFrame) -> pd.DataFrame:
    """職員番号を固定して、日付列(1〜31)をアンピボット。空セルは除外"""
    day_cols = [c for c in df.columns if c != '職員番号']
    result = df.melt(
        id_vars=['職員番号'],
        value_vars=day_cols,
        var_name='日付',
        value_name='シフト',
    )
    # 月の日数が28〜31で変動しても、空セルの行を除外
    return result.dropna(subset=['シフト']).reset_index(drop=True)


PIPELINE = [
    step1_unpivot,
    # step2_xxx,  ← 次のステップをここに追加
]


# ══════════════════════════════════════════════════════════════════════════════
# 実行
# ══════════════════════════════════════════════════════════════════════════════

def run(input_path: str, output_path: str):
    df = pd.read_excel(input_path, header=0, engine='openpyxl')
    print(f"読み込み完了: {df.shape[0]}行 × {df.shape[1]}列")

    for step in PIPELINE:
        df = step(df)
        print(f"[{step.__name__}] → {df.shape[0]}行 × {df.shape[1]}列")

    df.to_csv(output_path, index=False, encoding='utf-8-sig')
    print(f"書き出し完了: {output_path}")


if __name__ == '__main__':
    if len(sys.argv) < 2:
        print("使い方: python convert.py <Excelファイル> [出力CSVファイル]")
        sys.exit(1)

    input_file = sys.argv[1]
    output_file = sys.argv[2] if len(sys.argv) >= 3 else input_file.rsplit('.', 1)[0] + '_output.csv'

    run(input_file, output_file)
