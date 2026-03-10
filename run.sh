#!/usr/bin/env bash
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

echo "=== Excel → CSV パイプライン ==="

# Check Python
if ! command -v python3 &>/dev/null; then
  echo "Error: python3 が見つかりません。Python 3.9以上をインストールしてください。"
  exit 1
fi

# Create / activate venv
VENV="$SCRIPT_DIR/.venv"
if [ ! -d "$VENV" ]; then
  echo "→ 仮想環境を作成中..."
  python3 -m venv "$VENV"
fi

source "$VENV/bin/activate"

# Install dependencies
echo "→ 依存関係をインストール中..."
pip install -q -r "$SCRIPT_DIR/backend/requirements.txt"

# Create uploads dir
mkdir -p "$SCRIPT_DIR/uploads"

echo ""
echo "→ サーバーを起動中: http://localhost:8000"
echo "   (停止: Ctrl+C)"
echo ""

cd "$SCRIPT_DIR/backend"
exec uvicorn main:app --host 0.0.0.0 --port 8000 --reload
