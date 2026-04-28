#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
OUT_DIR="$ROOT_DIR/data/raw/diabetes130"
ZIP_PATH="$OUT_DIR/diabetes130.zip"
URL="https://archive.ics.uci.edu/static/public/296/diabetes+130-us+hospitals+for+years+1999-2008.zip"

mkdir -p "$OUT_DIR"
curl -L "$URL" -o "$ZIP_PATH"
unzip -o "$ZIP_PATH" -d "$OUT_DIR"
rm -f "$ZIP_PATH"
echo "Downloaded Diabetes 130 dataset to $OUT_DIR"