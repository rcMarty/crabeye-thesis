#!/usr/bin/env bash
set -euo pipefail

mkdir -p pages/pdf
cp src/main.pdf pages/main.pdf

if ! command -v texcount >/dev/null 2>&1; then
  sudo apt-get update
  sudo apt-get install -y texlive-extra-utils
fi

{
  echo '# Build statistics'
  echo ''
  echo "- Branch: ${GITHUB_REF_NAME:-unknown}"
  echo "- Commit: ${GITHUB_SHA:-unknown}"
  echo "- Built at (UTC): $(date -u '+%Y-%m-%d %H:%M:%S')"
  echo ''
  echo '## TeXcount output'
  echo ''
  echo '```text'
  (cd src && texcount -inc -total main.tex)
  echo '```'
} > pages/STATS.md

if [ -f readme.md ]; then
  cp readme.md pages/README.md
elif [ -f README.md ]; then
  cp README.md pages/README.md
else
  printf '%s\n' '# README' '' 'README was not found in the repository.' > pages/README.md
fi

cp .github/pages/index.html pages/index.html
cp .github/pages/pdf/index.html pages/pdf/index.html
