#!/usr/bin/env bash
#
# Regenerate the `release` branch — a prebuilt copy of @inngest/agent-kit with
# its `dist/` at the repo ROOT, so it can be installed straight from GitHub:
#
#     pnpm add github:eadwinCode/agent-kit#release
#
# The package normally lives in packages/agent-kit (a monorepo subdir) and
# gitignores dist/, so a plain `github:eadwinCode/agent-kit` install would get
# the workspace root with no build output. This branch solves both: package at
# root, dist committed, no build-on-install.
#
# Run from a clean `main` checkout after the changes you want to publish are
# merged. Force-pushes `release`.
#
# Usage: scripts/publish-github-release.sh [branch-name]   (default: release)
set -euo pipefail

REPO="$(cd "$(dirname "$0")/.." && pwd)"
PKG="$REPO/packages/agent-kit"
BRANCH="${1:-release}"
WT="$(mktemp -d)"

cd "$REPO"

echo "==> Building @inngest/agent-kit"
( cd "$PKG" && pnpm build )

echo "==> Rebuilding orphan branch '$BRANCH' with the prebuilt package at root"
# Drop any stale local branch so the orphan checkout is clean on re-runs
# (origin/$BRANCH is the source of truth and is force-pushed below).
git branch -D "$BRANCH" 2>/dev/null || true
git worktree add --detach "$WT" HEAD >/dev/null
(
  cd "$WT"
  git checkout --orphan "$BRANCH"
  git rm -rqf . 2>/dev/null || true
  find . -maxdepth 1 ! -name '.' ! -name '.git' -exec rm -rf {} + 2>/dev/null || true

  cp -R "$PKG/dist" ./dist
  cp "$PKG/package.json" ./package.json
  [ -f "$PKG/README.md" ] && cp "$PKG/README.md" ./README.md || true
  [ -f "$PKG/LICENSE.md" ] && cp "$PKG/LICENSE.md" ./LICENSE.md || true
  [ -f "$REPO/MIGRATION.md" ] && cp "$REPO/MIGRATION.md" ./MIGRATION.md || true

  # Prebuilt artifact: no build/prepare on consumer install, no dev toolchain.
  node -e "const fs=require('fs');const p=JSON.parse(fs.readFileSync('package.json','utf8'));p.scripts={};delete p.devDependencies;fs.writeFileSync('package.json',JSON.stringify(p,null,2)+'\n');"

  VERSION="$(node -p "require('./package.json').version")"
  git add -A
  git commit -q -m "release: prebuilt @inngest/agent-kit@${VERSION} for GitHub install"
  git push -f origin "$BRANCH"
)

git worktree remove --force "$WT"
git branch -D "$BRANCH" 2>/dev/null || true
echo "==> Done. Install with: pnpm add github:eadwinCode/agent-kit#${BRANCH}"
