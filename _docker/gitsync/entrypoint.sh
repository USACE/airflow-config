#!/bin/sh
set -e

# Load env vars or defaults
REPOSITORY_URL="${REPOSITORY_URL:?REPOSITORY_URL is required}"
REMOTE_NAME="${REMOTE_NAME:-origin}"
REMOTE_BRANCH="${REMOTE_BRANCH:-main}"
PULL_INTERVAL="${PULL_INTERVAL:-2m}"
TARGET_DIR="/data"

log() {
  echo "[$(date)] $1"
}

# Clone the repo if not already present
if [ ! -d "$TARGET_DIR/.git" ]; then
  log "Cloning $REPOSITORY_URL into $TARGET_DIR..."
  git clone --depth=1 --branch "$REMOTE_BRANCH" "$REPOSITORY_URL" "$TARGET_DIR"
else
  log "Git repo already exists at $TARGET_DIR"
fi

cd "$TARGET_DIR"

while true; do
  log "Sleeping for $PULL_INTERVAL..."
  sleep "$PULL_INTERVAL"

  log "Pulling from $REMOTE_NAME/$REMOTE_BRANCH..."
  if git pull "$REMOTE_NAME" "$REMOTE_BRANCH"; then
    COMMIT_HASH=$(git rev-parse HEAD)
    COMMIT_MSG=$(git log -1 --pretty=format:"%h %s")
    log "Current Commit: $COMMIT_MSG ($COMMIT_HASH)"
  else
    log "Pull failed."
  fi
done
