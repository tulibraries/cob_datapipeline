#!/usr/bin/env bash

set -eo pipefail

# Load shell customizations when available, but don't fail if the file is absent.
if [[ -f "$HOME/.bashrc" ]]; then
  source "$HOME/.bashrc"
fi

export PATH="$HOME/.rbenv/shims:$HOME/.rbenv/bin:$PATH"

: "${GENCON_TEMP_PATH:?GENCON_TEMP_PATH must be set}"
: "${GENCON_CSV_S3:?GENCON_CSV_S3 must be set}"
: "${GIT_BRANCH:?GIT_BRANCH must be set}"
: "${SOLR_URL:?SOLR_URL must be set}"

for required_command in aws git bundle ruby; do
  if ! command -v "$required_command" >/dev/null 2>&1; then
    echo "Error: required command '$required_command' is not available in PATH."
    exit 1
  fi
done

# Get the raw CSV files from S3
mkdir -p "$GENCON_TEMP_PATH"
aws s3 sync "$GENCON_CSV_S3" "$GENCON_TEMP_PATH" --include "*.csv"

if [[ ! -d "gencon_index" ]]; then
  git clone https://github.com/tulibraries/gencon_index.git --branch="$GIT_BRANCH"
  cd gencon_index
else
  # If the repository already exists locally, navigate to its directory and pull the latest changes.

  if git -C "gencon_index" rev-parse --is-inside-work-tree >/dev/null 2>&1; then
      cd gencon_index
      git pull --ff-only origin "$GIT_BRANCH"
  else
      echo "Error: Local 'gencon_index' directory is not a Git repository."
      exit 1
  fi
fi

bundle config set force_ruby_platform true
bundle install --without=debug

bundle exec ruby harvest_all.rb
