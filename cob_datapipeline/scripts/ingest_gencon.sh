#!/usr/bin/env bash

set -eo pipefail

# export / set all environment variables passed here by task for pick-up by subprocess
export AIRFLOW_USER_HOME="/home/airflow"
export HOME=$AIRFLOW_USER_HOME

source $HOME/.bashrc

export PATH="$AIRFLOW_USER_HOME/.local/bin:$PATH"
export PATH="$AIRFLOW_USER_HOME/.rbenv/shims:$AIRFLOW_USER_HOME/.rbenv/bin:$PATH"

export SOLR_URL="${SOLR_WEB_URL//\/\////$SOLR_AUTH_USER:$SOLR_AUTH_PASSWORD@}"
export GENCON_INDEX_PATH="$PWD/gencon_index"

echo ">>> My Dreictory: $PWD"

# Get the raw CSV files from S3
aws s3 sync $GENCON_CSV_S3 $GENCON_TEMP_PATH --include "*.csv"

if [[ ! -d "$GENCON_INDEX_PATH" ]]; then
  git clone https://github.com/tulibraries/gencon_index.git $GENCON_INDEX_PATH
  cd $GENCON_INDEX_PATH
else
  # If the repository already exists locally, navigate to its directory and pull the latest changes.

  if [[ -d "$GENCON_INDEX_PATH/.git" ]]; then
      cd $GENCON_INDEX_PATH
      git pull origin main
  else
      echo "Error: Local 'gencon_index' directory is not a Git repository."
      exit 1;
  fi
fi

bundle config set force_ruby_platform true
bundle install --without=debug

ruby harvest_all.rb
