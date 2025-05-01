#!/usr/bin/env bash

set -eo pipefail

# export / set all environment variables passed here by task for pick-up by subprocess
export AIRFLOW_USER_HOME="/home/airflow"
export HOME=$AIRFLOW_USER_HOME

source $HOME/.bashrc

export PATH="$AIRFLOW_USER_HOME/.rbenv/shims:$AIRFLOW_USER_HOME/.rbenv/bin:$PATH"

cd /opt/airflow/dags/cob_datapipeline/scripts/gencon_dags

export SOLR_URL="${SOLR_WEB_URL//\/\////$SOLR_AUTH_USER:$SOLR_AUTH_PASSWORD@}"

bundle config set force_ruby_platform true
bundle install --without=debug

ruby harvest_all.rb
