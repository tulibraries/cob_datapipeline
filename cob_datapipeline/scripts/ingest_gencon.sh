#!/usr/bin/env bash

set -eo pipefail

# export / set all environment variables passed here by task for pick-up by subprocess
export AIRFLOW_USER_HOME="/home/airflow"
export HOME=$AIRFLOW_USER_HOME

source $HOME/.bashrc

export PATH="$AIRFLOW_USER_HOME/.local/bin/aws:$PATH"
export PATH="$AIRFLOW_USER_HOME/.rbenv/shims:$AIRFLOW_USER_HOME/.rbenv/bin:$PATH"

mkdir -p /tmp/gencon
aws s3 sync s3://tulib-airflow-prod/gencon /tmp/gencon --include "*.csv"
#/home/airflow/.local/bin/aws s3 sync s3://tulib-airflow-prod/gencon /tmp/gencon --include "*.csv"

cd $AIRFLOW_USER_HOME/dags/cob_datapipeline/scripts/gencon_dags
#cd /opt/airflow/dags/cob_datapipeline/scripts/gencon_dags

export SOLR_URL="${SOLR_WEB_URL//\/\////$SOLR_AUTH_USER:$SOLR_AUTH_PASSWORD@}"

bundle config set force_ruby_platform true
bundle install --without=debug

ruby harvest_all.rb
