#!/usr/bin/env bash
source $HOME/.bashrc
# export / set all environment variables passed here by task for pick-up by subprocess
export AIRFLOW_USER_HOME="/home/airflow"
export HOME=$AIRFLOW_USER_HOME
source ~/.bashrc
export PATH="$AIRFLOW_USER_HOME/.rbenv/shims:$AIRFLOW_USER_HOME/.rbenv/bin:$PATH"

cd gencon_dags
# have any error in following cause bash script to fail
set -e pipefail
set -aux

set -e
bundle config set force_ruby_platform true
bundle install --without=debug

ruby harvest_all.rb
