#!/usr/bin/env bash
source $HOME/.bashrc

# This script is used by task_ingest_databases to load a specific version of
# the az indexing executable (cob_az_index) and installs and runs the ingest command

# The following environment variables are set for the script:
# * SOLR_AZ_URL: used by the cob_az_index to set SOLR_URL for traject process.
# * AZ_BRANCH: used here to checkout the corrent cob_az_index version.
# * AZ_CLIENT_ID, AZ_CLIENT_SECRET: used by cob_az_index to authenticate against az service.
# * AZ_DELETE_SWITCH: Used to enable/disable delete of collection prior to ingesting.
# * SOLR_AUTH_USER, SOLR_AUTH_PASSWORD: For basic auth requests.
# * AIRFLOW_HOME: not currently using.
# * AIRFLOW_DATA_DIR: not currently using.
# * AIRFLOW_LOG_DIR: not currently using.

set -e
git clone https://github.com/tulibraries/cob_az_index.git --branch=$AZ_BRANCH
cd cob_az_index
bundle install --without=debug
bundle exec cob_az_index ingest $AZ_DELETE_SWITCH
