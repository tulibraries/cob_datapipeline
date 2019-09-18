#!/usr/bin/env bash
source $HOME/.bashrc

# This script is used by task_ingest_web_content to load a specific version of
# the web_content indexing executable (cob_web_content_index) and installs and runs the ingest command

# The following environment variables are set for the script:
# * SOLR_WEB_URL: used by the cob_web_content_index to set SOLR_URL for traject process.
# * WEB_CONTENT_BRANCH: used here to checkout the corrent cob_web_content_index version.
# * WEB_CONTENT_BASE_URL: Base url to retreive api data
# * SOLR_AUTH_USER, SOLR_AUTH_PASSWORD: For basic auth requests.
# * DELETE_SWITCH: Used to enable/disable delete of collection prior to ingesting.

# * WEB_CONTENT_BASIC_AUTH_USER, WEB_CONTENT_BASIC_AUTH_PASSWORD: used by cob_web_content_index to authenticate.
# * This is Temporary until the website is in production.

# * AIRFLOW_HOME: not currently using.
# * AIRFLOW_DATA_DIR: not currently using.
# * AIRFLOW_LOG_DIR: not currently using.

set -e
git clone https://github.com/tulibraries/cob_web_index.git --branch=$WEB_CONTENT_BRANCH
cd cob_web_index
bundle install
bundle exec cob_web_index ingest $DELETE_SWITCH
