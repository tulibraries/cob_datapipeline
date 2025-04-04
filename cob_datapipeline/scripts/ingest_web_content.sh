#!/usr/bin/env bash
source $HOME/.bashrc
export PATH="$HOME/.rbenv/shims:$HOME/.rbenv/bin:$PATH"

# This script is used by task_ingest_web_content to load a specific version of
# the web_content indexing executable (cob_web_content_index) and installs and runs the ingest command

# The following environment variables are set for the script:
# * SOLR_WEB_URL: used by the cob_web_content_index to set SOLR_URL for traject process.
# * WEB_CONTENT_BRANCH: used here to checkout the corrent cob_web_content_index version.
# * WEB_CONTENT_BASE_URL: Base url to retreive api data
# * SOLR_AUTH_USER, SOLR_AUTH_PASSWORD: For basic auth requests.
# * SOLR_AUTH_USER, SOLR_AUTH_PASSWORD: For basic auth requests.
# * DELETE_SWITCH: Used to enable/disable delete of collection prior to ingesting.


# * WEB_CONTENT_BASIC_AUTH_USER, WEB_CONTENT_BASIC_AUTH_PASSWORD: used by cob_web_content_index to authenticate.
# * WEB_CONTENT_READ_TIMEOUT: used by cob_web_content_index to handle slow response for large data requests.
# * This is Temporary until the website is in production.

set -e
git clone https://github.com/tulibraries/cob_web_index.git --branch=$WEB_CONTENT_BRANCH
cd cob_web_index
bundle config set force_ruby_platform true
bundle install --without=debug
bundle exec cob_web_index ingest $DELETE_SWITCH
