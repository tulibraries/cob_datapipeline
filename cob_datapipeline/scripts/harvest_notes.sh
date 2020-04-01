#!/bin/bash --login

set -eo pipefail

source $HOME/.bashrc

get_latest_release_number() {
  curl --silent "https://github.com/$1/releases/latest" | sed 's#.*tag/\(.*\)\".*#\1#'
}

if [ $LATEST_RELEASE == "true" ]; then
  GIT_BRANCH=`get_latest_release_number tulibraries/cob_index`
fi

git clone https://github.com/tulibraries/cob_index.git tmp/cob_index --branch=$GIT_BRANCH
cd tmp/cob_index
gem install bundler
bundle install --without=debug
mkdir output; cd output
bundle exec cob_index harvest
aws s3 sync . s3://tulib-airflow-prod/electronic-notes/$DATETIME --exclude '*' --include '*.json'
