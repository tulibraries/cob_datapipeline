#/bin/bash

set -e

get_latest_release_number() {
  curl --silent "https://github.com/$1/releases/latest" | sed 's#.*tag/\(.*\)\".*#\1#'
}

if [ ! -d $HOME/cob_index ]; then
  cd $HOME
  git clone https://github.com/tulibraries/cob_index.git
fi

cd $HOME/cob_index

git fetch origin --tags

if [ $LATEST_RELEASE == "true" ]; then
  GIT_BRANCH=`get_latest_release_number tulibraries/cob_index`
fi

git fetch origin $GIT_BRANCH
git checkout $GIT_BRANCH
git reset --hard FETCH_HEAD
