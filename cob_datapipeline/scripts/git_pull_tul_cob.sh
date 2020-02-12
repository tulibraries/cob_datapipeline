#!/bin/bash
# Argument 1 is the LATEST_RELEASE flag, if "true" fetch latest tagged release
# Argument 2, if arg 1 is not true, is any git ref or branch to switch to
cd $HOME/cob_index

git fetch https://github.com/tulibraries/cob_index.git  --tags
git reset --hard HEAD

if [ $1 == "true" ]; then
  latest=`git describe --tags $(git rev-list --tags --max-count=1)`
  git checkout $latest
else
  git fetch origin $2
  git checkout $2
  git reset --hard FETCH_HEAD
fi
