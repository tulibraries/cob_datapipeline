#!/bin/bash

cd $HOME/tul_cob

git fetch https://github.com/tulibraries/tul_cob.git  --tags
git reset --hard HEAD

if [ $1 == "true" ]; then
  latest=`git describe --tags $(git rev-list --tags --max-count=1)`
  git checkout $latest
else
  # TODO: we should not hard code master here...
  git fetch origin master
  git checkout master
  git reset --hard FETCH_HEAD
fi
