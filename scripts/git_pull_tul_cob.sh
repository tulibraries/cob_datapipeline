#!/bin/bash

cd $HOME/tul_cob

git fetch https://github.com/tulibraries/tul_cob.git  --tags
git reset --hard HEAD

if [ "$latest_release" == "true" ]; then
  latest=`git describe --tags $(git rev-list --tags --max-count=1)`
  git checkout $latest
else
  git checkout master
  git pull origin master
fi
