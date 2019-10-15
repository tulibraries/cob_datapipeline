#/bin/bash --login

set -e

source $HOME/.bashrc
export PATH="$HOME/.rbenv/shims:$HOME/.rbenv/bin:$PATH"

get_latest_release_number() {
  curl --silent "https://github.com/$1/releases/latest" | sed 's#.*tag/\(.*\)\".*#\1#'
}

if [ $LATEST_RELEASE == "true" ]; then
  GIT_BRANCH=`get_latest_release_number tulibraries/cob_index`
fi

git clone https://github.com/tulibraries/cob_index.git tmp/cob_index --branch=$GIT_BRANCH

cd tmp/cob_index

gem install bundler
bundle install

for file in `ls $ALMASFTP_HARVEST_PATH/$DATA_IN`
do
  bundle exec cob_index ingest $file
done
