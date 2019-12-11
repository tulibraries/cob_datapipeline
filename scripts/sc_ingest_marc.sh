#/bin/bash --login

set -e
set -o pipefail

source $HOME/.bashrc
export PATH=$HOME/.rbenv/shims:$HOME/.rbenv/bin:$PATH

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

data_in=$(aws s3api list-objects --bucket $BUCKET --prefix $FOLDER | jq -r '.Contents[].Key')

for file in $data_in
do
  bundle exec cob_index ingest https://$BUCKET.s3.amazonaws.com/$file
done
