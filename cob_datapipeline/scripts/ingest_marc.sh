#/bin/bash --login

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
bundle config set force_ruby_platform true
bundle install --without=debug

# force Ruby’s external/internal encodings to UTF-8
export RUBYOPT='-E UTF-8'
export LANG=en_US.UTF-8
export LC_ALL=en_US.UTF-8

data_in=$(echo $DATA | jq -r '.[]')

if [ -z "$COMMAND" ]; then
  COMMAND=ingest
fi

for file in $data_in
do
  echo "Indexing file: "$file
  bundle exec cob_index $COMMAND $(aws s3 presign s3://$BUCKET/$file)
  processed_file=$(echo $file | sed 's/new-updated/processed-new-updated/' | sed 's/deleted/processed-deleted/')

  # In full reindex context $file and $processed_file are equal.
  if [ "$file" != "$processed_file" ]; then
    aws s3 mv s3://$BUCKET/$file s3://$BUCKET/$processed_file
  fi
done
