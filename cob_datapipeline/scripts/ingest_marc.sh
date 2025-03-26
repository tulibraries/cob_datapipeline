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

if [ -z "$COMMAND" ]; then
  COMMAND=ingest
fi

ingest_marc_file() {
  set -eo pipefail  # Ensure failures stop execution inside this function
  cd tmp/cob_index || exit 1
  file="$1"
  echo "Indexing file: "$file
  bundle exec cob_index $COMMAND $(aws s3 presign s3://$BUCKET/$file)
  processed_file=$(echo $file | sed 's/new-updated/processed-new-updated/' | sed 's/deleted/processed-deleted/')

  # In full reindex context $file and $processed_file are equal.
  if [ "$file" != "$processed_file" ]; then
    aws s3 mv s3://$BUCKET/$file s3://$BUCKET/$processed_file
  fi
}

export -f ingest_marc_file
export COMMAND
export BUCKET
export SOLR_AUTH_USER
export SOLR_AUTH_PASSWORD
export SOLR_URL
export ALMAOAI_LAST_HARVEST_FROM_DATE

echo "$DATA" | jq -r '.[]' | xargs -n 1 -I {} bash -c 'igest_marc_file "{}"'

