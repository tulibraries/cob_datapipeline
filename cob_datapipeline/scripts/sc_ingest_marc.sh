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
bundle install --without=debug

echo "Grabbing files from S3"
data_in=$(aws s3api list-objects --bucket $BUCKET --prefix $FOLDER | jq -r '.Contents[].Key')

if [ -z "$COMMAND" ]; then
  COMMAND=ingest
fi

for file in $data_in
do
  echo "Indexing file: "$file
  bundle exec cob_index $COMMAND $(aws s3 presign s3://$BUCKET/$file)
  processed_file=$(echo $file | sed 's/new-updated/processed-new-updated/' | sed 's/deleted/processed-deleted/')
  aws s3 mv s3://$BUCKET/$file s3://$BUCKET/$processed_file
done
