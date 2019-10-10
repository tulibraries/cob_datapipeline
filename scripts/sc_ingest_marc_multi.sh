#/bin/bash --login

set -e

source $HOME/.bashrc
export PATH="$HOME/.rbenv/shims:$HOME/.rbenv/bin:$PATH"
# grab the catalog indexer (ruby / traject) & install related gems
git clone https://github.com/tulibraries/cob_index.git tmp/cob_index
cd tmp/cob_index
gem install bundler
bundle install

for file in $ALMASFTP_HARVEST_PATH/$DATA_IN
do
  bundle exec cob_index ingest $file
done
