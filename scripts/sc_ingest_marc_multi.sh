#/bin/bash --login
# have any error in following cause bash script to fail
set -e

source $HOME/.bashrc
export PATH="$HOME/.rbenv/shims:$HOME/.rbenv/bin:$PATH"
# grab the catalog indexer (ruby / traject) & install related gems
git clone https://github.com/tulibraries/cob_index.git tmp/cob_index
cd tmp/cob_index
gem install bundler
bundle install

for file in $ALMASFTP_HARVEST_PATH/alma_bibs__*.xml
do
  bundle exec cob_index ingest $file
done
