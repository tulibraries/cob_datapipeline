#/bin/bash --login

set -e
source $HOME/.bashrc

# check that the MARC XML file exists
if [ ! -e $ALMASFTP_HARVEST_PATH/$DATA_IN ]
then
  exit 1
fi

export PATH="$HOME/.rbenv/shims:$HOME/.rbenv/bin:$PATH"
# grab the catalog indexer (ruby / traject) & install related gems
git clone https://github.com/tulibraries/cob_index.git tmp/cob_index
cd tmp/cob_index
gem install bundler
bundle install
bundle exec cob_index ingest $ALMASFTP_HARVEST_PATH/$DATA_IN
