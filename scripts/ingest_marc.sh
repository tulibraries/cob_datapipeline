#!/bin/bash --login
# Argument 1 is the MARCXML file to ingest
# Argument 2 is the SOLR URL
# Argument 3 is the harvest-from date
source $HOME/.bashrc
whoami
pwd
echo $HOME
which ruby
which gem
which bundle
ruby -v

if [ ! -e ${1} ]
then
  exit 1
fi

cd $HOME/tul_cob
gem install bundler
bundle install
SOLR_URL="$2" ALMAOAI_LAST_HARVEST_FROM_DATE="$3" bundle exec traject -c lib/traject/indexer_config.rb ${1}
return 0
