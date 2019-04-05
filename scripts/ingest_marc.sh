#!/bin/bash --login

if [ ! -e ${1} ]
then
  exit 1
fi

cd $HOME/tul_cob
gem install bundler
bundle install
SOLR_URL="$2" bundle exec traject -c lib/traject/indexer_config.rb ${1}
return 0
