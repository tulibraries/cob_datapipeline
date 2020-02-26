#!/bin/bash --login
# Argument 1 is the MARCXML file to ingest
# Argument 2 is the SOLR URL
# Argument 3 is the harvest-from date
# Argument 4 is the Traject log file for future parsing
source $HOME/.bashrc
# print some information for future debugging
set -ux
echo $1
echo $2
echo $3
whoami
pwd
echo $HOME
which ruby
which gem
which bundle
ruby -v

# check that the MARC XML file exists
if [ ! -e ${1} ]
then
  exit 1
fi

# make sure the indexer config exists
cd $HOME/cob_index || exit 1
gem install bundler || exit 1
bundle install || exit 1
# ingest the MARC XML, teeing the output to a separate log file
{
SOLR_URL="$2" ALMAOAI_LAST_HARVEST_FROM_DATE="$3" bundle exec traject -c lib/cob_index/indexer_config.rb ${1}
}  2>&1 | tee ${4}
