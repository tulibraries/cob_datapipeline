#!/bin/bash --login
# Argument 1 is the directory containing MARCXML files to ingest
# Argument 2 is the SOLR URL
source $HOME/.bashrc
echo $1
echo $2
whoami
pwd
echo $HOME
which ruby
which gem
which bundle
ruby -v

cd $HOME/cob_index
gem install bundler
bundle install
for f in $1/alma_bibs__*.xml
do
TRAJECT_FULL_REINDEX='yes' SOLR_URL="$2" bundle exec traject -c lib/traject/indexer_config.rb $f
done
return 0
