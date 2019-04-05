#!/bin/bash --login


cd $HOME/tul_cob
gem install bundler
bundle install
for f in $1/alma_bibs__*.xml
do
TRAJECT_FULL_REINDEX='yes' SOLR_URL="$2" bundle exec traject -c lib/traject/indexer_config.rb $f
done
return 0
