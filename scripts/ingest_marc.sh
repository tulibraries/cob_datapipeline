#!/bin/bash --login
# Load RVM into a shell session *as a function*
cd $HOME/tul_cob
gem install bundler
bundle install
bundle exec traject -c lib/traject/indexer_config.rb ${1}
return 0
