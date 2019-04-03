#!/bin/bash --login
# Load RVM into a shell session *as a function*
if [[ -s "$HOME/.rvm/scripts/rvm" ]] ; then
  printf "First try to load from a user install"
  source "$HOME/.rvm/scripts/rvm"
elif [[ -s "/usr/local/rvm/scripts/rvm" ]] ; then
  printf "Then try to load from a root install"
  source "/usr/local/rvm/scripts/rvm"
else
  printf "ERROR: An RVM installation was not found.\n"
fi

if [ ! -e ${1} ]
then
  exit 1
fi

cd $HOME/tul_cob
rvm use 2.4.1@tul_cob
gem install bundler
bundle install
SOLR_URL='http://162.216.18.86:8983/solr/blacklight-core' bundle exec traject -c lib/traject/indexer_config.rb ${1}
return 0
