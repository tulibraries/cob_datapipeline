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
cd $HOME/tul_cob
rvm use 2.4.1@tul_cob
gem install bundler
bundle install
for f in $1/alma_bibs__*.xml
do
bundle exec traject -c lib/traject/indexer_config.rb $f
done
return 0
