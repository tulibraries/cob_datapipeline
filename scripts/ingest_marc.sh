#!/bin/bash
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
bundle install
bundle exec traject -c lib/traject/indexer_config.rb ${1}
