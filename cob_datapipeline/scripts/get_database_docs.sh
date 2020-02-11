#!/usr/bin/env bash

source $HOME/.bashrc
cd $HOME/tul_cob
bundle install --without production
bundle exec ruby bin/libguide_cache.rb
