#!/bin/bash

export WORKING_DIR=`pwd`
echo "> Working dir: $WORKING_DIR"

echo "> Getting playbook..."
git clone git@github.com:tulibraries/ansible-playbook-airflow.git

echo "> Making playbook dir"
sudo mv ansible-playbook-airflow ~/ansible-playbook-airflow
