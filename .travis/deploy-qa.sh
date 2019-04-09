#!/bin/bash
set -e

ssh vvv -T git@github.com

cd ..
git clone git@github.com/tulibraries/ansible-playbook-airflow.git # clone deployment playbook
cd ansible-playbook-airflow
sudo pip install pipenv # install playbook requirements
pipenv install # install playbook requirements
pipenv run ansible-galaxy -r requirements.yml # install playbook role requirements
cp .circleci/.vault ~/.vault # setup vault password retrieval from travis envvar
chmod +x ~/.vault  # setup vault password retrieval from travis envvar

# deploy to qa using ansible-playbook
pipenv run ansible-playbook -i inventory/qa/hosts playbook.yml --vault-password-file=~/.vault -e 'ansible_ssh_port=9229'
EOF
