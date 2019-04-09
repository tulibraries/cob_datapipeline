#!/bin/bash
set -e

eval "$(ssh-agent -s)" # Start ssh-agent cache
chmod 600 .travis/id_rsa # Allow read access to the private key
ssh-add .travis/id_rsa # Add the private key to SSH

if [[ -z "${TRAVIS_BRANCH}" ]]; then
  if [[ "${TRAVIS_BRANCH}" == 'qa' ]] then
    ENV="qa"
  elif [[ "${TRAVIS_BRANCH}" == 'master' ]] then
    ENV="stage"
  else
    echo "Not a deploy environment source"
else
  echo "Something shady is up with Travis"
fi

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
