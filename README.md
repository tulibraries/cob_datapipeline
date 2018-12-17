# cob_datapipeline

# Installation
## Requirements
### Python
xmltodict
sickle

### Ruby
ruby 2.4.1
rvm and gemset ``rvm use 2.4.1@tul_cob --create``
tul_cob

## Importing airflow private vars
$ airflow variables -i tulcob_import_vars.json

## Create connections in Airflow Admin UI
AIRFLOW_CONN_SOLR_LEADER

## Install systemd config files
https://github.com/apache/incubator-airflow/tree/master/scripts/systemd

## Install logrotate file
cp airflow_logrotated to /etc/logrotate.d/airflow

## Data dir
``/app-data/airflow/``

# Execution
$ systemctl enable airflow-scheduler
$ systemctl enable airflow-webserver
$ systemctl start airflow-scheduler
$ systemctl start airflow-webserver
