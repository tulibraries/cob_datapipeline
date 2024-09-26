# cob_datapipeline

[![CircleCI](https://circleci.com/gh/tulibraries/cob_datapipeline.svg?style=svg)](https://circleci.com/gh/tulibraries/cob_datapipeline)
![pylint Score](https://mperlet.github.io/pybadge/badges/9.47.svg)

**cob_datapipeline** is the repository that holds Airflow DAGs (Directed Acyclic Graphs, e.g., data processing workflows) along with related scripts for Temple University Libraries' Library Search  ([tul_cob](https://github.com/tulibraries/tul_cob)) indexing workflows.

These DAGs (and related scripts) are expecting to be run within an Airflow installation akin to the one built by our [TUL Airflow Playbook (private repository)](https://github.com/tulibraries/ansible-playbook-airflow).

Some DAG tasks in this repository use Temple University Libraries' centralized python library [tulflow](https://github.com/tulibraries/tulflow).

Local Development, QA, and Production environment usage of these DAGs is detailed below.

## Prerequisites

**Libraries & Packages**

- Python. Version as specified in `.python-version`.
- Python Package Dependencies: see the [Pipfile](Pipfile)
- Ruby (for running Traject via the TUL_COB Rails Application). 
  These steps are tested with the following Ruby versions:
  - 3.1.3
- Ruby Libraries:
  - rvm
  - [tul_cob](https://github.com/tulibraries/tul_cob) gemset installed:
    ```
    rvm use 2.4.1@tul_cob --create
    ```

**Airflow Variables**

These variables are initially set in the `variables.json` file.
Variables are listed in [variables.json](variables.json).


**Airflow Connections**

- `SOLRCLOUD`: An HTTP Connection used to connect to SolrCloud.
- `AIRFLOW_S3`: An AWS (not S3 with latest Airflow upgrade) Connection used to manage AWS credentials (which we use to interact with our Airflow Data S3 Bucket).
- `slack_api_default`: Used to report DAG run successes and failures to our internal slack channels.

## Local Development

Local development relies on the [Airflow Docker Dev Setup submodule](https://github.com/tulibraries/airflow-docker-dev-setup). 

This project uses the UNIX `make` command to build, run, stop, configure, and test DAGS for local development. These commands are written to first run the script to change into the submodule directory to use and access the development setup. See the [Makefile](Makefile) for the complete list of commands available.

## Related Documentation and Projects

- [Airflow Docker Development Setup](https://github.com/tulibraries/airflow-docker-dev-setup)
- [Ansible Playbook Airflow](https://github.com/tulibraries/ansible-playbook-airflow)
- [Apache Airflow](https://airflow.apache.org/docs/)
- [CircleCI](https://circleci.com/docs/2.0/configuration-reference/)
- [tulflow](https://github.com/tulibraries/tulflow)
- [tul_cob](https://github.com/tulibraries/tul_cob)


## Linting & Testing

Perform syntax and style checks on airflow code with `pylint`

To install and configure `pylint`
```
$ pip install pipenv
$ pipenv install --dev
```

To `lint` the DAGs
```
$ pipenv run pylint cob_datapipeline
```

Use `pytest` to run unit and functional tests on this project.

```
$ pipenv run pytest
```

`lint` and `pytest` are run automatically by CircleCI on each pull request.

### Make Commands

#### rebuld-pipfile:
Used to rebuild Pipfile while making sure that airflow-constraints are met.

#### compare-dependencies:
Used to automatically check if a new dependency does not match upstream airflow contraints.

## Deployment

CircleCI checks (lints and tests) code and deploys to the QA server when development branches are merged into the `main` branch. Code is deployed to production when a new release is created. See the [CircleCI configuration file](cob_datapipeline/.circleci/config.yml) for details.
