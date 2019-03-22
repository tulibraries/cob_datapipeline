# cob_datapipeline

![pylint Score](https://mperlet.github.io/pybadge/badges/2.6.svg)

This is the repository for TUL COB (Temple University Libraries Catalog on Blacklight) Airflow DAGs (Directed Acyclic Graphs, e.g., data processing workflows) along with related scripts.

These DAGs (and related scripts) are expecting to be run within an Airflow installation akin to the one built by our [TUL Airflow Playbook (private repository)](https://github.com/tulibraries/ansible-playbook-airflow).

Local Development, QA, Stage, and Production environment usage of these DAGs is detailed below.

## Repository Structure

WIP.

## Airflow Expectations

These the Airflow expectations for these TUL COB DAGs and scripts to successfully run. These apply across environments (development, QA, stage, production).

**Libraries & Packages**

- Python. These steps are tested with the following Python versions:
  - 3.6.8 (with pip version 18.1)
  - 3.7.2 presents errors with an `enum34` vs `enum` error.
- Python Libraries: see the [Pipfile](Pipfile).
- Ruby (for running Traject via TUL_COB Rails Application) These steps are tested with the following Ruby versions:
  - 2.4.1
- Ruby Libraries & Applications:
  - [tul_cob](https://github.com/tulibraries/tul_cob)
  - rvm and tul_cob gemset installed:
    ```
    rvm use 2.4.1@tul_cob --create
    ```

**Airflow Variables**

For these TUL COB Dags, the following variables are required:

- **AIRFLOW_DATA_DIR**: The directory that Airflow puts or pulls data being processed from.
- **AIRFLOW_HOME**: The local Airflow home directory. Should match `$AIRFLOW_HOME` and the value for airflow_home in the `Airflow.cfg` configuration file. By default, this is `~/airflow` for the application.
- **ALMASFTP_HOST**: The SFTP Host for the Alma instance (sandbox or production) used by these workflows. This is the SFTP box that has Alma bibliographic data dumps for full indexing to the designated `BLACKLIGHT_CORE_NAME` in Solr.
- **ALMASFTP_PASSWD**: The password for `ALMASFTP_USER` to access the SFTP Host for the Alma instance (sandbox or production) used by these workflows. This is the SFTP box that has Alma bibliographic data dumps for full indexing to the designated `BLACKLIGHT_CORE_NAME` in Solr.
- **ALMASFTP_PORT**: The port, if non-standard, for accessing the SFTP Host for the Alma instance (sandbox or production) used by these workflows. This is the SFTP box that has Alma bibliographic data dumps for full indexing to the designated `BLACKLIGHT_CORE_NAME` in Solr.
- **ALMASFTP_USER**: The user that with `ALMASFTP_PASSWD` will access the SFTP Host for the Alma instance (sandbox or production) used by these workflows. This is the SFTP box that has Alma bibliographic data dumps for full indexing to the designated `BLACKLIGHT_CORE_NAME` in Solr.
- **ALMA_OAI_ENDPOINT**: The ALMA bibliographic data changeset OAI endpoint accessed for indexing partial updates to the designated `BLACKLIGHT_CORE_NAME` in Solr.
- **BLACKLIGHT_CORE_NAME**: This should be the Solr collection / core name you want to index records into as part of your environment TUL COB DAGs work.
- **almafullreindex_inprogress**: Flag variable to indicate whether or not a Alma bibliographic full index is in progress.
- **almaoai_last_harvest_date**: The date of the last successfully completed partial Alma bibliographic index process.
- **almaoai_last_num_oai_delete_recs**: The number of bibliographic records successfully processed for deleting from the index in the last partial index.
- **almaoai_last_num_oai_update_recs**: The number of bibliographic records successfully processed for updating or creation in the last partial index.
- **traject_num_rejected**: The number of bibliographic records reject by the Traject indexing process during a partial indexing process.

**Airflow Connections**

WIP.

**Environment Variables**

- AIRFLOW_HOME (defaults to ~/airflow)
- AIRFLOW_DATA_DIR (where to put & pull data processed by Airflow)
- AIRFLOW_LOGS_DIR (where to put Airflow Logs; also managed in Airflow config)

**Infrastructure**

## Linting, Testing, Coverage

WIP.

## Running these DAGs

### Development: Install & Run Airflow & COB DAGs

**Requirements Installation & Spin-up**

See above for information what exactly we are installing here. Install setup and development requirements by running:

```
# Ensure you have the correct Python & Pip running;
# your output will vary, but check for Python 3.6.* & that version's Pip.
$ python --version
  Python 3.6.8
$ pip --version
  pip 18.1 from /home/tul08567/.pyenv/versions/3.6.8/lib/python3.6/site-packages/pip (python 3.6)
# Install Pipenv;
$ pip install pipenv
  Collecting pipenv
    Using cached https://files.pythonhosted.org/packages/13/b4/3ffa55f77161cff9a5220f162670f7c5eb00df52e00939e203f601b0f579/pipenv-2018.11.26-py3-none-any.whl
  Requirement already satisfied: setuptools>=36.2.1 in /home/tul08567/.pyenv/versions/3.6.8/lib/python3.6/site-packages (from pipenv) (40.6.2)
  Requirement already satisfied: pip>=9.0.1 in /home/tul08567/.pyenv/versions/3.6.8/lib/python3.6/site-packages (from pipenv) (18.1)
  Collecting virtualenv (from pipenv)
    Downloading https://files.pythonhosted.org/packages/33/5d/314c760d4204f64e4a968275182b7751bd5c3249094757b39ba987dcfb5a/virtualenv-16.4.3-py2.py3-none-any.whl (2.0MB)
      100% |████████████████████████████████| 2.0MB 11.1MB/s
  Collecting certifi (from pipenv)
    Downloading https://files.pythonhosted.org/packages/60/75/f692a584e85b7eaba0e03827b3d51f45f571c2e793dd731e598828d380aa/certifi-2019.3.9-py2.py3-none-any.whl (158kB)
      100% |████████████████████████████████| 163kB 16.6MB/s
  Collecting virtualenv-clone>=0.2.5 (from pipenv)
    Using cached https://files.pythonhosted.org/packages/e3/d9/d9c56deb483c4d3289a00b12046e41428be64e8236fa210111a1f57cc42d/virtualenv_clone-0.5.1-py2.py3-none-any.whl
  Installing collected packages: virtualenv, certifi, virtualenv-clone, pipenv
  Successfully installed certifi-2019.3.9 pipenv-2018.11.26 virtualenv-16.4.3 virtualenv-clone-0.5.1
# Install requirements in Pipenv. Latest Apache version requires the envvar.
$ SLUGIFY_USES_TEXT_UNIDECODE=yes pipenv install --dev
  Pipfile.lock not found, creating…
  Locking [dev-packages] dependencies…
  ✔ Success!
  Locking [packages] dependencies…
  ✔ Success!
  Updated Pipfile.lock (6d2295)!
  Installing dependencies from Pipfile.lock (6d2295)…
    🐍   ▉▉▉▉▉▉▉▉▉▉▉▉▉▉▉▉▉▉▉▉▉▉▉▉▉▉▉▉▉▉▉▉ 80/80 — 00:00:18
  To activate this project's virtualenv, run pipenv shell.
  Alternatively, run a command inside the virtualenv with pipenv run.
# Activate Pipenv shell for the rest of the development steps.
$ pipenv shell
  Launching subshell in virtual environment…
  # environment loading for your particular shell
(cob_datapipeline) $
```

This gets Airflow and needed libraries (for Airflow and for the TUL COB DAGs) installed in your Pipenv virtual environment. You can see and managed the Airflow auto-generated configurations by looking in your `$AIRFLOW_HOME` (by default, `~/airflow`) directory.

Now you have to setup your local environment Airflow database; here, we're just using SQLite for our Airflow database & the `SequentialExecutor`, which runs Airflow jobs on the local machine and without concurrency. You can use other databases and executors if you wish; see the other environment setups to understand how they need to be connected to Airflow:

```
(cob_datapipeline) $ airflow initdb
  [2019-03-21 14:34:26,312] {__init__.py:51} INFO - Using executor SequentialExecutor
  DB: sqlite:////home/tul08567/airflow/airflow.db
  [2019-03-21 14:34:26,503] {db.py:338} INFO - Creating tables
  INFO  [alembic.runtime.migration] Context impl SQLiteImpl.
  ...
  Done.
```

Finally, start running the airflow webserver and the airflow scheduler (you'll need multiple shell sessions during local development, or to run these as background jobs):

```
(cob_datapipeline) $ airflow webserver -p 8080
  [2019-03-21 14:44:47,863] {__init__.py:51} INFO - Using executor SequentialExecutor
    ____________       _____________
   ____    |__( )_________  __/__  /________      __
  ____  /| |_  /__  ___/_  /_ __  /_  __ \_ | /| / /
  ___  ___ |  / _  /   _  __/ _  / / /_/ /_ |/ |/ /
   _/_/  |_/_/  /_/    /_/    /_/  \____/____/|__/

  [2019-03-21 14:44:48,130] {models.py:273} INFO - Filling up the DagBag from /home/tul08567/airflow/dags
  Running the Gunicorn Server with:
  Workers: 4 sync
  Host: 0.0.0.0:8080
  Timeout: 120
  Logfiles: - -
  ... # running process in this shell
# In a new shell, in Pipenv:
$ pipenv shell
(cob_datapipeline) $ airflow scheduler
  [2019-03-21 14:46:44,609] {__init__.py:51} INFO - Using executor SequentialExecutor
    ____________       _____________
   ____    |__( )_________  __/__  /________      __
  ____  /| |_  /__  ___/_  /_ __  /_  __ \_ | /| / /
  ___  ___ |  / _  /   _  __/ _  / / /_/ /_ |/ |/ /
   _/_/  |_/_/  /_/    /_/    /_/  \____/____/|__/

  [2019-03-21 14:46:44,783] {jobs.py:1477} INFO - Starting the scheduler
  # In a new shell, in Pipenv:
```

**Import DAGs to Local Airflow**

This gets a local copy of the TUL COB DAGs into your local Airflow Environment for development purposes. By default, this local installation of Airflow expects DAGs to be places in `$AIRFLOW_HOME/dags`

```
(cob_datapipeline) $ mkdir $AIRFLOW_HOME/dags
(cob_datapipeline) $ cd $AIRFLOW_HOME/dags
(cob_datapipeline) $ git clone https://github.com/tulibraries/cob_datapipeline.git
```
There are quite a few ways you can get the cob_datapipeline DAGs into that directory; just ensure that the directory mirrors the order seen in the Github repository layout.

**Set DAG Required Airflow Variables for Local Airflow**

To load the required variables into your local airflow, you can perform the following process:

```
(cob_datapipeline) $ airflow variables -i config/airflow-variables.json
  [2019-03-21 16:46:26,868] {__init__.py:51} INFO - Using executor SequentialExecutor
  [2019-03-21 16:46:27,030] {models.py:160} WARNING - cryptography not found - values will not be stored encrypted.
  13 of 13 variables successfully updated.
```

You should update that `airflow-variables.json` file to have the variables you need for your local development environment, particularly the `AIRFLOW_DATA_DIR` `AIRFLOW_HOME` variables, replacing `~/airflow` with the absolute path to your airflow installation.
Note: Airflow does then later update these variables during processes.

**Set DAG Required Airflow Connections for Local Airflow**

WIP.


### Dev: COB DAGS & Airflow Setup on Lurch

WIP.

**Importing airflow private vars**

```
$ airflow variables -i tulcob_import_vars.json
```

**Create connections in Airflow Admin UI**

AIRFLOW_CONN_SOLR_LEADER

**Install systemd config files**

https://github.com/apache/incubator-airflow/tree/master/scripts/systemd

**Install logrotate file**

```
$ cp airflow_logrotated to /etc/logrotate.d/airflow
```

**Airflow Data dir**

`/app-data/airflow/`

**Log dir**

`/app-data/airflow/logs/`

**Execution**

```
$ systemctl enable airflow-scheduler
$ systemctl enable airflow-webserver
$ systemctl start airflow-scheduler
$ systemctl start airflow-webserver
```

### QA: Deploy COB DAGs to Airflow

WIP.

### Stage: Deploy COB DAGs to Airflow

WIP.

### Production: Deploy COB DAGs to Airflow

WIP.
