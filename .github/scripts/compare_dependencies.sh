#!/usr/bin/env bash

# Get Airflow version from Pipfile
AIRFLOW_VERSION=$(pipenv run pip freeze | grep 'apache-airflow==' | cut -d'=' -f3)
echo "Airflow version: $AIRFLOW_VERSION"

# Get Python version from .python-version (first two parts)
PYTHON_VERSION=$(cut -d'.' -f1-2 .python-version)
echo "Python version: $PYTHON_VERSION"

# Download the Airflow constraints file
curl -O https://raw.githubusercontent.com/apache/airflow/constraints-$AIRFLOW_VERSION/constraints-$PYTHON_VERSION.txt

# Get a list of current dependencies.
pipenv run pip freeze | grep '==' > project-dependencies.txt

# Compare direct dependencies with constraints
mismatch_found=false
while IFS= read -r dep; do
  dep_name=$(echo $dep | cut -d'=' -f1)
  project_version=$(echo $dep | cut -d'=' -f3)

  # Find the dependency in the constraints file
  constraint_version=$(grep "^$dep_name==" constraints-$PYTHON_VERSION.txt | cut -d'=' -f3)

  pipfile_match=$(grep "$dep_name\s*=" Pipfile)

  if [ "$pipfile_match" = "" ]; then
    # Don't worry about things we aren't pinning in the Pipfile
    continue
  fi

  if [ "$constraint_version" != "" ] && [ "$constraint_version" != "$project_version" ]; then
    echo "Version mismatch for $dep_name: project ($project_version) != constraint ($constraint_version)"
    mismatch_found=true
  fi
done < project-dependencies.txt

# Fail the script if any mismatch is found
if [ "$mismatch_found" = true ]; then
  echo "Dependency version mismatches found!"
  exit 1
else
  echo "All direct dependencies match constraint versions."
fi
