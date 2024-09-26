#!/usr/bin/env bash

# Get Airflow version from Pipfile
AIRFLOW_VERSION=$(pipenv run pip freeze | grep 'apache-airflow==' | cut -d'=' -f3)
echo "Airflow version: $AIRFLOW_VERSION"

# Get Python version from .python-version (first two parts)
PYTHON_VERSION=$(cut -d'.' -f1-2 .python-version)
echo "Python version: $PYTHON_VERSION"

# Download the Airflow constraints file
curl -O https://raw.githubusercontent.com/apache/airflow/constraints-$AIRFLOW_VERSION/constraints-$PYTHON_VERSION.txt


# Generate the list of packages from the Pipfile
packages=$(grep '\w\s*=\s*"==' Pipfile | cut -d= -f1 | sed 's/[[:space:]]*$/==/')

# Create a new empty requirements.txt file
> requirements.txt

# Loop through each package and check if it's in the constraints file
while IFS= read -r package; do
  match=$(grep "^$package" "constraints-$PYTHON_VERSION.txt")
  if [ "$match" != "" ]; then
    echo $match >> requirements.txt;
  fi
done <<< "$packages"
