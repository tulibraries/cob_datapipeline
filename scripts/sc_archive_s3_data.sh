#/bin/bash

# Untars xml files, then injects a namespace
set -e
set -o pipefail

source $HOME/.bashrc

data_in=$(echo $DATA_IN | sed "s/\]//g" | sed "s/\[//g" | sed "s/'//g" | sed "s/,//g")

for file in $data_in
do
  file_out="${file/$SOURCE_FOLDER/$DEST_FOLDER}"
  aws s3 cp s3://$BUCKET/$file s3://$BUCKET/$file_out
  aws s3 rm s3://$BUCKET/$file
done
