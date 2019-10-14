#/bin/bash

# Untars xml files, then injects a namespace
set -e

xml_files_in=$(aws s3api list-objects --bucket $BUCKET --prefix $SOURCE_FOLDER --query "Contents[?starts_with(Key, '$SOURCE_FOLDER/alma_bibs__') && ends_with(Key, '.xml.tar.gz')]" | jq -r '.[].Key')
bw_files_in=$(aws s3api list-objects --bucket $BUCKET --prefix almasftp --query "Contents[?Key == '$SOURCE_FOLDER/boundwith_merged.xml']" | jq -r '.[].Key')

# grab list of items from designated aws bucket (creds are envvars), then untar & mv each item
for file in $xml_files_in:
do
  file_out="${file/$SOURCE_FOLDER/$DEST_FOLDER}"
  file_out_untarred="${file_out/.tar.gz/}"
  aws s3 cp s3://$BUCKET/$file - | tar xzf - | sed -En "s~<collection><record>~<collection xmlns=\"http://www.loc.gov/MARC21/slim\"><record>~" | aws s3 cp - s3://$BUCKET/$file_out_untarred
done

for file in $bw_files_in:
do
  file_out="${file/$SOURCE_FOLDER/$DEST_FOLDER}"
  file_out_untarred="${file_out/.tar.gz/}"
  aws s3 cp s3://$BUCKET/$file - | tar xzf - | aws s3 cp - s3://$BUCKET/$file_out_untarred
done
