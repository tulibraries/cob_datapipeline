#/bin/bash --login
# have any error in following cause bash script to fail
set -e

# check that the MARC archived directory exists
if [ ! -e $ALMASFTP_HARVEST_PATH/archived ]
then
  mkdir $ALMASFTP_HARVEST_PATH/archived
fi


cd $ALMASFTP_HARVEST_PATH
tar -cvzf alma_bibs__$ALMASFTP_HARVEST_RAW_DATE.tar.gz alma_bibs__$ALMASFTP_HARVEST_RAW_DATE*.xml
mv alma_bibs__$ALMASFTP_HARVEST_RAW_DATE.tar.gz archived
rm -f alma_bibs__$ALMASFTP_HARVEST_RAW_DATE*.xml
rm -f alma_bibs__$ALMASFTP_HARVEST_RAW_DATE*.xml.tar.gz
