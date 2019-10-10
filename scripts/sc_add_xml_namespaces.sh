#/bin/bash

# Untars xml files, then injects a namespace
set -e

case "$OSTYPE" in
  darwin*)  SEDTYPE="BSD" ;;
  bsd*)     SEDTYPE="BSD" ;;
  linux*)   SEDTYPE="LINUX" ;;
  *)        SEDTYPE="LINUX" ;;
esac

if  [ -f /proc/cpufinfo ]; then
  NUM_PROCESSES=`grep -c ^processor /proc/cpuinfo`
else
  NUM_PROCESSES=1
fi

cd $ALMASFTP_HARVEST_PATH

for file in ./alma_bibs__*.xml.tar.gz
do
tar -xvzf $file
done

if [[ $SEDTYPE == "BSD" ]]; then
  find . -name "alma_bibs__*.xml" | xargs -t -n 1 -P $NUM_PROCESSES sed -i "" "s~<collection><record>~<collection xmlns=\"http://www.loc.gov/MARC21/slim\"><record>~"
fi

if [[ $SEDTYPE == "LINUX" ]]; then
  find . -name "alma_bibs__*.xml" | xargs -t -n 1 -P $NUM_PROCESSES sed -i "s~<collection><record>~<collection xmlns=\"http://www.loc.gov/MARC21/slim\"><record>~"
fi
