#!/bin/bash

cd $1

for f in $1/alma_bibs__*.xml.tar.gz
do
tar -xvzf $f
done

for f in $1/alma_bibs__*.xml
do
chmod ug+rw $f
$HOME/tul_cob/bin/massage.sh $f
done
