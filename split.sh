#!/bin/bash
python spliter.py $1
if [ $((`wc -l save/$1/* | tail -n 1 | sed -E "s/total//" | bc` - `ls save/$1/* | wc -l | bc`)) !=  $((`wc -l data/StandardEquities_$1_out.csv | sed -E "s/data\/StandardEquities_$1_out.csv//" | bc` - 1)) ]; then
  echo "check error"
  exit 1
else
  cd save
  if [ `ls $1/* | xargs -I {} -P 31 uniq -d {} | wc -l` != 0 ]; then
    echo "check error"
    exit 1
  else
    zip -r $1.zip $1
    rm -r $1
  fi
fi
exit 0
