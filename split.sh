#/bin/bash
SSD_DIR=/ssd/b2018mhirano/
CUR_DIR=`pwd`
cp data/StandardEquities_$1_out.csv $SSD_DIR
mkdir $SSD_DIR/$1
seq -f %02g 1000 9999 | xargs -I {} -P 32 sh -c "head -n 1 $SSD_DIR/StandardEquities_$1_out.csv > $SSD_DIR/$1/{}.csv"
tail -n +2 $SSD_DIR/StandardEquities_$1_out.csv | xargs -I {} -P 32 sh -c "echo {} | cut -d, -f4 | xargs -I [] sh -c 'echo {} >> $SSD_DIR/$1/[].csv'"
cd $SSD_DIR
cd $1
ls | xargs -I {} -P 32 bash -c "wc -l {} | cut -d' ' -f1 | xargs -I [] sh -c 'if [ "[]" = "1" ]; then rm {}; fi'"
cd ..
zip -r $1.zip $1
rm -r $1
mv $1.zip $CUR_DIR/save

