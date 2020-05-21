#/bin/bash
SSD_DIR=/ssd/b2018mhirano/
CUR_DIR=`pqd`
cp data/StandardEquities_$1_out.csv $SSD_DIR
mkdir $SSD_DIR/$1
seq -f %02g 1000 9999 | xargs -I {} sh -c "head -n 1 $SSD_DIR/StandardEquities_$1_out.csv > $SSD_DIR/$1/{}.csv"
tail -n +2 $SSD_DIR/StandardEquities_$1_out.csv | xargs -I {} sh -c "echo {} | cut -d, -f4 | xargs -I [] sh -c 'echo {} >> $SSD_DIR/$1/[].csv'"
cd $SSD_DIR
zip -r $1 $1.zip
rm -r $1
mv $1.zip $CUR_DIR/save

