#!/bin/bash

###############################################################################
###############################################################################
##             EDGAR_run.sh --- EDGAR carbon grided data to shapefile
##  Introduction:
##    产生这个脚本的原因完全是因为EDGAR给的数据从txt开始处理实在是太大了。
##    一般情况下，读入所有文件到csv以后就已经达到了18G，如果进一步导入数据库，
##    在不添加索引和geom列的情况下整个数据表已经达到50G左右。如果，再生成索引
##    和geom列，这个数据表可能超过130G！这个大小是不能接受的！如此大的表无论是
##    储存还是操作都对硬盘造成极大空间和I/O操作负担。
##    所以，有必要尽心另一种思路。
##   
##   这里，我采用单个分类，单个处理，各个击破的方案。
##   每个分类从txt处理成为csv，导入postgresql，生成geom列，导出到shapefile，删除表。
##   采取时间换空间的方案~~~嘤嘤嘤，一寸光阴一寸金~~~
##
##   脚本会自动操作本路径下的所有数据，所以，谨慎操作！
##
##  Syntax:
##    ./EDGAR_run.sh
##
###############################################################################
###############################################################################

###############################################################################
###############################################################################
# GLOBAL VARIABLES
db_host=localhost
db_port=1921
db_user=postgres
db_database=carbonProject

###############################################################################
###############################################################################
# FUNCTIONS
db_create_table(){
    table_create=`echo 'CREATE TABLE grid_co2.EDGAR_'$table_name'(edgar_ver TEXT,substance TEXT,substance_info TEXT,yr INT,categories_abbr TEXT,categories_edgar TEXT,categories_ipcc TEXT,latitude DOUBLE PRECISION,longitude DOUBLE PRECISION,emi DOUBLE PRECISION);'`
    psql -h $db_host -p $db_port -U $db_user -w -d $db_database -c $table_create
   
}

db_import_and_copy(){
    if ! [ -f $1 ]
    then
        echo "ERROR: No import data file!"
        return 1
    fi
    table_name=`echo $1 | \
                awk '{na=substr($0, match($0, "E[0-9].*_[0-9]*\.csv")); print na;}' | \
                    cut -d . -f 1` 
    db_create_table $table_name
    copy_data = `echo "\\copy EDGAR_"$table_name" FROM "$1" (FORMAT csv, DELIMITER ',');"`
    psql -h $db_host -p $db_port -U $db_user -w -d db_database -c $copy_data 

}

###############################################################################
###############################################################################
# MAIN SCRIPT BEGAIN
# Run EDGAR_txt_to_csv script to generate data. This data will be import
# to database.
if [ -f ./EDGAR_txt_to_csv.sh ]
then
    bash ./EDGAR_txt_to_csv.sh ./
else
    echo "Please put EDGAR_txt_to_csv.sh file to location same with EDGAR_run.sh!"
fi

# find all csv data to process list
find -regex '.*csv' -type f > ./import.DAT.temp

# check import file list
if [ -f ./import.DAT.temp ] && [ -s ./import.DAT.temp ]
then
    sed -e '/world_data.csv/d' ./import.DAT.temp > ./import.temp.DAT
    rm ./import.DAT.temp
else 
    echo "ERROR: No import.temp.DAT. Check result of EDGAR_run.sh!"
    exit 1
fi

# MAIN PROCESS BEGAIN
while read im
do
    db_import_and_copy $im
done < ./import.temp.DAT
# MAIN PROCESS END

# delete temp file
#if [ -f ./import.temp.DAT ]
#then
#    rm ./import.temp.DAT
#fi

# MAIN SCRIPT END
