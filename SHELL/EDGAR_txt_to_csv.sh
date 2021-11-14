#!/bin/bash

###############################################################################
###############################################################################
##             EDGAR_txt_to_csv.sh --- EDGAR carbon grided data transformer
##  Introduction:
##    EDGAR 提供的 CO2 格网（格点）数据是单行的浓度数据。脚本将所有选出类型的
##    不同排放源数据整合为可导入数据库的csv独立文件。
##    数据包含一行信息标题，需要从标题一行中提取部分信息。
##    数据已经包含座标信息。
##    数据采用的分隔符为';'。
##    使用该脚本需要保证awk分类文件与脚本在同一目录下。同时还需要注意，EDGAR在
##       5.0版本以后的文件命名方式发生了变化，由直接使用IPCC分类命名变化为自己的
##       命名分类。选择对应的版本所需的awk分割文件，旧版本用v432进行标注，新版本
##      未注明版本号信息。
##
##    注意：
##        *展开所有数据可能会消耗大量时间！
##        *展开所有数据可能会占据大量磁盘空间！
##        *切勿修改从EDGAR下载的txt文件解压后的文件夹名，该文件夹名为重要分类
##         信息
##        *这个脚本可以单独使用，比如提取所有分类排放的全球总排放信息，其他情况
##         暂时未有明确用途
##
##  Syntax:
##    ./EDGAR_txt_to_csv.sh path_to_extracted_top_dir
##
##  Input:
##    path_to_extracted_top_dir: top dirction of extracted txt file from EDGAR
###############################################################################
###############################################################################

# check argument is valid for a  path
if [ -z $1 ]
then
    print 'Path to extracted files should be input!'
    exit 1
fi

# Generate files list which will be add colum
find $1 -type f | 
   sed -e '/\.temp/d' -e '/\.html/d' -e '/\.sh/d' -e '/\.awk/d' > files.temp.DAT 

# use AWK to process every .txt files
# the process detail be writen in EDGAR_Concentrations_process.awk file.
while read files
do
    awk -f ./EDGAR_Concentrations_process.awk $files
    rm $files
done < files.temp.DAT

# delete temp file
if [ -e files.temp.DAT ]
then
    rm files.temp.DAT
fi
