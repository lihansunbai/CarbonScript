#!/bin/bash -x

###############################################################################
###############################################################################
##             CDICA_run.sh --- CDICA carbon grided data transformer
##  Introduction:
##    CDICA 提供的 CO2 格网（格点）数据是单行的浓度数据。根据数据说明文档给出
##    的提示，整个数据可以按纬向展开为全球数据。所以逐一解压文件并添加经纬度
##    和时间字段。
##    注意：
##        展开所有数据可能会消耗大量时间！
##        展开所有数据可能会占据大量磁盘空间！
##  Syntax:
##    ./CDICA_run.sh compressed_file.tar
###############################################################################
###############################################################################

#uncompress all .Z files.
#if have argument of .tar files, first extract .z files from .tar file.

if [ -n $1 ]
then
    tar -vxf $1
fi

#uncompress .z files.
#option 'v' means uncompressing files under verbose model.
#option 'r' means uncompressing files recrusively.
uncompress -vr ./

#Generate files list which will be add colum
ls | 
   sed -e '/\.awk/d' -e '/\.tar/d' -e '/\.doc/d' \
       -e '/\.txt/d' -e '/\.DAT/d' -e '/\.sh/d' > files.temp.DAT 

#use AWK to process every .z files
#the process detail be writen in Concentration_to_CSV file.
while read files
do
    awk -f ./CDICA_Concentrations_to_CSV.awk $files
    rm $files
done < files.temp.DAT
