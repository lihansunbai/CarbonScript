#! /bin/bash 

################################################################################
################################################################################
##           delete_temp_files --- delete temp files which for tast data
##  Introduction:
##    这个脚本是用来删除解压出来的各种文件的。删除文件夹内的除了 tar, awk 和
##    sh 之外的所有文件。
##    使用请一定小心！！！
##
##  Syntax:
##     ./CDICA_delete_temp_files.sh
################################################################################
################################################################################

ls  |
  sed -e '/\.tar/d' -e '/\.sh/d' -e '/\.awk/d' |
    tr -d '\r' > files.delete.temp 

while read files
do
    if [ -n files ]
    then
        rm $files
    else
        break
    fi
done < files.delete.temp

