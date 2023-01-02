################################################################################ 
################################################################################
##  将不同年份的Tabulate Area的统计数据转换为数据库可用格式
##  注意：
##  初步可用！待完善细节！
##  
##  Usage：awk -f EDGAR_tabulate_area_decomposer.awk output_file_name
################################################################################ 
################################################################################
# 文件预处理部分
BEGIN{
    FS=",";

    # TODO 增加自定义输出文件名的功能
    # if (ARGC < 2){
    #     print "Output file name does not exist."
    #     exit 1
    # }

    output_file = "ouput.csv"
}

# 处理首行：从标题行中提取每个唯一值的名字。
NR == 1 {
    # trim_length = length($0) - 2
    # trim_str = substr($0, 1, trim_length)
    # trim_str = gsub(/\r\n/, "", $0)
    filed_numbers = split($0, table_fields, ",")
}

# 处理非首行
# 遍历行中的所有非空行，并输出到文件中。
((NR != 1) && ($0 !~ /^$/)) {
    for(i = 3; i < NF; i++){
        value_name = substr(table_fields[i], 7)
        printf("%s,%s,%s,%s", $2, $i, value_name, $NF) >> output_file
    }
}
# 文件结尾部分
END{
    print "Process finished."
}