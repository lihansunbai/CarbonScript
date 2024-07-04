################################################################################ 
################################################################################
##                       Concentrations to CSV script
## Introduction:
##   From EDGAR data files, we can get information of co2 emission, which are 
##   emission types, time, substances, and etc.
##   Data can be transformed using this awk script.
##   EDGAR raw file will be conver into to files, which are points emissions file 
##   that consist with latitude and longitude and world total emissions file.
##
##   points emission: points_data.csv
##   world total emission: world_data.csv
##
## Syntax:
##   awk -f EDGAR_Concentrations_to_CSV.awk raw_data_txt
##
## Input:
##   raw_data_txt: edgar_grid_txt
################################################################################ 
################################################################################

#Initiate global variations before program start
BEGIN { 
    FS="[:;]";
    categories_edgar = ""

# extract information from filename
# 文件名居然是第1标号变量。这个ARGV数组组成真是略奇怪啊......
# "get_filename = substr(ARGV[1], index(ARGV[1], "_txt")+5);"这一句里，
#    index(xxx)+5 是因为，在完整处理流程中，这个 awk 分割文件用到了完整路径名的
#    输入参数，例如“./xxx/xxxxx/xxxx/xxx_txt/xxxxx_xxxx_xxxx_xxx.txt”，
#    所以需要利用“_txt”作为分隔符来分割出需要处理的文件名。这里的“+5”是因为
#    index()方法计算的位置包括了分隔符，返回值是分割符的第一个字符所在的位置。
#    当然在这里为了分割出单纯的文件名，还需要从中删除“/”字符，
#    所以需要后移5个字符，即“+5”
    get_filename = substr(ARGV[1], index(ARGV[1], "_txt")+5);
    n = split(get_filename, filename, "_");
    edgar_ver = filename[1];
    substance = filename[4];
    substance_info = filename[2] "_" filename[3];
    yr = filename[5];

# 提取排放类型信息:
#    这里都在处理排放部门（sector）种类的不同情况，主要是截取字段的不同情况。
#    n == 6 的判断情况是因为原始文件名分割后的第6项开始是分类。
#    EDGAR 给出的命名分类情况中，有部分是多种排放类型合并为同一个文件，所以要依次
#       处理每个提到的项目，也就是第6项以后的所有项。
#   这里因该可以处理所有情况吧，否则，好像设置保存文件那里就可能出错…
    temp_cate = "";
    if (n == 6){
        categories_ipcc = substr(filename[8], 1, length(filename[8])-4);
        categories_abbr = categories_ipcc;
    }
    else{
        for(i=8; i<=n; i++){
            temp_cate = temp_cate "_" filename[i];
        }
        categories_ipcc = substr(temp_cate, 2, length(temp_cate)-5);
        categories_abbr = categories_ipcc;
    }


#set output file name
    output_pe = categories_abbr "_" yr ".csv";
    output_wte = "world_data.csv";
    }

#THE MAIN METHOD
# process world total emission
$0 ~ /^(Compound\:)/{
    categories_edgar = $4;
    total = substr($8, 1, index($8, "(")-1);

#Output format information
    printf("%s,%s,%s,%s,%s,%s,%s,%s\n", edgar_ver, substance, substance_info, yr, categories_abbr, categories_edgar, categories_ipcc, total) >> output_wte;
    }

# process points emission
$0 ~ /[0-9\.-]*\;[0-9\.-]*\;/{
    latitude = $1 - 0.05;
    longitude = $2 + 0.05;

#Output format information
    printf("%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n", edgar_ver, substance, substance_info, yr, categories_abbr, categories_edgar, categories_ipcc, latitude, longitude, $3) >> output_pe;
    }
