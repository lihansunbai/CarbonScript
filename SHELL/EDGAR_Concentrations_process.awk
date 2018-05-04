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
##   awk -f CDICA_Concentrations_to_CSV.awk -v raw_data_txt
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
    get_filename = substr(ARGV[1], index(ARGV[1], "_txt")+5);
    n = split(get_filename, filename, "_");
    edgar_ver = filename[1];
    substance = filename[2];
    substance_info = filename[3] "_" filename[4] "_" filename[5] "_" filename[6];
    yr = filename[7];

# 这里都在处理IPCC描述的种类的不同情况，主要是截取字段的不同情况
# n == 9的判断情况是因为原始文件名分割后的第9项开始是分类
# ipcc的分类中有一些分类只有一个整数数字，处理起来略麻烦，这里采用强行只截取一个字符
# 上面这条感觉怪怪的，感觉之前的逻辑有点复杂，下次再分析过程的时候再修改吧
# 这里因该可以处理所有情况吧，否则，好像设置保存文件那里就可能出错
    temp_cate = "";
    if (n == 9){
        if (length(filename[9]) == 5) categories_ipcc = substr(filename[9], 1, 1);
        else{
        categories_ipcc = substr(filename[9], 1, length(filename[9])-4);
        }
# 所有分类缩写都是以E开头，主要是因为数据库和各种高级语言中数字开头的变量命名都是非法的
        categories_abbr = "E" toupper(categories_ipcc);
    }
    else{
        for(i=9; i<=n; i++){
            temp_cate = temp_cate "+" filename[i];
        }
        categories_ipcc = substr(temp_cate, 2, length(temp_cate)-5);
        categories_abbr = "E" toupper(substr(categories_ipcc, 1, index(categories_ipcc,"+")-1));
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
