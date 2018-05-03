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
    temp_cate = "";
# 这里都在处理IPCC描述的种类的不同情况，主要是截取字段的不同情况
    if (n == 9){
        if (length(filename[9]) == 5) categories_ipcc = substr(filename[9], 1, 1);
        else{
        categories_ipcc = substr(filename[9], 1, length(filename[9])-4);
        }
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
    output_pe="points_data.csv"
    output_wte="world_data.csv"
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
