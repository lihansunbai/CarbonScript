################################################################################ 
################################################################################
##                       Create a blank background raster
## Introduction:
##   When calculating edgar raster to each other, raw data in .txt files did not
##   have total points of worldwide. So create a blank raster of whole world is
##   neccessery to mosicing categories data into it.
##
## Syntax:
##   awk -f EDGAR_Creat_blank_raster background_value_lines_files
##
## Input:
##   6480000 lines background value
################################################################################ 
################################################################################


#Initiate global variations before program start
BEGIN { 

#set data regions
    maxlat=180;
    maxlon=360;
    delta_lat=0;
    delta_lon=0;
#latitude_change_flag switch the loop to another latitude.
#Use those variations because I was too fool to write a loop process in the 
#main structure. And I write a man-made global "FOR" loop.
#Maybe someone can rewrite this script
    latitude_change_flag=0;

#set output file name
    output="blank_raster.csv";
}

#Get latitude function
function get_lat(){
#In WGS_84 the region of latitude is -90 to 90.
    lat = (maxlat/2) - delta_lat;

    if (delta_lat >maxlat) delta_lat = 0;

#Switch to next latitude
    if (latitude_change_flag == 1){
        delta_lat = delta_lat + 0.1;
        latitude_change_flag = 0;
        }
    return lat - 0.05;
    }

#Get longitude function
function get_lon(){
    lon = delta_lon - (maxlon/2);
    delta_lon = delta_lon + 0.1;

#change the loop into another latitude
    if (int(delta_lon) == maxlon) {
        latitude_change_flag = 1;
        delta_lon = 0;
        }
    return lon + 0.05;
    }

#THE MAIN METHOD
{
#First you should get the longitude. And, then you can get the latitude.
    longitude = get_lon();
    latitude = get_lat();

#Output format information
    printf("%s,%s,%s\n", $1, longitude, latitude) >> output;
}
