################################################################################ 
################################################################################
##                       Concentrations to CSV script
## Introduction:
##   As the manual of the CDICA DATA.
##   In FORTRAN, the data were read by:
## 
##   parameter (maxlat=180, maxlon=360)
## 
##   C DISTRIBUTE C ON GRID
##
##   do 1200, i=1, maxlat
##   do 1200, j=1, maxlon
##   write (14, 1310) gridcar (i,j)
##   1200 continue
##   1310 format (ES13.6E2)
##
##   So, when the data were plot to a WGS_84 geographic projetions, the regions
##   of latitude and longitude are -90 to 90 and -180 to 180. 
##
##   Data can be transformed use follow awk script.
##
## Syntax:
##   awk -f CDICA_Concentrations_to_CSV.awk datafilename
################################################################################ 
################################################################################


#Initiate global variations before program start
BEGIN { 
    FS="E";

#set data regions
    maxlat=180;
    maxlon=360;
    delta_lat=0;
    delta_lon=0;
#latitude_change_flag switch the loop to another latitude.
#Use those variations because I was too fool to write a loop process in the 
#main structure. And I write a man-made global "FOR" loop.
#Maybe someone can rewrite this script
    latitude_change_flag=0

#set output file name
    output="gridcar.csv"
    }

#Get latitude function
function get_lat(){
#In WGS_84 the region of latitude is -90 to 90.
    lat = (maxlat/2) - delta_lat 

    if (delta_lat >maxlat) delta_lat = 0

#Switch to next latitude
    if (latitude_change_flag == 1){
        delta_lat++
        latitude_change_flag = 0
        }
    return lat
    }

#Get longitude function
function get_lon(){
    lon = delta_lon - (maxlon/2)
    delta_lon++

#change the loop into another latitude
    if (delta_lon == maxlon) {
        latitude_change_flag = 1
        delta_lon = 0
        }
    return lon
    }

#THE MAIN METHOD
$0 ~ /[-\+]?[0-9]?\.[0-9]*E[-\+]?[0-9]*/ {

#Change the Fortran DS13.6E2 fromat float into a normal double float.
    carbon=$1*(10**$2);

#Why AWK can't use variation FILENAME in BEGIN proccess?
#Do you know how many time has been waste in this two lines?
#Extract year information from the input file name.
    year=substr(FILENAME,9);

#First you should get the longitude. And, then you can get the latitude.
    longitude = get_lon();
    latitude = get_lat();

#Output format information
    printf("%.6f,%s,%s,%s\n", carbon, longitude, latitude, year) >> output
    }
