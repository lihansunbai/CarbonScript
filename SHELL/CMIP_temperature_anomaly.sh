#!/bin/bash

############################################################
############################################################
##             Caculate Temperature Anomaly
##  Introduction:
##    Using cdo(climate data operator) caculate temperature
##    anomaly of CMIP6 data.
##
##  Syntax:
##    ./CMIP_temperature_anomaly.sh -m CMIP_data -s standard_temperature -o output_file_name
##  
##  Option:
##    -m: CMIP data will be infurstructed the process.
##    -s: standard temperature of corresponding CMIP data
##    -o: output file name
##  
##  Warning:
##    You should containt only one data file in the working
##    direction to excuate this script.
##    If you have splited files of data, please run 
##    the mergetime script before run this script.
############################################################
############################################################

###############################################################################
###############################################################################
# GLOBAL VARIABLES
# DEFAULT OUTPUT FILE NAME
output="TA"

###############################################################################
###############################################################################
# MAIN PROCESS BEGAIN 

#process the options in command line
while getopts :m:s:o: opt
do
    case $opt in
    m)  cmipdata=$OPTARG
        ;;
    s)  standard=$OPTARG
        ;;
    o)  output=$OPTARG
        ;;
    '?')    echo "$0: invalid option -$OPTARG" >&2
            exit 1
            break
            ;;
    esac
done

shift $((OPTIND - 1))
#end the process of options

# check the origin cmip data
if [ ! -n $cmipdata ]
then
    printf "CMIP data \'%s\' dose not exist!" $cmipdata
    exit 1
fi
          
# check the origin cmip data
if [ ! -n $standard ]
then
    printf "Standard temperature \'%s\' dose not exist!" $standard
    exit 1
fi

# call cdo run the time series
temp_out="temp.nc"
cdo -yearmean -fldmean $cmipdata $temp_out

cdo -sub $temp_out $standard $output
