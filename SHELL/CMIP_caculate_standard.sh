#!/bin/bash

############################################################
############################################################
##             Caculate standard temperature
##  Introduction:
##    Using cdo(climate data operator) caculate the standard
##    temperature of a definite climate period of 30 years.
##
##  Syntax:
##    ./CMIP_caculate_standard.sh [-s start_year] -f locate_of_cmipdata -o output_file_name
##  
##  Option:
##    -f: where the cmip data locate
##    -s(optional): define the start year of climate period. Default year is 1961
##    -o: output file name
##  
##  Warning:
##    If you have date slice data files, please run 
##    the mergetime script before run this script.
############################################################
############################################################

###############################################################################
###############################################################################
# GLOBAL VARIABLES
# DEFAULT CLIMATE PERIOD
cp_start=1961

# DEFAULT OUTPUT FILE NAME
output="STTAS"

###############################################################################
###############################################################################
# MAIN PROCESS BEGAIN 

#process the options in command line
while getopts :f:s:o: opt
do
    case $opt in
    f)  cmipdata=$OPTARG
        ;;
    s)  cp_start=$OPTARG
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

# change direction into working direction
if ! [ -f "$cmipdata"  ]
then
    printf "Data \'%s\' dose not exist!" $cmipdata
    exit 1
fi
          
# caculate the offset and skip amount
noffset=`echo "($cp_start - 1850) * 12" | bc`
nskip=`echo "(2015 - $cp_start - 30) * 12" | bc`


# call cdo run the time split
temp_out="temp_out"
cdo -splitsel,360,$noffset,$nskip $cmipdata $temp_out

temp_in=`echo $temp_out"000000.nc"`
cdo -timselmean,360 -fldmean $temp_in $output

