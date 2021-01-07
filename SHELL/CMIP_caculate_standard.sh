#!/bin/bash

############################################################
############################################################
##             Caculate standard temperature
##  Introduction:
##    Using cdo(climate data operator) caculate the standard
##    temperature of a definite climate period of 30 years.
##
##  Syntax:
##    ./CMIP_caculate_standard.sh [-s start_year] -w workdirction -o output_file_name
##  
##  Option:
##    -s(optional): define the start year of climate period. Default year is 1961.
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
# DEFAULT CLIMATE PERIOD
cp_start=1961

# DEFAULT OUTPUT FILE NAME
output="STTAS"

###############################################################################
###############################################################################
# MAIN PROCESS BEGAIN 

#process the options in command line
while getopts :w:s:o: opt
do
    case $opt in
    w)  workdirection=$OPTARG
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
if [ -n "$workdirection"  ]
then
    cd $workdirection
else
    printf "The direction \'%s\' dose not exist!" $workdirection
    exit 1
fi
          
# caculate the offset and skip amount
noffset=`echo "($cp_start - 1850) * 12" | bc`
nskip=`echo "(2015 - $cp_start - 30) * 12" | bc`

#get the nc file
tasfile=`find *.nc`

if ! [ -f "./$tasfile"  ]
then
    exit 1
fi

# call cdo run the time split
temp_out="temp_out"
cdo -splitsel,360,$noffset,$nskip $tasfile $temp_out

temp_in=`echo $temp_out"000000.nc"`
cdo -timselmean,360 -fldmean $temp_in $output

