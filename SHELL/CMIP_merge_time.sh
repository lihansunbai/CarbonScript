#!/bin/bash

################################################################################
################################################################################
##                   Merge Splited-time Data to Single File
##  Introduction:
##    This script merge split time data into a single file.
##
##  Usage:
##    CMIP_merge_time.sh -d dirction_invole_files_to_merge -o output_file_name
##
################################################################################
################################################################################

### MAIN PROCCESS ###

#process the options in command line
while getopts :d:o: opt
do
    case $opt in
    d)  workdirection=$OPTARG
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

##change direction into work direction
if [ -n "$workdirection" ]
then
    cd $workdirection
else
    exit 1
fi

#get the nc files
mergefiles=`echo *`

if [ -z "$mergefiles" ]
then
    exit 2
fi

cdo mergetime $mergefiles $output
### END OF MAIN PROCCESS ###
