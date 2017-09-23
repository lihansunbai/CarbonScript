#!/bin/bash

################################################################################
################################################################################
##                              Date Generator
##  Introduction:
##    This is a generator to generate a series of date string in format.
##    All the dates are output to a specific file that with default name of a
##    name you input.
##    The date format in to styles:
##      1) yyyy
##      2) yyyymm
##
##  Usage:
##    ./Date_Generator.sh [-y/-m] -s start_year -e end_year -o output_file_name
##
##    There are shell default options in script:
##      1) start_year=1751
##      2) end_year=2013
##      3) output="date.year"
################################################################################
################################################################################

#set the variables, which is global and defaul
start_year=1751
end_year=2013
flag="year"
output="date.year"

#funcion of generate date without months
year_generator(){
    for (( i=$1; i<=$2; i++ ))
    do
    printf "%d\n" $i >> $3
    done

}

#funcion of generate date with months
month_generator(){
    for (( i=$1; i<=$2; i++ ))
    do
        for (( j=1; j<13; j++ ))
        do
            printf "%d%.2d\n" $i $j >> $3
        done
    done
}


#main process
#process the options in command line
while getopts :yms:e:o: opt
do
    case $opt in
    y)  flag="year"
        ;;
    m)  flag="month"
        ;;
    s)  start_year=$OPTARG
        ;;
    e)  end_year=$OPTARG
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

#swich two formats of dates
case $flag in
    year)    year_generator $start_year $end_year $output
             ;;
    month)    month_generator $start_year $end_year $output
              ;; 
esac
