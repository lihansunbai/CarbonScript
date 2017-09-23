#!/bin/bash

start_year=1751
end_year=2013
flag="year"
output="date.year"

year_generator(){
    for (( i=$1; i<=$2; i++ ))
    do
    printf "%d\n" $i >> $3
    done

}

month_generator(){
    for (( i=$1; i<=$2; i++ ))
    do
        for (( j=1; j<13; j++ ))
        do
            printf "%d%.2d\n" $i $j >> $3
        done
    done
}


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


case $flag in
    year)    year_generator $start_year $end_year $output
             ;;
    month)    month_generator $start_year $end_year $output
              ;; 
esac
