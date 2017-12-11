#! /bin/bash -x

################################################################################
################################################################################
##                  Dump data to shapefiles monthly
##  Introduction:
##    Dump database data to shapefiles. Main process is using pgsql2shp.
##    It will dump data from database VIEWs to seperated shapefiles.
##    You should specific the file include month you want to dump 
##    from database.
##    It defined several default option:
##      1) host
##      2) port
##      3) user
##      4) password
##      5) geom
##
##  Usage:
##    ./CDICA_data_to_shp_monthly.sh -i inputfile [-d database -h host -p port \
##      -u user -P password -g geom]
##
################################################################################
################################################################################

#Initiate variables
filename="0000"
inputfile="0000"
host=localhost
port=1921
password=postgres
user=postgres
geom=geom
database=carbonProject

#main process
#process the options in command line
while getopts :h:p:P:u:g:d:i: opt
do
    case $opt in
    h)  host=$OPTARG
        ;;
    p)  port=$OPTARG
        ;;
    P)  password=$OPTARG
        ;;
    u)  user=$OPTARG
        ;;
    g)  geom=$OPTARG
        ;;
    d)  database=$OPTARG
        ;;
    i)  inputfile=$OPTARG
        ;;
    '?')    echo "$0: invalid option -$OPTARG" >&2
            exit 1
            break
            ;;
    esac
done


shift $((OPTIND - 1))
#end the process of options


#Test inputfile is exist
if [ -z $inputfile ]
then
    echo "Incorrect inputfile name!"
    exit 1
fi

#Process every month
while read month
do
    filename=cdica_monthly_$month 
    pgsql2shp -f $filename -h $host -p $port -u $user -P $password \
        -g $geom $database "SELECT * FROM grid_co2.cdica_monthly_$month ;"
done < $inputfile
