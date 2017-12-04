clear all
close all
clc

%% construct default read nc data variables
%% nc_start = [1,1,1];
nc_count = [180,90,1];

%% initiate default write geotiff data variables
geo_filename = 'gistemp.tif';
geo_A = [];

%% construct georaster reference variable
geo_R = georasterref('RasterSize',[90,180], ...
                    'RasterInterpretation','cell', ...
                    'ColumnsStartFrom','south', ...
                    'RowsStartFrom','west', ...
                    'LatitudeLimits',[-90,90], ...
                    'LongitudeLimits',[-180,180]);
geo_CoordRefSysCode = 'EPSG:4326';


for i = 841:1652
%% generator of date string
    year_str = 1880 + fix(i/12);
    if mod(i,12) == 0
        year_str = year_str - 1;
        month_str = 12;
    else
        month_str = mod(i,12);
    end
    date_str = [sprintf('%04d',year_str),sprintf('%02d',month_str)];

%% read temperature anomaly data from compressed netcdf
    message_str = sprintf('Getting data of %s',date_str);
    disp(message_str)
    tempdata = ncread('gistemp250.nc','tempanomaly',[1,1,i],[180,90,1]);
    tempdata = tempdata' ;

%% write data to geotiff file
    geo_filename = ['gistemp_',date_str,'.tif'];
    message_str = sprintf('Writing data to %s',geo_filename);
    disp(message_str)
    geotiffwrite(geo_filename,tempdata,geo_R,'CoordRefSysCode',geo_CoordRefSysCode);
end
