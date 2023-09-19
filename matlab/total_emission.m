%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% ****注意：
%%      该脚本写法采用Matlab 2014b以后版本推荐的引用对象写法。该版本之前的Matlab将无法运行此脚本。
%%      若需在较低版本Matlab运行，请重写各个子图的标注部分为set()函数格式。
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
clear all
close all
clc


%% 建立文件列表
for i = [1970:2018]
    filelist{i-1970+1} =['D:\\workplace\\geodatabase\\raster\\total_emission_'  num2str(i) '.TIFF'];
end
%% 存储mat
tiff_mat = [];
%%  读取arcgis导出的栅格数据,替换其中的NoData为NaN
for i = [1:length(filelist)]
    temp_rawdata = importdata(filelist{i});
    temp_rawdata(temp_rawdata == -999) = NaN;
    temp_rawdata(temp_rawdata == 0) = NaN;
    temp_rawdata = sum(temp_rawdata,'all','omitnan');
    tiff_mat = [tiff_mat temp_rawdata];
end

