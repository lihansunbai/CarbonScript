%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% 该脚本绘制EDGAR 碳排放总量场不同阶段的年排放速率
%% 技术路线：
%%  1. 读取arcgis导出的栅格数据，替换其中的NoData为NaN；
%%  2. 绘制histogram，读取Values属性
%%  3. 绘制Surface
%%
%%
%% ****注意：
%%      该脚本写法采用Matlab 2014b以后版本推荐的引用对象写法。该版本之前的Matlab将无法运行此脚本。
%%      若需在较低版本Matlab运行，请重写各个子图的标注部分为set()函数格式。
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
clear all
close all
clc


%% 建立文件列表
for i = [2000:2009]
    filelist{i-2000+1} =['D:\\workplace\\geodatabase\\raster\\total_emission_'  num2str(i) '.TIFF'];
end

%% 存储cell
tiff_cell = {};
%%  读取arcgis导出的栅格数据,替换其中的NoData为NaN
for i = [1:length(filelist)]
    temp_rawdata = importdata(filelist{i});
    temp_rawdata(temp_rawdata == -999) = NaN;
    temp_rawdata(temp_rawdata == 0) = NaN;
    tiff_cell{end+1} = temp_rawdata;
end

%% 元胞转矩阵
emission_matrix = zeros(1800,3600);
for i = [1:length(tiff_cell)]
    temp_emission_matrix = cell2mat(tiff_cell(i)); 
    emission_matrix = cat(3, emission_matrix, temp_emission_matrix);
end

%% 修整碳排放总量场的结果
emission_matrix = emission_matrix(:,:,2:end);

%% 计算阶段的累积排放
%% 其实就是均值
emission_rates = mean(emission_matrix,3,"omitnan");



% %% 延时间维度计算标准差
% emission_variance = std(emission_matrix,1,3,"omitnan");

% %% 修正标准差中为0的栅格，这些栅格是分配模型的冗余错误
% emission_variance(emission_variance==0)=NaN;

% %% 计算场均值
% emission_mean = mean(emission_matrix,3,"omitnan");

% %% 计算变异系数
% emission_variance_rates = emission_variance ./ emission_mean ;

%% matrix 结果转geotiff
%% 设置空间参考
temp_georefference = georasterref('RasterSize',size(emission_rates),'LatitudeLimits',[-90 90],'LongitudeLimits',[-180 180]);
%% 输出文件名
tiffile = 'emission_rates_00.tif' ;
geotiffwrite(tiffile,flip(emission_rates,1),temp_georefference)
%% Read geotiff file
%%[A, R] = geotiffread(tiffile);