%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% 该脚本绘制EDGAR数据的，对数化直方图，以及变化。
%% 技术路线：
%%  1. 读取arcgis导出的栅格数据，替换其中的NoData为NaN；
%%  2. 绘制histogram，读取Values属性
%%  3. 绘制Surface
%%
%% ****注意：
%%  importfile_EDGAR.m 函数需要包含在同一目录下。
%%
%% ****注意：
%%      该脚本写法采用Matlab 2014b以后版本推荐的引用对象写法。该版本之前的Matlab将无法运行此脚本。
%%      若需在较低版本Matlab运行，请重写各个子图的标注部分为set()函数格式。
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
clear all
close all
clc


%% 建立文件列表
for i = [1970:2012]
filelist{i-1970+1} =['D:\workplace\workplace\EDGAR\edgar_'  num2str(i) '_log.tif'];
end
%% 存储cell
tiff_cell = {};
%%  读取arcgis导出的栅格数据,替换其中的NoData为NaN
for i = [1:length(filelist)]
importfile_EDGAR(filelist{i});
end

%% 绘制直方图
