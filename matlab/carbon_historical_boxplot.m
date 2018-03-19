%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% This matlab script plot three boxplots of global fossil fuel CO2 emission in one figure
%% 绘制全球化石燃料CO2排放四分位图
%%      *排放数据源为CDIAC和EDGAR
%%      *CDIAC时间序列为1751-2014
%%      *EDGAR时间序列为1970-2016
%%      *排放量已做对数处理
%% ****注意：
%%      该脚本写法采用Matlab 2014b以后版本推荐的引用对象写法。该版本之前的Matlab将无法运行此脚本。
%%      若需在较低版本Matlab运行，请重写各个子图的标注部分为set()函数格式。
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% clean various and existed figures
clear all
close all
clc

% load data from mat file
load('historical_nations.mat');

%%% 每一个绘图都要保存绘图的Handle 信息
figure; 

%%% CDIAC 1751-2014
CDIAC_ax = subplot(2,2,[1,2]);
hold on
% draw boxplot
boxplot(CDIAC_ax,CDIAC_co2_all_lg,CDIAC_yr,...
        'MedianStyle','line',...
        'PlotStyle','compact',...
        'OutlierSize',3);
% draw global total emissions line
plot(CDIAC_ax,CDIAC_co2_sum_lg,...
    'Color',[0.9804 0.5020 0.0392],...
    'LineWidth',1.5);
% set labels
CDIAC_ax.XLim = [1769.5 2016.5]-1751;
CDIAC_ax.YLim = [-0.5 7.5];
CDIAC_ax.XTick = [1750:25:2025]-1750;
CDIAC_ax.XTickLabel = [1750:25:2025];
CDIAC_ax.YTick = [0:7];
CDIAC_ax.YTickLabel = [0:7];
CDIAC_ax.YGrid = 'on';
CDIAC_ax.YLabel = ylabel('log_{10}(C) /kt C');
CDIAC_ax.XLabel = xlabel('Year');
CDIAC_ax.Title = title(CDIAC_ax,...
                      {'Global Fossil Fuels Emission CO_{2}';...
                      'Source Data: CDIAC Time Series:1751-2014'});
legend(CDIAC_ax,...
       'global total emission',...
       'Location','best',...
       'Orientation','horizontal')
CDIAC_ax.Box = 'off';
hold off

%%% CDIAC 1970-2014
CDIAC_1970_ax = subplot(2,2,3);
hold on
% draw boxplot
boxplot(CDIAC_1970_ax,CDIAC_co2_all_lg,CDIAC_yr,...
        'OutlierSize',3);
% draw global total emissions line
plot(CDIAC_1970_ax,CDIAC_co2_sum_lg,...
     'Color',[0.9804 0.5020 0.0392],...
     'LineWidth',1.5);
% set labels
CDIAC_1970_ax.XLim = [1968 2020]-1751;
CDIAC_1970_ax.XTick = [1970:10:2020]-1750;
CDIAC_1970_ax.XTickLabel = [1970:10:2020];
CDIAC_1970_ax.YLim = [-1.5 7.5];
CDIAC_1970_ax.YGrid = 'on';
CDIAC_1970_ax.YLabel = ylabel('log_{10}(C) /kt C');
CDIAC_1970_ax.XLabel = xlabel('Year');
CDIAC_1970_ax.Title = title(CDIAC_1970_ax,...
                            {'Global Fossil Fuels Emission CO_{2}';...
                            'Source Data: CDIAC Time Series:1970-2014'});
legend(CDIAC_1970_ax,...
       'global total emission',...
       'Location','best',...
       'Orientation','horizontal')
CDIAC_1970_ax.Box = 'off';
hold off

%%% EDGAR 1970-2016
EDGAR_ax = subplot(2,2,4);
hold on
% draw boxplot
boxplot(EDGAR_ax,EDGAR_co2_all_lg,EDGAR_yr,...
        'Colors','k',...
        'OutlierSize',3);
% draw global total emissions line
plot(EDGAR_ax,EDGAR_co2_sum_lg,...
     'Color',[0.9804 0.5020 0.0392],...
     'LineWidth',1.5);
% set labels
EDGAR_ax.XLim = [1970 2020]-1970;
EDGAR_ax.XTick = [1970:10:2020]-1970;
EDGAR_ax.XTickLabel = [1970:10:2020];
EDGAR_ax.YLim = [-1.5 7.5];
EDGAR_ax.YGrid = 'on';
EDGAR_ax.YLabel = ylabel('log_{10}(C) /kt C');
EDGAR_ax.XLabel = xlabel('Year');
EDGAR_ax.Title = title(EDGAR_ax,...
                       {'Global Fossil Fuels Emission CO_{2}';...
                       'Source Data: EDGAR Time Series:1970-2016'});
legend(EDGAR_ax,...
       'global total emission',...
       'Location','best',...
       'Orientation','horizontal')
EDGAR_ax.Box = 'off';
hold off