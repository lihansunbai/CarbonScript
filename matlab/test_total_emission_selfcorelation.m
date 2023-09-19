% %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% %% 该脚本绘制EDGAR 碳排放总量场的自相关
% %% 技术路线：
% %%
% %% ****注意：
% %%      可以采用matlab 中的Climate Data Toolbox for MATLAB 工具包，其中提供了detrend3 函数能方便的达到此效果。
% %%      这个脚本循环次数过多，计算时间过久需要改进
% %%      该脚本写法采用Matlab 2014b以后版本推荐的引用对象写法。该版本之前的Matlab将无法运行此脚本。
% %%      若需在较低版本Matlab运行，请重写各个子图的标注部分为set()函数格式。
% %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
close all
clc


% 读入排放数据，可以是去趋势也可以没有趋势
temp_emission = emission_matrix_detrend;

% 采用修改矩阵维度的方式来减少循环次数
emission_matrix_size = size(temp_emission);
temp_reshape_matrix = reshape(temp_emission,[emission_matrix_size(1)*emission_matrix_size(2),emission_matrix_size(3)]);

% 预先分配内存提高运行速度
correlation_r = nan(emission_matrix_size(3)*2-1,emission_matrix_size(1)*emission_matrix_size(2));

% 逐记录（相当于栅格点）计算自相关
for i =[1:emission_matrix_size(1)*emission_matrix_size(2)]
    temp_emission_corr =  temp_reshape_matrix(i,:);
    [correlation_r(:,i), correlation_lags] = xcorr(temp_emission_corr,'normalized' );
end


% 将r和lags结果还原为全球范围的矩阵
% tips: 可能到这一步的时候内存会不够，但是好消息，结果都在correlation_r里面，删掉几个工作区的变量就能缓解
correlation_r = reshape(correlation_r',emission_matrix_size(1), emission_matrix_size(2),[]);

% %% matrix 结果转geotiff
% %% 设置空间参考
% temp_georefference = georasterref('RasterSize',size(emission_variance),'LatitudeLimits',[-90 90],'LongitudeLimits',[-180 180]);
% %% 输出文件名
% tiffile = 'emission_rates_70.tif' ;
% geotiffwrite(tiffile,flip(emission_rates,1),temp_georefference);
% %% Read geotiff file
% %%[A, R] = geotiffread(tiffile);