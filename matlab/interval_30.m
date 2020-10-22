clear all
clc

load('CDIAC.mat');

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% 功能：
%%      1. 计算1850年起的累积->历史效应（或祖父责任）
%%      2. 计算时间断面30年的累积->气候效应
%% TODO:
%%      1. 计算温度的增速
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% Interval of 30 years
%% 30年间隔
interval = 30;

%% start year
%% 起始年
start_position = 1850;
start_year = start_position - 1750;

%% output data
%% 输出数据
%% saving the size of output matrix
%% 先要获得输出数组的大小，这里根据上下文其实保存全部大小也是可行的......
output_size = size(emission);
output_column = ceil(output_size(2)/interval);
output_size = [output_size(1) output_column*2];

%% historical cumulative emission
%% 历史累积排放
output_filename_historical = 'historical_emission_30.xlsx';
nation_historical_cumulative = zeros(output_size);
global_historical_cumulative = [];

%% climate period emission
%% 30年气候期内的累积排放
output_filename_climate = 'historical_clmate_30.xlsx';
nation_climate_cumulative = zeros(output_size);
global_climate_cumulative = [];

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% MAIN APPROACH
for i = [1850:interval:2010 2010]
    %% 循环变量
    if i == 1850
        ii = 1;
    end


    %% actual calculatung year
    %% 实际计算年份
    calculate_year = i - 1750;
    
    %% historical cumulative emission
    %% 计算历史累积排放
    temp_nation_historical_cumulative = sum(emission(:,start_year:calculate_year),2);
    temp_global_historical_cumulative = sum(sum(emission(:,start_year:calculate_year)));
    temp_sort_historical = sortrows([nation temp_nation_historical_cumulative],2,'descend');
    
    %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
    %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
    %% 将数据转化为80%贡献国和其他国家排放的形式
    %% ############### 悲报 ####################################
    %% 累积80%贡献排放不能用prctile百分位函数算！
    %% #########################################################
    
    %% 计算总量的80%
    temp_historical_threshold_80 = temp_global_historical_cumulative * 0.8;
    %% 不断累积各个国家的排放直到贡献到达全球排放的80%
    for ci = [1:size(temp_sort_historical,1)]
        ttemp_historical_emission_sum = sum(temp_sort_historical(1:ci,2));
        if (ttemp_historical_emission_sum > temp_historical_threshold_80)
            %% 记住双保险！！！不仅仅是现在累积超过80%。
            %% 它的前一个累积还需要小于80%！！！
            if (ttemp_historical_emission_sum - temp_sort_historical(ci,2)) < temp_historical_threshold_80
                temp_historical_prctile_80 = ci;
                break;
            end
        end
    end
    
    %% reorgnize matrix to majorities and others
    %% 重新整理数组为主要排放国（80%贡献者）和其他排放国
    temp_historical_majority = temp_sort_historical(1:ci,:);
    temp_historical_trivial_emission = sum(temp_sort_historical(ci+1:end,2));
    
    %% 下面代码里的那个‘-1000’代表的时给国家代码UNCODE一个编码。
    %% 这个‘-1000’是我、自己、这个脚本的作者自己定的，
    %% 在原始数据中它没有任何实际意义，
    %% 它的唯一意义是标注其他国家。
    temp_reorgnize_historical = [temp_historical_majority; -1000 temp_historical_trivial_emission];
    
    %% saving output data
    %% 保存输出数据
    %% 我们渴望获得的输出数据结构为下表所示：
    %%
    % +─────────+───────────+─────────+───────────+─────────+───────────+
    % |       1850          |       1880          |       1910          |
    % +─────────+───────────+─────────+───────────+─────────+───────────+
    % | UNCODE  | EMISSION  | UNCODE  | EMISISON  | UNCODE  | EMISSION  |
    % +─────────+───────────+─────────+───────────+─────────+───────────+
    % | 111     | 222       | 333     | 444       | 555     | 666       |
    % +─────────+───────────+─────────+───────────+─────────+───────────+
    % | 777     | 888       | 999     | 101       | 1111    | 121       |
    % +─────────+───────────+─────────+───────────+─────────+───────────+
    % |         ...         |        ...          |         ...         |
    % +─────────+───────────+─────────+───────────+─────────+───────────+
    %% 
    %% 根据表格的样子，其实我们需要得到的最重要的指针是每个数据年份UNCODE列
    %% 所在的位置。
    %% 通过这个列的位置坐标，得到：1、行范围，（当年数据的行值）,
    %% 2、列范围（坐标 + 1）。
    temp_historical_output_row = size(temp_reorgnize_historical);
    temp_historical_save_pointer = (ii - 1)*2 + 1;
    nation_historical_cumulative(1 : temp_historical_output_row(1), temp_historical_save_pointer : temp_historical_save_pointer + 1) = temp_reorgnize_historical;
    
    global_historical_cumulative = [global_historical_cumulative; temp_global_historical_cumulative];
    %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
    %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
    
    %% climate period emission
    %% 计算30年气候期内的累积排放
    temp_nation_climate_cumulative = sum(emission(:,(calculate_year - 29):calculate_year),2);
    temp_global_climate_cumulative = sum(sum(emission(:,(calculate_year - 29):calculate_year),2));
    temp_sort_climate = sortrows([nation temp_nation_climate_cumulative],2,'descend');

    %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
    %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
    %% 将数据转化为80%贡献国和其他国家排放的形式
    %% ############### 悲报 ####################################
    %% 累积80%贡献排放不能用prctile百分位函数算！
    %% #########################################################
    
    %% 计算总量的80%
    temp_climate_threshold_80 = temp_global_climate_cumulative * 0.8;
    %% 不断累积各个国家的排放直到贡献到达全球排放的80%
    for ci = [1:size(temp_sort_climate,1)]
        ttemp_climate_emission_sum = sum(temp_sort_climate(1:ci,2));
        if (ttemp_climate_emission_sum > temp_climate_threshold_80)
            %% 记住双保险！！！不仅仅是现在累积超过80%。
            %% 它的前一个累积还需要小于80%！！！
            if (ttemp_climate_emission_sum - temp_sort_climate(ci,2)) < temp_climate_threshold_80
                temp_climate_prctile_80 = ci;
                break;
            end
        end
    end
    
    %% reorgnize matrix to majorities and others
    %% 重新整理数组为主要排放国（80%贡献者）和其他排放国
    temp_climate_majority = temp_sort_climate(1:ci,:);
    temp_climate_trivial_emission = sum(temp_sort_climate(ci+1:end,2));
    
    %% 下面代码里的那个‘-1000’代表的时给国家代码UNCODE一个编码。
    %% 这个‘-1000’是我、自己、这个脚本的作者自己定的，
    %% 在原始数据中它没有任何实际意义，
    %% 它的唯一意义是标注其他国家。
    temp_reorgnize_climate = [temp_climate_majority; -1000 temp_climate_trivial_emission];
    
    %% saving output data
    %% 保存输出数据
    temp_climate_output_row = size(temp_reorgnize_climate);
    temp_climate_save_pointer = (ii - 1)*2 + 1;
    nation_climate_cumulative(1 : temp_climate_output_row(1), temp_climate_save_pointer : temp_climate_save_pointer + 1) = temp_reorgnize_climate;
    
    global_climate_cumulative = [global_climate_cumulative; temp_global_climate_cumulative];
    %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
    %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

    %% 循环变量自增1
    ii = ii + 1;
end


%% export data to EXCEL files
%% 导出数据到文件
nation_historical_cumulative(nation_historical_cumulative == 0) = missing;
global_historical_cumulative(global_historical_cumulative == 0) = missing;
writematrix(nation_historical_cumulative,output_filename_historical,'Sheet',1)
writematrix(global_historical_cumulative,output_filename_historical,'Sheet',2)

nation_climate_cumulative(nation_climate_cumulative == 0) = missing;
global_climate_cumulative(global_climate_cumulative == 0) = missing;
writematrix(nation_climate_cumulative,output_filename_climate,'Sheet',1)
writematrix(global_climate_cumulative,output_filename_climate,'Sheet',2)





% %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% %% 将数据转化为80%贡献国和其他国家排放的形式
% %% ############### 悲报 ####################################
% %% 累积80%贡献排放不能用prctile百分位函数算！
% %% #########################################################
% %% 计算总量的80%
% temp_historical_threshold_80 = temp_global_historical_cumulative * 0.8;
% %% 不断累积各个国家的排放直到贡献到达全球排放的80%
% for ci = [1:size(temp_sort_historical,1)]
%     ttemp_historical_emission_sum = sum(temp_sort_historical(1:ci,2));
%     if (ttemp_historical_emission_sum > temp_historical_threshold_80)
%         %% 记住双保险！！！不仅仅是现在累积超过80%。
%         %% 它的前一个累积还需要小于80%！！！
%         if (ttemp_historical_emission_sum - temp_sort_historical(ci,2)) < temp_historical_threshold_80
%             temp_historical_prctile_80 = ci;
%             break;
%         end
%     end
% end
% %% reorgnize matrix to majorities and others
% %% 重新整理数组为主要排放国（80%贡献者）和其他排放国
% temp_historical_majority = temp_sort_historical(1:ci,:);
% temp_historical_trivial_emission = sum(temp_sort_historical(ci+1:end,2));
% %% 下面代码里的那个‘-1000’代表的时给国家代码UNCODE一个编码。
% %% 这个‘-1000’是我、自己、这个脚本的作者自己定的，
% %% 在原始数据中它没有任何实际意义，
% %% 它的唯一意义是标注其他国家。
% temp_reorgnize_historical = [temp_historical_majority; -1000 temp_historical_trivial_emission];
% %% saving output data
% %% 保存输出数据
% %% 我们渴望获得的输出数据结构为下表所示：
% %%
% % +─────────+───────────+─────────+───────────+─────────+───────────+
% % |       1850          |       1880          |       1910          |
% % +─────────+───────────+─────────+───────────+─────────+───────────+
% % | UNCODE  | EMISSION  | UNCODE  | EMISISON  | UNCODE  | EMISSION  |
% % +─────────+───────────+─────────+───────────+─────────+───────────+
% % | 111     | 222       | 333     | 444       | 555     | 666       |
% % +─────────+───────────+─────────+───────────+─────────+───────────+
% % | 777     | 888       | 999     | 101       | 1111    | 121       |
% % +─────────+───────────+─────────+───────────+─────────+───────────+
% % |         ...         |        ...          |         ...         |
% % +─────────+───────────+─────────+───────────+─────────+───────────+
% %% 
% %% 根据表格的样子，其实我们需要得到的最重要的指针是每个数据年份UNCODE列
% %% 所在的位置。
% %% 通过这个列的位置坐标，得到：1、行范围，（当年数据的行值)
% %% 2、列范围（坐标 + 1）。
% temp_historical_output_row = size(temp_reorgnize_historical);
% temp_historical_save_pointer = (i - 1)*2 + 1;
% nation_historical_cumulative(1 : temp_historical_output_row(1), temp_historical_save_pointer : temp_historical_save_pointer + 1) = temp_reorgnize_historical;
% global_historical_cumulative = [global_historical_cumulative; temp_global_historical_cumulative];
% %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%