close all
clear all
clc




%% import files from path
import xxxxx


%% main process
data(data < 0 ) = [];
data_log = log10(data);
data_log(isinf(data_log)) = [];

%%% normalize the log data