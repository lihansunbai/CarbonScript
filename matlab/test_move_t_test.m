%% 读入排放量距平
%% 需要读入其他数据的时候修改此处
emission_anomoly = emission_detrend;

%%    滑动T检验
step = 5; % length of subsequence
% v = step+step-2;  % degreee of freedom
% ttest = 2.878; % sinnificant level  alpha=0.01;
len1 = step;
len2 = step;
length_1 = length(emission_anomoly);

% plot x-axis
x = [step:length_1 - step];

% 自由度计算/修正
v = step+step-2;  % degreee of freedom
ttest = 3.355; % sinnificant level  alpha=0.01;

% 这个方法计算的t-test从滑动检验的时间段开始，并不是从数据开头进行计算
for i = step:length_1 - step
    n1 = emission_anomoly(i-step+1:i);
    n2 = emission_anomoly(i+1:i+step);
    mean1 = mean(n1);
    mean2 = mean(n2);
    c = (len1+len2)/(len1*len2);
    var1 = 1/len1*sum((n1 - mean1).^2);% 直接数组求和
    var2 = 1/len2*sum((n2 - mean2).^2);
    delta1 = (len1-1)*var1 + (len2-1)*var2;
    delta = delta1/(len1 + len2 - 2);
    t(i-step+1) = (mean1 - mean2)/sqrt(delta*c);
end

%%  滑动T检验图
plot(x,t,'b-','linewidth',1);
xlabel('t(year)','FontName','TimesNewRoman','FontSize',12,'fontweight','bold');
ylabel('Moving T Test','FontName','TimeNewRoman','FontSize',12,'fontweight','bold');
axis([min(x),max(x),min(t),max(t)]);% y axis limitation
hold on
plot(x,0*ones(i-step+1,1),'-.','linewidth',1);
plot(x,ttest*ones(i-step+1,1),':','linewidth',3);% line of the significant level
plot(x,-ttest*ones(i-step+1,1),':','linewidth',3);
H=legend('t','0.01 significant level');% explain
title('t-test results','fontweight','bold','fontsize',20);