clear, clc;

%% 读取数据
% data是表结构
data = readtable('data.xlsx', 'PreserveVariableNames', true);

% 读取data.xlsx表格中数据，需包含所有年份的所有指标。
% 「修改」：“3:11”为第3列至11列数据
data1 = data(:, 3:11);

% 转成矩阵，以便使用max等函数
data1 = table2array(data1);

%% 标准化
max_indicator = max(data1);  % 每个指标的最大值
min_indicator = min(data1);  % 每个指标的最小值
diff_indicator = max_indicator - min_indicator;  % 差值

% 正负指标矩阵。0表示正指标归一化；1表示负指标归一化
% 「修改」：根据实际数据的正负指标情况，对向量 Indicator_Type 做出修改
Indicator_Type = [0 0 1 0 1 0 0 0 0];  

[data1_line, data1_column] = size(data1);

for i = 1:data1_line
    for j = 1:data1_column
        if Indicator_Type(j) == 0
            % 正指标归一化
            normal_result(i,j) = (data1(i,j) - min_indicator(j)) / diff_indicator(j);
        else
            % 负指标归一化
            normal_result(i,j) = (max_indicator(j) - data1(i,j)) / diff_indicator(j);
        end
    end
end  

% 非负平移，对所有数值都加上0.001
normal_result = normal_result + 0.001

%% 计算权重
sum_column = sum(normal_result);
for j = 1:data1_column
    for i = 1:data1_line
        Y(i, j) = normal_result(i, j)/sum_column(j)
    end
end


%% 计算熵值
year_num = 6;  % 「修改」年份数
city_num = 16;  % 「修改」城市数
k = 1 / log(year_num*city_num);

for j = 1:data1_column
    for i = 1:data1_line
        temp_a(i, j) = Y(i, j).*log(Y(i, j));
    end
end

S = -k * sum(temp_a)

%% 计算第j项指标的差异系数
E = ones(1, data1_column) - S;

%% 计算第j项指标权重
sum_E = sum(E);
for j = 1:data1_column
    W(j) = E(j)/sum_E;
end

%% 计算综合得分
H = W*normal_result';