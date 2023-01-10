
# -*- coding: utf-8 -*-

# 路径处理模块
# Systerm path proccessing module
import os
import sys
import tqdm
from tqdm import tqdm

# Arcpy 相关模块
# Arcpy module
import arcpy
from arcpy import env
from arcpy.sa import *

# 引入自己的ES_spatial模块
sys.path.append('E:\\CODE\\CARBON\\CarbonScript\\EDGAR')
from EDGAR_spatial import EDGAR_spatial

# 为工作空间进行赋值
# 这里需要为两个参数赋值：第一个参数是系统中arcpy environment workspace 参数，
# 该参数保证了进行arcgis空间运算的“空间分析扩展”检查通过；第二个参数是为了
# 缩短代码中“arcpy.env.workspace”属性的书写长度而设置的代用变量。
workspace = 'E:\\Documents\\CarbonProject\\geodatabase\\EDGAR_v60_no_ship.gdb'

# 处理年份岂止
start_year = 1980
end_year = 2017

# 存储要处理的点数据
working_point_feature = []

# 初始化ES_spatial
extract_total = EDGAR_spatial.extract_center(workspace=workspace,st_year=start_year, en_year=end_year,log_path='Extract_total.log')

# 列出要处理的点数据
for yr in range(start_year, end_year+1):
    temp_wild_card = 'sectoral_weights_%s' % yr
    working_point_feature.extend(arcpy.ListFeatureClasses(
        wild_card=temp_wild_card, feature_type='Point'))


# 检查列出点数据是否存在
if len(working_point_feature) == 0:
    print 'ERROR: no point feature list!'
    exit


# 开始处理
for pt in tqdm(working_point_feature):
    temp_year = pt[-4:]
    temp_total_emission = 'total_emission_%s' % temp_year
    temp_outpoint = 'sectoral_weights_with_total_%s' % temp_year

    if not(arcpy.Exists(temp_total_emission)):
        print 'ERROR: total emission dose not exists!'
        extract_total.ES_logger.error('total emission dose not exists.')
        break

    extract_total.do_ETP(ExtractPoint=pt,
                        ValueRaster=temp_total_emission,
                        outPoint=temp_outpoint,
                        NewFieldName=('RASTERVALU','grid_total_emission'))


