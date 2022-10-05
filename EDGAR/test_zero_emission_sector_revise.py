# -*- coding: utf-8 -*-

# 路径处理模块
# Systerm path proccessing module
import os
import tqdm
from tqdm import tqdm

# Arcpy 相关模块
# Arcpy module
import arcpy
from arcpy import env
from arcpy.sa import *

# 为工作空间进行赋值
# 这里需要为两个参数赋值：第一个参数是系统中arcpy environment workspace 参数，
# 该参数保证了进行arcgis空间运算的“空间分析扩展”检查通过；第二个参数是为了
# 缩短代码中“arcpy.env.workspace”属性的书写长度而设置的代用变量。
arcpy.env.workspace = 'E:\\Documents\\CarbonProject\\geodatabase\\EDGAR_v60_no_ship.gdb'
# 利用栅格计算器进行栅格代数计算时需要先检查是否开启了空间扩展
arcpy.CheckOutExtension('Spatial')
# 将多线程处理设置为100%
#   吐槽：虽然没什么用，cpu利用率最多也只能达到5%
arcpy.env.parallelProcessingFactor = "100%"

# 处理年份岂止
start_year = 1970
end_year = 2018

# 存储要处理的点数据
working_point_feature = []

# 列出要处理的点数据
for yr in range(start_year, end_year+1):
    temp_wild_card = 'sectoral_weights_%s' % yr
    working_point_feature.extend(arcpy.ListFeatureClasses(
        wild_card=temp_wild_card, feature_type='Point'))

# 检查列出点数据是否成狗
if len(working_point_feature) == 0:
    print 'ERROR: no point feature list!'
    exit

# 开始处理
for pt in working_point_feature:
    print 'start revise %s' % pt

    # --first lets make a list of all of the fields in the table
    fields = arcpy.ListFields(pt)
    field_names = [field.name for field in fields]
    # 注意：
    # 根据arcpy文档给出的说明：
    # UpdateCursor 用于建立对从要素类或表返回的记录的读写访问权限。
    # 返回一组迭代列表。 列表中值的顺序与 field_names 参数指定的字段顺序相符。

    # 这里要找到四个需要修改的数据的位置
    index_wmax = field_names.index('wmax')
    index_wmaxid = field_names.index('wmaxid')
    index_wraster = field_names.index('wraster')
    index_sector_counts = field_names.index('sector_counts')

    # 构造游标，开始逐行操作
    with arcpy.da.UpdateCursor(pt, field_names) as cursor:
        for row in tqdm(cursor):
            # 检查部门数量是否为0
            if row[index_sector_counts] == 0:
                # 检查最大部门排放是否为0
                # 二次确认
                if row[index_wmax] == 0:
                    row[index_wraster] = 0
                    row[index_wmaxid] = 'NULL'

            # 更新数据
            cursor.updateRow(row)

    save_raster_categories = 'main_emi_%s' % pt[-4:]
    save_raster_weight = 'main_emi_weight_%s' % pt[-4:]
    # 用wraster列转栅格
    try:
        arcpy.PointToRaster_conversion(pt,
                                       'wraster',
                                       save_raster_categories,
                                       'MOST_FREQUENT',
                                       '#',
                                       '0.1')
        arcpy.PointToRaster_conversion(pt,
                                       'wmax',
                                       save_raster_weight,
                                       'MOST_FREQUENT',
                                       '#',
                                       '0.1')
        print 'Create main emission raster: %s' % pt

    except:
        print 'Create main emission raster field: %s' % pt

        print arcpy.GetMessages()
