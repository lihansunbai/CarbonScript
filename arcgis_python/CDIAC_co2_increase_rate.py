# -*- coding: utf-8 -*-

import os
import re
import arcpy
from arcpy import env
from arcpy.sa import *

# 设置计算增长倍数的间隔年
year_gap = 5

# 设置arcpy临时工作空间
arcpy.env.workspace = 'E:\\workplace\\CarbonProject\\geodatabase\\carbon_temp.gdb' 

# 检查arcgis空间分析扩展许可
arcpy.CheckOutExtension("Spatial")

# 列出数据库中所有已经进行LOG计算操作的栅格数据
rasters = arcpy.ListRasters("*log*")
raster_1990 = arcpy.env.workspace + '\\' + arcpy.ListRasters("*1990_log*")[0]

# 这一步确定最后一个年份数据是否是5或10结尾
# 确定是否保留做进一步处理
rasters.sort()

# 保存栅格数据的最后一年和第一年的栅格数据
first_year = rasters[0]
last_year = []

# 下面这个切片是不是很复杂啊~~~~~
# 数据库里的栅格数据都是以这个格式保存的:
#      cdiac_xxxx_log (xxxx是四位数年份)
# 所以，这个切片就是表示栅格数据列表中的最后一个栅格数据的年份的最后一位。
# 抱歉~我也不想这么写~
if rasters[-1][-8:-4][-1] != '5':
    if rasters[-1][-8:-4][-1] != '0':
        last_year = rasters[-1]

# Main method starts
for raster in rasters:
    # 处理第一年，该年不需要与前一年做增长率比较
    if raster == first_year:
        temp_saveDiff_1990 = 'cdiac_diff_1990_' + raster[-8:-4]
        try:
            raster = arcpy.env.workspace + '\\' + raster
            raster_diff = Raster(raster) - Raster(raster_1990)
            raster_diff.save(temp_saveDiff_1990)
        except:
            print arcpy.GetMessages()
        continue

    # 处理最后一年，该年需要与前一个5或0年进行增长率比较
    if raster == last_year:
        temp_mod = str(int(raster[-8:-4]) - (int(raster[-8:-4]) % 5))
        temp_minus = arcpy.env.workspace + '\\cdiac_' + temp_mod + '_log'
        temp_saveDiff_1990 = 'cdiac_diff_1990_' + raster[-8:-4]
        temp_saveIncrease = 'cdiac_increase_' + temp_mod + '_' + raster[-8:-4]
        temp_saveIncrease_points = 'cdiac_increase_points_' + temp_mod + '_' + raster[-8:-4]
        try:
            # calculate raster
            raster = arcpy.env.workspace + '\\' + raster
            raster_diff = Raster(raster) - Raster(raster_1990)
            raster_increase = Raster(raster) - Raster(temp_minus)
            raster_diff.save(temp_saveDiff_1990)
            raster_increase.save(temp_saveIncrease)

            # converts increase raster dataset to points
            arcpy.RasterToPoint_conversion(temp_saveIncrease, temp_saveIncrease_points)
        except:
            print arcpy.GetMessages()
        continue

    # 处理非特殊年份
    temp_year = raster[-8:-4]

    # 找到当年的前x年
    # 再次抱歉~不该写这么复杂
    temp_year_minus = str(int(temp_year) - year_gap)
    temp_year_increase = arcpy.env.workspace + '\\cdiac_year_' + temp_year_minus + '_log'

    # 设置保存路径
    saveDiff_1990 = 'cdiac_diff_1990_' + temp_year
    saveIncrease = 'cdiac_increase_' + temp_year_minus + '_' + temp_year
    saveIncrease_points = 'cdiac_increase_points_' + temp_year_minus + '_' + temp_year

    try:
        # raster map algebra
        raster = arcpy.env.workspace + '\\' + raster
        raster_diff = Raster(raster) - Raster(raster_1990)
        raster_increase = Raster(raster) - Raster(temp_year_increase)
        raster_diff.save(saveDiff_1990)
        raster_increase.save(saveIncrease)

        # converts increase raster dataset to points
        arcpy.RasterToPoint_conversion(saveIncrease, saveIncrease_points)
    except:
        print arcpy.GetMessages()

# Main method ends
