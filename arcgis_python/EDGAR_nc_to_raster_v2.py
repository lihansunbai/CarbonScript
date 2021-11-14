# -*- coding: utf-8 -*-

import os
import re
import arcpy
from arcpy import env
from arcpy.sa import *

## !!! 注意 !!! 运行此脚本前，请先运行不同种类的转换
## !!! 注意 !!! 不要更改EDGAR下载数据解压后的文件名，可能导致生成数据的年份出错
# 设置arcpy临时工作空间
arcpy.env.workspace = 'E:\\workplace\\CarbonProject\\geodatabase\\carbon_temp.gdb'

# 检查arcgis空间分析扩展许可
arcpy.CheckOutExtension('Spatial')

re_shp = re.compile(r'.nc')
# files_path = 'E:\\workplace\\CarbonProject\\DATA\\temp\\v432_CO2_excl_short-cycle_org_C_TOTALS_nc'
raster_path = arcpy.env.workspace

variable = 'emi_co2'
XDimension = 'lon'
YDimension = 'lat'
bandDimmension = ''
dimensionValues = ''
valueSelectionMethod = ''

yr = 1970
while yr <= 2012:
    # 找到所有部分 
    E1A1A_temp = arcpy.env.workspace + '\\E1A1A_' + str(yr)
    E1A1B_temp = arcpy.env.workspace + '\\E1A1B_' + str(yr)
    E1A2_temp = arcpy.env.workspace + '\\E1A2_' + str(yr)
    E1A3B_temp = arcpy.env.workspace + '\\E1A3B_' + str(yr)
    E1A3E_temp = arcpy.env.workspace + '\\E1A3E_' + str(yr)
    E1A4_temp = arcpy.env.workspace + '\\E1A4_' + str(yr)
    E1B1A_temp = arcpy.env.workspace + '\\E1B1A_' + str(yr)
    E2A_temp = arcpy.env.workspace + '\\E2A_' + str(yr)
    E2B_temp = arcpy.env.workspace + '\\E2B_' + str(yr)
    E2C1A_temp = arcpy.env.workspace + '\\E2C1A_' + str(yr)
    E2C3_temp = arcpy.env.workspace + '\\E2C3_' + str(yr)
    E2G_temp = arcpy.env.workspace + '\\E2G_' + str(yr)
    E3_temp = arcpy.env.workspace + '\\E3_' + str(yr)

    # 构造保存路径
    # outRasterLayer = raster_path + '\\' + ncfile[:-3]
    saveRasterLayer = raster_path + '\\edgar_' + str(yr)
    saveRasterLayer_raw = raster_path + '\\edgar_' + str(yr) + '_raw'
    saveRasterLayer_log = raster_path + '\\edgar_' + str(yr) + '_log'
    saveRasterLayer_sum = raster_path + '\\edgar_' + str(yr) + '_sum'

    # Excute raster calculator.
    calculate_sum = Raster(E1A1A_temp) + Raster(E1A1B_temp)  + Raster(E1A2_temp)+ Raster(E1A3B_temp) + Raster(E1A3E_temp) + Raster(E1A4_temp) + Raster(E1B1A_temp) + Raster(E2A_temp) + Raster(E2B_temp)  + Raster(E2C1A_temp)+ Raster(E2C3_temp) + Raster(E2G_temp) + Raster(E3_temp)

    # To convert flux unit to ton * (0.1 degree)-2 * year-1
    calculate_temp = calculate_sum * 2.3486 * Power(10, 12)

    # clean nondata and calculate log function
    setNull_temp = SetNull(calculate_temp, calculate_temp, 'VALUE = 0')
    calculate_temp_log = Log10(setNull_temp)

    # save raw, clean nondata and log raster
    calculate_sum.save(saveRasterLayer_sum)
    calculate_temp.save(saveRasterLayer_raw)
    setNull_temp.save(saveRasterLayer)
    calculate_temp_log.save(saveRasterLayer_log)
    
    print 'Finish file: ' + 'edgar_' + str(yr)
    yr += 1
    
