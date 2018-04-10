# -*- coding: utf-8 -*-

import os
import re
import arcpy
from arcpy import env
from arcpy.sa import *

## !!! 注意 !!! 运行此脚本前，请先运行提取国际航线的四个脚本（ACRS、ACDS、ALTO、ASPS）。
## !!! 注意 !!! 请保证国际航线数据和工作空间在同一GDB数据库内。
## !!! 注意 !!! 不要更改EDGAR下载数据解压后的文件名，可能导致生成数据的年份出错
# 设置arcpy临时工作空间
arcpy.env.workspace = 'E:\\workplace\\CarbonProject\\geodatabase\\carbon_temp.gdb'

# 检查arcgis空间分析扩展许可
arcpy.CheckOutExtension('Spatial')

re_shp = re.compile(r'.nc')
files_path = 'E:\\workplace\\CarbonProject\\DATA\\temp\\v432_CO2_excl_short-cycle_org_C_TOTALS_nc'
raster_path = 'E:\\workplace\\CarbonProject\\geodatabase\\carbon_temp.gdb'
files = os.listdir(files_path)
ncfiles = []

variable = 'emi_co2'
XDimension = 'lon'
YDimension = 'lat'
bandDimmension = ''
dimensionValues = ''
valueSelectionMethod = ''

for file in files:
    if not os.path.isdir(file):
        if re_shp.search(file):
            ncfiles.append(file)

if not ncfiles:
    exit

for ncfile in ncfiles:
    yr_temp = ncfile[-15:-11]
    inNetCDFFile = files_path + '\\' + ncfile

    # 构造国际航线的栅格，国际航线排放排放将被去除
    ACRS_temp = arcpy.env.workspace + '\\ACRS_' + yr_temp
    ACDS_temp = arcpy.env.workspace + '\\ACDS_' + yr_temp
    ALTO_temp = arcpy.env.workspace + '\\ALTO_' + yr_temp
    ASPS_temp = arcpy.env.workspace + '\\ASPS_' + yr_temp

    # 构造保存路径
    outRasterLayer = raster_path + '\\' + ncfile[:-3]
    saveRasterLayer = raster_path + '\\edgar_' + yr_temp
    saveRasterLayer_raw = raster_path + '\\edgar_' + yr_temp + '_raw'
    saveRasterLayer_log = raster_path + '\\edgar_' + yr_temp + '_log'
    raster_temp = raster_path + '\\temp'
    # Execute MakeNetCDFRasterLayer
    arcpy.MakeNetCDFRasterLayer_md(inNetCDFFile, variable, XDimension, YDimension,
                                   outRasterLayer, bandDimmension, dimensionValues,
                                   valueSelectionMethod)
    # Execute make raster
    # make raster without nodata value
    arcpy.CopyRaster_management(outRasterLayer, raster_temp)

    # Excute raster calculator.
    # 除去国际航线排放
    # 2004年以前用此计算
    calculate_exc_av = Raster(raster_temp) - Raster(ACRS_temp) - Raster(ACDS_temp) - Raster(ALTO_temp) - Raster(ASPS_temp)
    # 2004年，和以后，用此计算
    # calculate_exc_av = Raster(raster_temp) - Raster(ACRS_temp) - Raster(ACDS_temp) - Raster(ALTO_temp)

    # To convert flux unit to ton 0.1 degree2 year-1
    calculate_temp = calculate_exc_av * 3.8815885 * Power(10, 14)

    # clean nondata and calculate log function
    setNull_temp = SetNull(calculate_temp, calculate_temp, 'VALUE = 0')
    calculate_temp_log = Log10(setNull_temp)

    # save raw, clean nondata and log raster
    calculate_temp.save(saveRasterLayer_raw)
    setNull_temp.save(saveRasterLayer)
    calculate_temp_log.save(saveRasterLayer_log)
    # delete temp file
    arcpy.Delete_management(raster_temp)

    print 'Finish file: ' + ncfile
