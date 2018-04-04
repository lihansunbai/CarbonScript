# -*- coding: utf-8 -*-

import os
import re
import arcpy
from arcpy import env
from arcpy.sa import *

## !!! 注意 !!! 不要更改EDGAR下载数据解压后的文件名，可能导致生成数据的年份出错
# 设置arcpy临时工作空间
env.workplace = 'E:\\workplace\\CarbonProject\\temp'

# 检查arcgis空间分析扩展许可
arcpy.CheckOutExtension("Spatial")

re_shp = re.compile(r'.nc')
files_path = 'E:\\workplace\\CarbonProject\\DATA\\EDGAR_nc'
raster_path = 'E:\\workplace\\CarbonProject\\geodatabase\\EDGAR.gdb'
files = os.listdir(files_path)
ncfiles = []

variable = "emi_co2"
XDimension = "lon"
YDimension = "lat"
bandDimmension = ""
dimensionValues = ""
valueSelectionMethod = ""

for file in files:
    if not os.path.isdir(file):
        if re_shp.search(file):
            ncfiles.append(file)

if not ncfiles:
    exit

for ncfile in ncfiles:
    inNetCDFFile = files_path + '\\' + ncfile
    outRasterLayer = raster_path + '\\' + ncfile[:-3]
    saveRasterLayer = raster_path + '\\edgar_' + ncfile[-15:-11]
    saveRasterLayer_raw = raster_path + '\\edgar_' + ncfile[-15:-11] + '_raw'
    saveRasterLayer_log = raster_path + '\\edgar_' + ncfile[-15:-11] + '_log'
    raster_temp = raster_path + '\\temp'
    # Execute MakeNetCDFRasterLayer
    arcpy.MakeNetCDFRasterLayer_md(inNetCDFFile, variable, XDimension, YDimension,
                                   outRasterLayer, bandDimmension, dimensionValues,
                                   valueSelectionMethod)
    # Execute make raster
    # make raster without nodata value
    arcpy.CopyRaster_management(outRasterLayer, raster_temp)

    # Excute raster calculator.
    # To convert flux unit to ton 0.1 degree2 year-1
    calculate_temp = (Raster(raster_temp) * 3.8815885) * Power(10,14)

    # clean nondata and calculate log function
    setNull_temp = SetNull(calculate_temp, calculate_temp, "VALUE = 0")
    calculate_temp_log = Log10(setNull_temp)

    # save raw, clean nondata and log raster
    calculate_temp.save(saveRasterLayer_raw)
    setNull_temp.save(saveRasterLayer)
    calculate_temp_log.save(saveRasterLayer_log)
    # delete temp file
    arcpy.Delete_management(raster_temp)

    print 'Finish file: ' + ncfile
