# -*- coding: utf-8 -*-

import os
import re
import arcpy
from arcpy import env
from arcpy.sa import *

## !!! 注意 !!! 不要更改EDGAR下载数据解压后的文件名，可能导致生成数据的年份出错
# 设置arcpy临时工作空间
arcpy.env.workspace = 'E:\\workplace\\CarbonProject\\geodatabase\\carbon_temp.gdb'

# 检查arcgis空间分析扩展许可
arcpy.CheckOutExtension("Spatial")

re_shp = re.compile(r'.nc')
files_path = 'E:\\workplace\\CarbonProject\\DATA\\temp\\REF_TRF_nc'
raster_path = 'E:\\workplace\\CarbonProject\\geodatabase\\carbon_temp.gdb'
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
    outRasterLayer = raster_path + '\\' + ncfile[:-3] + '_nc'
    save_raster = raster_path + '\\E1A1B_' + ncfile[-64:-60]
    # Execute MakeNetCDFRasterLayer
    arcpy.MakeNetCDFRasterLayer_md(inNetCDFFile, variable, XDimension, YDimension,
                                   outRasterLayer, bandDimmension, dimensionValues,
                                   valueSelectionMethod)
    # Execute make raster
    # make raster without nodata value
    arcpy.CopyRaster_management(outRasterLayer, save_raster)

    # delete temp file
    arcpy.Delete_management(outRasterLayer)

    print 'Finish file: ' + ncfile
