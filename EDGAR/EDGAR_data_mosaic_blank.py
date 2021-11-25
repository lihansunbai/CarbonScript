# -*- coding: utf-8 -*-

import os
import re
import arcpy
from arcpy import env
from arcpy.sa import *

# 设置arcpy临时工作空间
arcpy.env.workspace = 'E:\\workplace\\CarbonProject\\geodatabase\\EDGAR.gdb'

# 检查arcgis空间分析扩展许可
arcpy.CheckOutExtension('Spatial')

add_file = 'E:\\workplace\\CarbonProject\\geodatabase\\carbon.gdb\\background'
save_raster_path = 'E:\\workplace\\CarbonProject\\geodatabase\\temp.gdb'
projection_path = 'E:\\workplace\\CarbonProject\\DATA\\temp\\WGS1984.prj'

rasters = arcpy.ListRasters("*")

for raster in rasters:
    # construct input
    inputRaster = '"%s;%s"' % (raster, add_file)

    try:
        arcpy.MosaicToNewRaster_management(inputRaster, save_raster_path, raster, projection_path,
                                           "64_BIT", "#", "1", "MAXIMUM", "FIRST")
    except:
        print arcpy.GetMessages()
        print 'Mosaic raster field: %s' % raster

    print "Finished: %s" % raster
