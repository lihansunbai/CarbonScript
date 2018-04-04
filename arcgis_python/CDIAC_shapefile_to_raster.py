# -*- coding: utf-8 -*-

import os
import re
import arcpy
from arcpy import env
from arcpy.sa import *

# 设置arcpy临时工作空间
env.workplace = 'E:\\workplace\\CarbonProject\\temp'
arcpy.CheckOutExtension("Spatial")

re_shp = re.compile(r'.shp')
files_path = 'E:\\workplace\\CarbonProject\\DATA\\CDIAC_shapefiles'
raster_path = 'e:\\workplace\\CarbonProject\\geodatabase\\CDIAC.gdb'
files = os.listdir(files_path)
shapefiles = []

value_field = 'co2_con'
cell_assignment = 'MOST_FREQUENT'

for file in files:
    if not os.path.isdir(file):
        if re_shp.search(file):
            shapefiles.append(file)

if not shapefiles:
    exit

for shapefile in shapefiles:
    in_features = files_path + '\\' + shapefile
    raster_file_raw = raster_path + '\\cdiac' + shapefile[5:-4] + '_raw'
    raster_file = raster_path + '\\cdiac' + shapefile[5:-4]
    raster_file_log = raster_path + '\\cdiac' + shapefile[5:-4] + '_log'
    arcpy.PointToRaster_conversion(in_features,
                                   value_field,
                                   raster_file_raw,
                                   cell_assignment,
                                   "", 1)
    setNull_temp = SetNull(raster_file_raw, raster_file_raw, "VALUE = 0")
    log_temp = Log10(setNull_temp)
    setNull_temp.save(raster_file)
    log_temp.save(raster_file_log)

    print 'Finish file: ' + shapefile
