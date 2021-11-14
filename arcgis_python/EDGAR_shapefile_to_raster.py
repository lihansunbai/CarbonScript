# -*- coding: utf-8 -*-

import os
import re
import arcpy
from arcpy import env
from arcpy.sa import *

# 设置arcpy临时工作空间
arcpy.CheckOutExtension("Spatial")

re_shp = re.compile(r'.shp')
files_path = 'E:\\workplace\\CarbonProject\\DATA\\EDGAR_shapefile'
raster_path = 'e:\\workplace\\CarbonProject\\geodatabase\\EDGAR.gdb'
files = os.listdir(files_path)
shapefiles = []

value_field = 'EMI'
cell_assignment = 'MOST_FREQUENT'

for file in files:
    if not os.path.isdir(file):
        if re_shp.search(file):
            shapefiles.append(file)

if not shapefiles:
    exit

# 为了栅格保持数据的完整性，需要把所有数据叠加到一个背景值的栅格图上。这样才能保证
# 之后的操作不会有数据缺失。
for shapefile in shapefiles:
    in_features = files_path + '\\' + shapefile
    raster_file = raster_path + '\\' + shapefile[6:-4]
    arcpy.PointToRaster_conversion(in_features,
                                   value_field,
                                   raster_file,
                                   cell_assignment,
                                   "", 0.1)
    raster_file + raster_blank = new raster
    save new raster

    print 'Finish file: ' + shapefile
