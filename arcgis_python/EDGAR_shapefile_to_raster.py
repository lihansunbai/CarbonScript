# -*- coding: utf-8 -*-

################################################################################
################################################################################
# 备忘录：
# memorandum:
# 这里为了保险，制作了两类不同的栅格，第一类（1）：仅为点转栅格，注意，
# 这类栅格中没有数据的点数值为空，请谨慎用于栅格计算器操作；
# 第二类（2）:点转栅格后再叠加零值背景值，这类栅格中没有空值，可以
# 可以用于栅格计算器的计算。
##
# 两类栅格的命名方式为第一类（1）：Non-Background-Added (NBA)；第二类（2）
# Background-Added(BA)。
##
################################################################################
################################################################################

import os
import re
import arcpy
from arcpy import env
from arcpy.sa import *

# 设置arcpy临时工作空间
arcpy.CheckOutExtension("Spatial")

# re_shp = re.compile(r'.shp')
files_path = 'D:\\workplace\\DATA\\geodatabase\\EDGAR_v60_point.gdb'
raster_path = 'D:\\workplace\\DATA\\geodatabase\\EDGAR_v60_raster.gdb'
files = os.listdir(files_path)
shapefiles = []

value_field = 'EMI'
cell_assignment = 'MOST_FREQUENT'

# for file in files:
#     if not os.path.isdir(file):
#         if re_shp.search(file):
#             shapefiles.append(file)

arcpy.env.workspace = files_path
shapefiles = arcpy.ListFeatureClasses(feature_type='Point')

if shapefiles==[] :
    print "Empty direction or database! Or error input directions!"
    exit

for shapefile in shapefiles:

    in_features = files_path + '\\' + shapefile

    ############################################################################
    ############################################################################
    # 生成第一类栅格
    try:
        NBA_raster_file = raster_path + '\\NBA_' + shapefile
        arcpy.PointToRaster_conversion(in_features,
                                    value_field,
                                    NBA_raster_file,
                                    cell_assignment,
                                    "", 0.1)

        print 'Finish file: type_1:' + shapefile + '\n'
    except:
        print arcpy.GetMessages()
        print 'Failed excuting file: type_1:' + shapefile

    ############################################################################
    ############################################################################
    # 生成第二类栅格
    # 为了栅格保持数据的完整性，需要把所有数据叠加到一个背景值的栅格图上。这样才能保证
    # 之后的操作不会有数据缺失。
    BA_raster_file = 'BA_' + shapefile
    BA_Mosaic_input = NBA_raster_file + ';' + files_path + '\\background'

    try:
        arcpy.MosaicToNewRaster_management(
            BA_Mosaic_input, raster_path, BA_raster_file,  "#", "64_BIT", "#", "1", "SUM", "FIRST")
        print 'Finish file: type_2:' + shapefile
    except:
        print arcpy.GetMessages()
        print 'Failed excuting file: type_2:' + shapefile + '\n'