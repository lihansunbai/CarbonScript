# -*- coding: utf-8 -*-

import os
import re
import arcpy
from arcpy import env
# from arcpy.sa import *

# 设置arcpy临时工作空间
arcpy.env.workspace = 'E:\\workplace\\CarbonProject\\geodatabase\\EDGAR.gdb'
point_path = arcpy.env.workspace

# # 检查arcgis空间分析扩展许可
# arcpy.CheckOutExtension('Spatial')

# Fields waiting to delete
fields_delete = [
    'Join_Count',
    'TARGET_FID',
    'Join_Count_1',
    'TARGET_FID_1',
    'Join_Count_12',
    'TARGET_FID_12',
    'Join_Count_12_13',
    'TARGET_FID_12_13',
    'Join_Count_12_13_14',
    'TARGET_FID_12_13_14',
    'Join_Count_12_13_14_15',
    'TARGET_FID_12_13_14_15',
    'Join_Count_12_13_14_15_16',
    'TARGET_FID_12_13_14_15_16',
    'Join_Count_12_13_14_15_16_17',
    'TARGET_FID_12_13_14_15_16_17',
    'Join_Count_12_13_14_15_16_17_18',
    'TARGET_FID_12_13_14_15_16_17_18',
    'Join_Count_12_13_14_15_16_17_18_19',
    'TARGET_FID_12_13_14_15_16_17_18_19',
    'Join_Count_12_13_14_15_16_17_18_19_20',
    'TARGET_FID_12_13_14_15_16_17_18_19_20',
    'Join_Count_12_13_14_15_16_17_18_19_20_21',
    'TARGET_FID_12_13_14_15_16_17_18_19_20_21']

points = arcpy.ListFeatureClasses("categories*", "Point")

for point in points:
    point_feature = point_path + '\\' + point

    for field in fields_delete:
        # check filed exist
        if not arcpy.ListFields(point_feature, field):
            print 'Check field exsit failed: %s in %s' % (field, point)
            continue

        try:
            arcpy.DeleteField_management(point_feature, field)
        except:
            print arcpy.GetMessages()
            print 'Delete field failed: %s in %s' % (field, point)

        print 'Delete field success: %s in %s' % (field, point)
