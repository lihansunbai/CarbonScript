# -*- coding: utf-8 -*-

import arcpy
from arcpy import env
from arcpy.sa import *
import os
from tqdm import tqdm
from time import sleep

arcpy.env.workspace = 'C:\\Users\\fang\\Documents\\ArcGIS\\Default.gdb'
arcpy.CheckOutExtension('Spatial')

shp = arcpy.env.workspace + '\\code_block_test'

fields = ['s1', 's2', 's3', 's4', 's5', 'max_id', 'max_value','s_counts']

with arcpy.da.UpdateCursor(shp, fields) as cursor:
    for row in cursor:
        temp_max_value = max(row[0:len(row) - 3])
        temp_max_index = row.index(temp_max_value)
        temp_max_id = fields[temp_max_index]
        temp_s_counts = len([i for i in row[0:len(row) - 3] if i != 0 ])
        
        row[-3] = temp_max_id
        row[-2] = temp_max_value
        row[-1] = temp_s_counts
        print 'max value:%s\nmax index:%s\nmax id:%s\nnone zero fields:%s\n' % (temp_max_value,temp_max_index,temp_max_id,temp_s_counts)
        cursor.updateRow(row)
