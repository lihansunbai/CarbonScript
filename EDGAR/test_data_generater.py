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

fields = ['s1', 's2', 's3', 's4', 's5', 'max_id', 'max_value']

with arcpy.da.UpdateCursor(shp, fields) as cursor:
    for row in tqdm(cursor):
        temp_max_value = max(row[0:len(row) - 2])
        temp_max_index = row.index(temp_max_value)
        temp_max_id = fields[temp_max_index]
        
        row[-2] = temp_max_id
        row[-1] = temp_max_value
        cursor.updateRow(row)
        print 'max value:%s\nmax index:%s\nmax id:%s' % (temp_max_value,temp_max_index,temp_max_id)
        sleep(0.25)
