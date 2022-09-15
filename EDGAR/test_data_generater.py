# -*- coding: utf-8 -*-

import arcpy
from arcpy import env
from arcpy.sa import *
import os
from tqdm import tqdm

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



# class test_depack:
#     d1 = (4,3,2,1)

#     def depack(self, d2, d3):
#         print d2
#         print d3

# test_d = {'a':3,'b':{'d2':3,'d3':2}}

# test_c = test_depack()

# test_c.depack(**test_d['b'])

class test_property:
    inner_s1 = 0
    inner_s2 = 0
    def set_property(self, **kwargus):
        inner_s1 = kwargus['s1']
        inner_s2 = kwargus['s2']
    def get_property(self):
        return (self.inner_s1, self.inner_s2)
    tt_property = property(get_property, set_property)

ttt = test_property()
bbb = {'s1':3,'s2':5}
ttt.tt_property = bbb
ttt.tt_property


## 累加栅格函数测试部分
import arcpy
from arcpy import env
from arcpy.sa import *
import os
import tqdm
from tqdm import tqdm
arcpy.env.workspace = 'C:/Users/lihan/Documents/ArcGIS/Default.gdb'
arcpy.CheckOutExtension('Spatial')

def do_raster_add(raster_list, result_raster):
    # 将列表中的第一个栅格作为累加的起始栅格
    temp_raster = arcpy.Raster(raster_list[0])
    raster_list.pop(0)

    # 累加剩余栅格
    for r in tqdm(raster_list):
        temp_raster = temp_raster + arcpy.Raster(r)
    
    return temp_raster.save(result_raster)

ttt = arcpy.ListRasters()

output = 'C:/Users/lihan/Documents/ArcGIS/Default.gdb/output'

do_raster_add(ttt, output)


## 权重测试函数
import sys
sys.path.append("E:/CODE/CARBON/CarbonScript/EDGAR/") 
from EDGAR_emission_percentage_refine_class_spatial import EDGAR_spatial
test_es = {'E2A':'E2A','E1A1A':'E1A1A','E1A4':'E1A4'}
test_esc = {'E2A':1,'E1A1A':2,'E1A4':3}

aaa = EDGAR_spatial('D:\\workplace\\geodatabase\\EDGAR_test_42.gdb',st_year=2012,en_year=2012,sector=test_es,colormap=test_esc,background_flag=True, background_flag_label='')
aaa.prepare_raster()
print aaa.working_rasters
aaa.year_total_sectors_merge(2012)
aaa.sector_emission_percentage('E2A',2012,'test_e2a_weight')