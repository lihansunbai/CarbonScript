import arcpy
import os
import re

from arcpy import env
env.workplace = 'E:\\workplace\\CarbonProject\\temp'

re_shp = re.compile(r'shp')
files_path = 'E:\\workplace\\CarbonProject\\shapefiles'
raster_path = 'E:\\workplace\\CarbonProject\\raster'
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
    filename = raster_path + '\\' + shapefile[:-3] + 'tif'
    arcpy.PointToRaster_conversion(in_features, value_field, filename, cell_assignment, "", 1)
