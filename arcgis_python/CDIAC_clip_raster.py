import arcpy
import os
import re


from arcpy import env
env.workplace = 'E:\\workplace\\CarbonProject\\temp'

re_tif = re.compile(r'.tif$')
files_path = 'e:\\workplace\\CarbonProject\\CDICA'
raster_path = 'e:\\workplace\\CarbonProject\\raster'
files = os.listdir(files_path)
rasters = []
envelope_europe = "-30 40 45 70"
envelope_india = "65 15 90 20"
envelope_east_china = "95 15 135 50"
envelope_northen_east_asia = "120 30 150 45"
envelope_america = "-130 22 -60 55"

for file in files:
    if not os.path.isdir(file):
        if re_tif.search(file):
            rasters.append(file)

if not rasters:
    exit

for raster in rasters:
    in_raster = files_path + '\\' + raster

    europe_out_raster = raster_path + '\\' + raster[:-4] + '_europe.tif'
    print("Clipping " + europe_out_raster)
    arcpy.Clip_management(in_raster, envelope_europe, europe_out_raster)

    india_out_raster = raster_path + '\\' + raster[:-4] + '_india.tif'
    print("Clipping " + india_out_raster)
    arcpy.Clip_management(in_raster, envelope_india, india_out_raster)

    east_china_out_raster = raster_path + '\\' + raster[:-4] + '_east_china.tif'
    print("Clipping " + east_china_out_raster)
    arcpy.Clip_management(in_raster, envelope_east_china, east_china_out_raster)

    northen_east_asia_out_raster = raster_path + '\\' + raster[:-4] + '_northen_east_asia.tif'
    print("Clipping " + northen_east_asia_out_raster)
    arcpy.Clip_management(in_raster, envelope_northen_east_asia, northen_east_asia_out_raster)

    america_out_raster = raster_path + '\\' + raster[:-4] + '_america.tif'
    print("Clipping " + america_out_raster)
    arcpy.Clip_management(in_raster, envelope_america, america_out_raster)
    