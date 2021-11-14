# -*- coding: utf-8 -*-


# Import system modules
import arcpy
#import arcpy.sa
from arcpy.sa import *


arcpy.CheckOutExtension('Spatial')

# Set environment settings
arcpy.env.workspace = "E:\workplace\CarbonProject\geodatabase\carbon_temp.gdb"
#env.workspace = "E:\workplace\CarbonProject\geodatabase\Graduate_thesis_data.gdb"

workspace = arcpy.env.workspace

# ======================================================================
# ======================================================================
# ### 特别注意！！！
# 这个脚本中的所有步骤都需要在栅格操作一次，同时还要在点图层中操作一次方便统计
# 统计数据的导出。
# ======================================================================
# ======================================================================

# ======================================================================
# ======================================================================
# Data preperation
# ======================================================================
# ======================================================================

emission_2016_raster = arcpy.Raster(workspace + "\\CHINA_MAINLAND_FFCO2_2016")
emission_china_raster = SetNull(emission_2016_raster,
                                emission_2016_raster,
                                "VALUE = 0")

emission_china_point = workspace + "\\CHINA_MAINLAND_FFCO2"
arcpy.RasterToPoint_conversion(emission_china_raster,
                               emission_china_point,
                               "VALUE")

# ======================================================================
# ======================================================================
# Select high emission region
# ======================================================================
# ======================================================================

# log10 operation
# on raster
emission_2016_raster_log = Log10(emission_2016_raster)
emission_2016_raster_log.save(workspace + "\\CHINA_MAINLAND_FFCO2_2016_LOG")
# on vector
# Set local variables
HE_inFeatures = emission_china_point
HE_fieldName = "e_log_2016"
HE_field_type = "FLOAT"

# Execute AddField
arcpy.AddField_management(HE_inFeatures, HE_fieldName, HE_field_type)
arcpy.CalculateField_management(HE_inFeatures,
                                HE_fieldName,
                                "math.log10( !grid_code! )",
                                "PYTHON_9.3")

# select higher than 2 times std
HE_out_table = workspace + "\\statistic_table_2016"
HE_statistics_fields = [["grid_code", "SUM"],
                        ["grid_code", "count"],
                        [HE_fieldName, "STD"],
                        [HE_fieldName, "MEAN"]]

arcpy.Statistics_analysis(HE_inFeatures, HE_out_table, HE_statistics_fields)
# 读取表中的数据需要使用游标
# 游标是一个很恶心的东西，要是还有时间我一定不用他
# 下面写游标一定要按照这里的方法来
# 读取元组的名称和统计的内容顺序相同
HE_Cursor = arcpy.da.SearchCursor(HE_out_table, "*")
HE_statistics_temp = HE_Cursor.next()
HE_statistics = {"row": HE_statistics_temp[0],
                 "frequency": HE_statistics_temp[1],
                 "emission": HE_statistics_temp[2],
                 "emission_grid": HE_statistics_temp[3],
                 "emission_log_std": HE_statistics_temp[4],
                 "emission_log_mean": HE_statistics_temp[5]}

HE_threshold = HE_statistics["emission_log_std"] * \
    2 + HE_statistics["emission_log_mean"]

# 处理栅格
HE_out_raster = workspace + "\\CHINA_MAINLAND_FFCO2_2016_HIGH_LOG"
HE_whereclause = "VALUE < %s" % HE_threshold
emission_2016_raster_high_log = SetNull(emission_2016_raster_log,
                                        emission_2016_raster_log,
                                        HE_whereclause)
emission_2016_raster_high_log.save(HE_out_raster)
# 处理点矢量
# Set local variables
HE_inFeatures = emission_china_point
HE_fieldName = "e_log_2016"
HE_field_type = "FLOAT"

# Execute AddField
#arcpy.AddField_management(HE_inFeatures, HE_fieldName, HE_field_type)
