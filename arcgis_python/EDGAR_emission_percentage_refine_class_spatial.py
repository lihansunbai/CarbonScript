# -*- coding: utf-8 -*-

import os
import re
import arcpy
from arcpy import env
from arcpy.sa import *


# ======================================================================
# ======================================================================
# Memorandum:
# 备忘录：
#   1. 考虑是否需要在构造函数中包含 arcpy 的几个环境变量的引入；
#   2. 构造函数中需要初始化那些内容？例如：
#       a）传入年份
#       b）是否需要传入排放种类的字典
#       c) 是否需要在这里引入tqdm组建来展示处理进度？或者是在外层控制调用脚本显示
#           处理进？
# ======================================================================
# ======================================================================

# ======================================================================
# ======================================================================
# SPATIAL OPERATIONS CLASS
# ======================================================================
# ======================================================================
# 计算不同类型排放所占权重
class EDGAR_spatial:
    def __init__():
        arcpy.workspace.env


    # EDGAR sector dicts
    # 我觉得自己也可以传入一个新的字典，通过自己穿入的字典就可以统计
    #   感兴趣项目的情况，而不是每次都对所有部门排放进行总计算。
    default_EDGAR_sector = {'ENE': 'ENE',
            'REF_TRF': 'REF_TRF',
            'IND': 'IND',
            'TNR_Aviation_CDS': 'TNR_Aviation_CDS',
            'TNR_Aviation_CRS': 'TNR_Aviation_CRS',
            'TNR_Aviation_LTO': 'TNR_Aviation_LTO',
            'TRO_noRES': 'TRO_noRES',
            'TNR_Other': 'TNR_Other',
            'TNR_Ship': 'TNR_Ship',
            'RCO': 'RCO',
            'PRO': 'PRO',
            'NMM': 'NMM',
            'CHE': 'CHE',
            'IRO': 'IRO',
            'NFE': 'NFE',
            'NEU': 'NEU',
            'PRU_SOL': 'PRU_SOL',
            'AGS': 'AGS',
            'SWD_INC': 'SWD_INC',
            'FFF': 'FFF'}    
    
    default_sector_colormap = {'ENE': 1,
                     'REF_TRF': 2,
                     'IND': 3,
                     'TNR_Aviation_CDS': 4,
                     'TNR_Aviation_CRS': 5,
                     'TNR_Aviation_LTO': 6,
                     'TRO_noRES': 7,
                     'TNR_Other': 8,
                     'TNR_Ship': 9,
                     'RCO': 10,
                     'PRO': 11,
                     'NMM': 12,
                     'CHE': 13,
                     'IRO': 14,
                     'NFE': 15,
                     'NEU': 16,
                     'PRU_SOL': 17,
                     'AGS': 18,
                     'SWD_INC': 19,
                     'FFF': 20}


def weight_calculate(year, emi_type_dic, emi_weight_raster_dic, emi_weight_point_dic):
    for i in emi_type_dic:
        outPoint = '%s_weight_point_%s' % (i, year)
        emi_weight_raster_dic[i] = emi_type_dic[i] / calculate_sum

        try:
            # transform to point features
            emi_weight_point_dic[i] = workspace + '\\' + outPoint
            arcpy.RasterToPoint_conversion(
                emi_weight_raster_dic[i], outPoint, 'VALUE')

            # rename value field
            arcpy.AddField_management(emi_weight_point_dic[i], i, 'DOUBLE')
            arcpy.CalculateField_management(
                emi_weight_point_dic[i], i, '!grid_code!', 'PYTHON_9.3')
            arcpy.DeleteField_management(emi_weight_point_dic[i], 'pointid')
            arcpy.DeleteField_management(emi_weight_point_dic[i], 'grid_code')
            print 'Categories finished: %s' % i
        except:
            print 'Failed categories to point : %s' % i
            print arcpy.GetMessages()

    print 'Categories to point finished of %s' % year


# 整合所有权重到同一个点数据集中
def weight_joint(year, emi_weight_point_dic):
    # 理解这个函数中的操作需要将temp_pointer_a, temp_pointer_b视为指针一样的东西
    # 通过不停的改变他们指向的对象，来完成空间链接的操作。
    # C/C++万岁！！！指针天下第一！！！

    # 构造复制整个字典到一个操作字典中
    temp_emi_weight = emi_weight_point_dic.copy()
    save_shp = workspace + '\\categories_%s' % year
    iter_counter = 1

    # 构造三个特殊变量来完成操作和循环的大和谐~、
    # 因为SpatialJoin函数需要一个输出表，同时又不能覆盖替换另一个表
    # 所以需要用前两个表生成第一个循环用的表
    # 在程序的结尾用最后（其实可以是任意一个表）来完成年份的输出
    temp_first = temp_emi_weight.pop('FFF')
    temp_second = temp_emi_weight.pop('SWD_INC')
    temp_final = temp_emi_weight.pop('ENE')

    # 连接第一个表
    try:
        temp_pointer_a = workspace + '\\iter_%s_%s' % (year,iter_counter)
        arcpy.SpatialJoin_analysis(temp_first,
                                   temp_second,
                                   temp_pointer_a,
                                   'JOIN_ONE_TO_ONE', 'KEEP_ALL')
        iter_counter += 1
        print 'Subjoint finished: %s FFF with SWD_INC' % year
    except:
        print 'Spatia join failed: %s and %s' % (temp_first, temp_second)
        print arcpy.GetMessages()

    # loop begain
    for i in temp_emi_weight:
        temp_pointer_b = workspace + '\\iter_%s_%s' % (year,iter_counter)
        try:
            arcpy.SpatialJoin_analysis(temp_pointer_a,
                                       temp_emi_weight[i],
                                       temp_pointer_b,
                                       'JOIN_ONE_TO_ONE', 'KEEP_ALL')
            temp_pointer_a = temp_pointer_b
            iter_counter += 1
            print 'Subjoint finished: %s with %s' % (year,i)
        except:
            print 'Spatia join failed: %s' % temp_emi_weight[i]
            print arcpy.GetMessages()
        
    # loop ends

    # 保存最后的数据
    try:
        arcpy.SpatialJoin_analysis(temp_pointer_a,
                                   temp_final,
                                   save_shp,
                                   'JOIN_ONE_TO_ONE', 'KEEP_ALL')
    except:
        print 'Spatia join failed: %s' % temp_final
        print arcpy.GetMessages()

    print 'Finished categories to point features: %s' % save_shp


# 导出不同年份最大权重栅格
def weight_raster(year):
    temp_point = workspace + '\\categories_%s' % year
    save_raster_categories = workspace + '\\main_emi_%s' % year
    save_raster_weight = workspace + '\\main_emi_weight_%s' % year

    # 向point feature中添加列
    # 1.权重最大值 wmax
    # 2.权重最大值名称 wmaxid
    # 3.将权重最大值名称映射为一个整数，方便输出为栅格 wraster
    # 并计算添加字段的值
    try:
        # wmax
        arcpy.AddField_management(temp_point,
                                  'wmax',
                                  'DOUBLE', '#', '#', '#', '#',
                                  'NULLABLE', '#', '#')

        # wmaxid
        arcpy.AddField_management(temp_point,
                                  'wmaxid',
                                  'TEXT', '#', '#', '#', '#',
                                  'NULLABLE', '#', '#')

        # wraster
        arcpy.AddField_management(temp_point,
                                  'wraster',
                                  'SHORT', '#', '#', '#', '#',
                                  'NULLABLE', '#', '#')
    except:
        print 'Add field to point faild: %s' % temp_point
        print arcpy.GetMessages()
        return

    # calculatefield这个函数居然会raise exception！
    # 只能把它拿出来写了
    arcpy.CalculateField_management(temp_point,
                                    'wmax',
                                    categories_str_max(),
                                    'PYTHON_9.3')
    print 'Field calculate finished: %s in wmax' % year
    arcpy.CalculateField_management(temp_point,
                                    'wmaxid',
                                    'maxid(weight, ENE, REF_TRF, IND, TNR_Aviation_CDS, TNR_Aviation_CRS, TNR_Aviation_LTO, TRO_noRES, TNR_Other, TNR_Ship, RCO, PRO, NMM, CHE, IRO, NFE, NEU, PRU_SOL, AGS, SWD_INC, FFF)',
                                    'PYTHON_9.3',
                                    categories_codeblock_maxid)
    print 'Field calculate finished: %s in wmaxid' % year
    arcpy.CalculateField_management(temp_point,
                                    'wraster',
                                    'raster(!wmaxid!)',
                                    'PYTHON_9.3',
                                    categories_codeblock_raster)
    print 'Field calculate finished: %s in wraster' % year
    print 'Add and calculate fields finished: %s' % temp_point
    # 用wraster列转栅格
    try:
        arcpy.PointToRaster_conversion(temp_point,
                                       'wraster',
                                       save_raster_categories,
                                       'MOST_FREQUENT',
                                       '#',
                                       '0.1')
        arcpy.PointToRaster_conversion(temp_point,
                                       'wmax',
                                       save_raster_weight,
                                       'MOST_FREQUENT',
                                       '#',
                                       '0.1')
        print 'Create main emission raster: %s' % temp_point
    except:
        print 'Create main emission raster field: %s' % temp_point
        print arcpy.GetMessages()


def categories_str_max():
    str_re = ''
    for i in emi_cate:
        str_re += '!%s!,' % i

    str_re = 'max([%s])' % str_re[:-1]

    return str_re

def start_year(year):
    return """==============================
    ==============================
    Processing start of year %s
    ==============================
    ==============================""" % year

def finish_year(year):
    return """==============================
    ==============================
    Congratulations!
    Finished processing data of year %s
    ==============================
    ==============================""" % year

if __name__ == '__main__':
    exit