# -*- coding: utf-8 -*-

import os
import re
import arcpy
from arcpy import env
from arcpy.sa import *


# ======================================================================
# ======================================================================
# MISCELLANEOUS FUNCTIONS
# ======================================================================
# ======================================================================
# 计算不同类型排放所占权重
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
            print 'Categories to point failed: %s' % i
            print arcpy.GetMessages()

    print 'Categories to point finished: %s' % year


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
    temp_first = temp_emi_weight.pop('E1A1A')
    temp_second = temp_emi_weight.pop('E1A1B')
    temp_final = temp_emi_weight.pop('E3')

    # 连接第一个表
    try:
        temp_pointer_a = workspace + '\\iter_%s_%s' % (year,iter_counter)
        arcpy.SpatialJoin_analysis(temp_first,
                                   temp_second,
                                   temp_pointer_a,
                                   'JOIN_ONE_TO_ONE', 'KEEP_ALL')
        iter_counter += 1
        print 'Subjoint finished: %s E1A1A with E1A1B' % year
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
                                    'maxid(!wmax!,!E1A1A!,!E1A1B!,!E1A2!,!E1A3B!,!E1A3C!,!E1A4!,!E1B1A!,!E2A!,!E2B!,!E2C1A!,!E2C3!,!E2G!,!E3!)',
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


def finish_year(year):
    return """==============================
    ==============================
    Congratulations!
    Finished processing data of year %s
    ==============================
    ==============================""" % year


# ======================================================================
# ======================================================================
# MAIN SCRIPT
# ======================================================================
# ======================================================================
# !!! 注意 !!! 运行此脚本前，请先运行所有子部分排放的提取
# 设置arcpy工作空间
arcpy.env.workspace = 'E:\\workplace\\CarbonProject\\geodatabase\\EDGAR.gdb'
# arcpy.env.workspace实在是太长了，每次输入都老恶心了，重新做一个新的方便使用
workspace = arcpy.env.workspace

# 检查arcgis空间分析扩展许可
arcpy.CheckOutExtension('Spatial')

# set global perfix of emission types paths
emi_cate = {'E1A1A': 'E1A1A',
            'E1A1B': 'E1A1B',
            'E1A2': 'E1A2',
            'E1A3B': 'E1A3B',
            'E1A3C': 'E1A3C',
            'E1A4': 'E1A4',
            'E1B1A': 'E1B1A',
            'E2A': 'E2A',
            'E2B': 'E2B',
            'E2C1A': 'E2C1A',
            'E2C3': 'E2C3',
            'E2G': 'E2G',
            'E3': 'E3'}

# set global colormap of emission categories
emi_cate_colormap = {'E1A1A': 1,
                     'E1A1B': 2,
                     'E1A2': 3,
                     'E1A3B': 4,
                     'E1A3C': 5,
                     'E1A4': 6,
                     'E1B1A': 7,
                     'E2A': 8,
                     'E2B': 9,
                     'E2C1A': 10,
                     'E2C3': 11,
                     'E2G': 12,
                     'E3': 13}

# 下面两个字符串都是用来分类的codeblock，老长了~
categories_codeblock_maxid = """def maxid(weight,E1A1A,E1A1B,E1A2,E1A3B,E1A3C,E1A4,E1B1A,E2A,E2B,E2C1A,E2C3,E2G,E3):
    if weight == E1A1A:
        return 'E1A1A'
    elif weight == E1A1B:
        return 'E1A1B'
    elif weight == E1A2:
        return 'E1A2'
    elif weight == E1A3B:
        return 'E1A3B'
    elif weight == E1A3C:
        return 'E1A3C'
    elif weight == E1A4:
        return 'E1A4'
    elif weight == E1B1A:
        return 'E1B1A'
    elif weight == E2A:
        return 'E2A'
    elif weight == E2B:
        return 'E2B'
    elif weight == E2C1A:
        return 'E2C1A'
    elif weight == E2C3:
        return 'E2C3'
    elif weight == E2G:
        return 'E2G'
    elif weight == E3:
        return 'E3'
    else:
        return ''"""


categories_codeblock_raster = """def raster(id):
    if id == 'E1A1A':
        return emi_cate_colormap[id]
    elif id == 'E1A1B':
        return emi_cate_colormap[id]
    elif id == 'E1A2':
        return emi_cate_colormap[id]
    elif id == 'E1A3B':
        return emi_cate_colormap[id]
    elif id == 'E1A3C':
        return emi_cate_colormap[id]
    elif id == 'E1A4':
        return emi_cate_colormap[id]
    elif id == 'E1B1A':
        return emi_cate_colormap[id]
    elif id == 'E2A':
        return emi_cate_colormap[id]
    elif id == 'E2B':
        return emi_cate_colormap[id]
    elif id == 'E2C3':
        return emi_cate_colormap[id]
    elif id == 'E2C1A':
        return emi_cate_colormap[id]
    elif id == 'E2G':
        return emi_cate_colormap[id]
    elif id == 'E3':
        return emi_cate_colormap[id]
    else:
        return 0"""

# 列出所有文件
raster_path = arcpy.env.workspace

yr = list(range(1970,1981))
for y in yr:
    # define the variables
    calculate_sum = ''
    emi_cate_temp = {}
    emi_weight_raster_temp = {}
    emi_weight_point_temp = {}
    # 找到所有部分
    for i in emi_cate:
        emi_cate_temp[i] = workspace + '\\' + emi_cate[i] + '_' + str(y)

    calculate_sum = Raster(emi_cate_temp['E1A1A']) + Raster(emi_cate_temp['E1A1B']) + Raster(emi_cate_temp['E1A3B']) + Raster(emi_cate_temp['E1A3C']) + Raster(emi_cate_temp['E1A4']) + Raster(emi_cate_temp['E1B1A']) + Raster(emi_cate_temp['E2A']) + Raster(emi_cate_temp['E2B']) + Raster(emi_cate_temp['E2C3']) + Raster(emi_cate_temp['E2G']) + Raster(emi_cate_temp['E3']) + Raster(emi_cate_temp['E1A2']) + Raster(emi_cate_temp['E2C1A'])

    # calculate weights for each parts
    weight_calculate(y, emi_cate_temp,
                     emi_weight_raster_temp,
                     emi_weight_point_temp)
    weight_joint(y, emi_weight_point_temp)
    weight_raster(y)
    finish_year(y)

# MAIN SCRIPT ENDS
