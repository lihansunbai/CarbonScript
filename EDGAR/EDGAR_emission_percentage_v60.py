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
                                    'maxid(!wmax!)',
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


# ======================================================================
# ======================================================================
# MAIN SCRIPT
# ======================================================================
# ======================================================================
# !!! 注意 !!! 运行此脚本前，请先运行所有子部分排放的提取
# 这里输入的数据是已经转换为栅格的各个部门排放
# 设置arcpy工作空间
arcpy.env.workspace = 'D:\\workplace\\DATA\\geodatabase\\EDGAR_v60_raster.gdb'
# arcpy.env.workspace实在是太长了，每次输入都老恶心了，重新做一个新的方便使用
workspace = arcpy.env.workspace

# 检查arcgis空间分析扩展许可
arcpy.CheckOutExtension('Spatial')

# set global perfix of emission types paths
# categories fo EDGAR V60
emi_cate = {'ENE': 'ENE',
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

# set global colormap of emission categories
# categories colormap fo EDGAR V60
emi_cate_colormap = {'ENE': 1,
                     'REF_TRF': 2,
                     'IND': 3,
                     'TNR_Aviation_CDS': 4,
                     'TNR_Aviation_CRS': 5,
                     'TNR_Aviation_LTO': 6,
                     'TRO_noRES': 8,
                     'TNR_Other': 9,
                     'TNR_Ship': 10,
                     'RCO': 11,
                     'PRO': 12,
                     'NMM': 13,
                     'CHE': 14,
                     'IRO': 15,
                     'NFE': 16,
                     'NEU': 17,
                     'PRU_SOL': 18,
                     'AGS': 19,
                     'SWD_INC': 20,
                     'FFF': 21}

# 下面两个字符串都是用来分类的codeblock，老长了~
## For EDGAR V60
categories_codeblock_maxid = """def maxid(weight):
    if weight == ENE:
        return 'ENE'
    elif weight == REF_TRF:
        return 'REF_TRF'
    elif weight == IND:
        return 'IND'
    elif weight == TNR_Aviation_CDS:
        return 'TNR_Aviation_CDS'
    elif weight == TNR_Aviation_CRS:
        return 'TNR_Aviation_CRS'
    elif weight == TNR_Aviation_LTO:
        return 'TNR_Aviation_LTO'
    elif weight == TRO_noRES:
        return 'TRO_noRES'
    elif weight == TNR_Other:
        return 'TNR_Other'
    elif weight == TNR_Ship:
        return 'TNR_Ship'
    elif weight == RCO:
        return 'RCO'
    elif weight == PRO:
        return 'PRO'
    elif weight == NMM:
        return 'NMM'
    elif weight == CHE:
        return 'CHE'
    elif weight == IRO:
        return 'IRO'
    elif weight == NFE:
        return 'NFE'
    elif weight == NEU:
        return 'NEU'
    elif weight == PRU_SOL:
        return 'PRU_SOL'
    elif weight == AGS:
        return 'AGS'
    elif weight == SWD_INC:
        return 'SWD_INC'
    elif weight == FFF:
        return 'FFF'
    else:
        return ''"""

## For EDGAR v60
categories_codeblock_raster = """def raster(id):
    if id == 'ENE':
        return emi_cate_colormap[id]
    else id = 'REF_TRF'
        return emi_cate_colormap[id]
    else id = 'IND'
        return emi_cate_colormap[id]
    else id = 'TNR_Aviation_CDS'
        return emi_cate_colormap[id]
    else id = 'TNR_Aviation_CRS'
        return emi_cate_colormap[id]
    else id = 'TNR_Aviation_LTO'
        return emi_cate_colormap[id]
    else id = 'TRO_noRES'
        return emi_cate_colormap[id]
    else id = 'TNR_Other'
        return emi_cate_colormap[id]
    else id = 'TNR_Ship'
        return emi_cate_colormap[id]
    else id = 'RCO'
        return emi_cate_colormap[id]
    else id = 'PRO'
        return emi_cate_colormap[id]
    else id = 'NMM'
        return emi_cate_colormap[id]
    else id = 'CHE'
        return emi_cate_colormap[id]
    else id = 'IRO'
        return emi_cate_colormap[id]
    else id = 'NFE'
        return emi_cate_colormap[id]
    else id = 'NEU'
        return emi_cate_colormap[id]
    else id = 'PRU_SOL'
        return emi_cate_colormap[id]
    else id = 'AGS'
        return emi_cate_colormap[id]
    else id = 'SWD_INC'
        return emi_cate_colormap[id]
    else id = 'FFF'
        return emi_cate_colormap[id]
    else:
        return 0"""


# 设定要处理的时间范围
yr = list(range(1970,1972))

for y in yr:
    start_year(y)
    # define the variables
    calculate_sum = ''
    emi_cate_temp = {}
    emi_weight_raster_temp = {}
    emi_weight_point_temp = {}
    emi_raster_save_output = workspace + '\\total_mission_%s' % y
    # 找到所有部分
    # for i in emi_cate:
    #     emi_cate_temp[i] = workspace + '\\' + emi_cate[i] + '_' + str(y)

    emi_cate_raster_temp = arcpy.ListRasters(wild_card='BA_*_%s' % y)

    if emi_cate_raster_temp == []:
        print "Cannt find raster of %s : Empty direction or database! Or error input directions!" % yr
        exit
    
    # 为字典填充数据
    # 这里的填充方法比较麻烦。
    # 只能比较机械的逐值填充，暂时没有想到比较简明的方法
    for i in emi_cate:
        temp_cate_sector = [s for s in emi_cate_raster_temp if i in s]
        emi_cate_temp[i] = workspace + '\\%s' % temp_cate_sector[0]
        # 以防万一，清空临时的temp_cate_sector
        temp_cate_sector = []

    # For EDGAR V60
    calculate_sum = Raster(emi_cate_temp['ENE']) + Raster(emi_cate_temp['REF_TRF']) + Raster(emi_cate_temp['IND']) + Raster(emi_cate_temp['TNR_Aviation_CDS']) + Raster(emi_cate_temp['TNR_Aviation_CRS']) + Raster(emi_cate_temp['TNR_Aviation_LTO']) + Raster(emi_cate_temp['TRO_noRES']) + Raster(emi_cate_temp['TNR_Other']) + Raster(emi_cate_temp['TNR_Ship']) + Raster(emi_cate_temp['RCO']) + Raster(emi_cate_temp['PRO']) + Raster(emi_cate_temp['NMM']) + Raster(emi_cate_temp['CHE']) + Raster(emi_cate_temp['IRO']) + Raster(emi_cate_temp['NFE']) + Raster(emi_cate_temp['NEU']) + Raster(emi_cate_temp['PRU_SOL']) + Raster(emi_cate_temp['AGS']) + Raster(emi_cate_temp['SWD_INC']) + Raster(emi_cate_temp['FFF'])

    # Save total emission raster
    calculate_sum.save(emi_raster_save_output)
    print 'Total emission saved!\n'

    # calculate weights for each parts
    # Those four functions after designed by LHSB
    weight_calculate(y, emi_cate_temp,
                     emi_weight_raster_temp,
                     emi_weight_point_temp)
    weight_joint(y, emi_weight_point_temp)
    weight_raster(y)
    finish_year(y)

# MAIN SCRIPT ENDS
