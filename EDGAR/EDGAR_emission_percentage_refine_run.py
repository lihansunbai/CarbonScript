# -*- coding: utf-8 -*-

import os

import arcpy
import EDGAR_emission_percentage_refine_class_spatial
from GIT.EDGAR.EDGAR_emission_percentage_refine_class_spatial import EDGAR_spatial



# ======================================================================
# ======================================================================
# MAIN SCRIPT
# ======================================================================
# ======================================================================
# !!! 注意 !!! 运行此脚本前，请先运行所有子部分排放的提取
workspace =  'D:\\workplace\\DATA\\geodatabase\\test\\EDGAR_test.gdb'


test_edgar_spatial = EDGAR_spatial(workspace)

# TODO
# 这里可能需要一个输出，表示类创建成功，并请给出对应初始化后的参数
















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
