# -*- coding: utf-8 -*-

import os
import re

# 这两个库负责处理未来年份的日期
import time
import datetime

# arcpy 库
import arcpy
from arcpy import env
from arcpy.sa import *


# ============================================================================
# ============================================================================
# Fucntion:
#   TCalculator
# Arguments:
#   string TempType, string rcp path, int StartYear, int EndYear
# Usage:
#   这个函数将日平均转换为月平均（其实就是增加数据量，好看~我还能变着花儿的给你算各种
#   尺度的平均呢~）
#   如果第二个参数是hsit，代表计算历史时期的平均态的温度，时间段1961-1990
#   第一个参数是温度的类型：最高、最低和平均。
#   分别对应：tmax, tmin, tavg
#   后两个参数是起始和结束的年份。
#   实际计算方法可能会比较复杂，具体细节，边写代码边在这儿写
# ============================================================================
# ============================================================================
def TCalculator(tt, rcp, yrs, yre):
    # 选出需要时间段内的栅格
    TDate = DateSet(yrs, yre)
    rasters = SelectInYear(arcpy.ListRasters(tt + '_' + rcp + '*'), TDate)

    date_counter = TDate['start']
    while date_counter < TDate['end']:
        temp_raster = SelectInMonth(rasters, date_counter)
        save_raster = arcpy.env.workspace + os.sep + tt + '_' + rcp + '_' + date_counter.isoformat().replace('-', '')[:-2]
        avg_days = len(temp_raster)

        # 先用第一个栅格创建一个基础栅格图层，然后移除它
        # 为什么arcpy不实现一个生成空栅格的算法？难道是我想太多？
        avg_raster = Raster(arcpy.env.workspace + os.sep + temp_raster[0])
        temp_raster.remove(temp_raster[0])

        # 计算平均
        for tr in temp_raster:
            try:
                avg_raster = avg_raster + Raster(arcpy.env.workspace + os.sep + tr)
            except:
                files_fail['raster'].append(tr)
                print arcpy.GetMessages()
                print 'Calculate average failed: %s at %s' % (tr, date_counter.isoformat())

        avg_raster = avg_raster / avg_days
        avg_raster.save(save_raster)
        print 'Calculate average successed: %s' % date_counter.isoformat().replace('-', '')[:-2]

        if date_counter.month + 1 > 12:
            date_counter = date_counter.replace(year=date_counter.year + 1, month=1, day=1)
        else:
            date_counter = date_counter.replace(month=date_counter.month + 1)


# ============================================================================
# ============================================================================
# Fucntion:
#   THistoricalAvg
# Arguments:
#   none
# Usage:
#   这个函数将日平均转换为月平均（其实就是增加数据量，好看~我还能变着花儿的给你算各种
#   尺度的平均呢~）
#   如果第二个参数是hsit，代表计算历史时期的平均态的温度，时间段1961-1990
#   第一个参数是温度的类型：最高、最低和平均。
#   分别对应：tmax, tmin, tavg
#   后两个参数是起始和结束的年份。
#   实际计算方法可能会比较复杂，具体细节，边写代码边在这儿写
# ============================================================================
# ============================================================================
def THistoricalAvg():
    # 选出需要时间段内的栅格
    TDate = DateSet(1961, 1990)
    rasters = SelectInYear(arcpy.ListRasters('tas_hist_*'), TDate)

    month_countor = 1 
    while month_countor < 13:
        temp_raster = SelectInMonthHist(rasters, month_countor)
        save_raster = arcpy.env.workspace + os.sep + ('tas_historical_avg_%02d' % month_countor)
        avg_months = len(temp_raster)

        # 先用第一个栅格创建一个基础栅格图层，然后移除它
        # 为什么arcpy不实现一个生成空栅格的算法？难道是我想太多？
        avg_raster = Raster(arcpy.env.workspace + os.sep + temp_raster[0])
        temp_raster.remove(temp_raster[0])

        # 计算平均
        for tr in temp_raster:
            try:
                avg_raster = avg_raster + Raster(arcpy.env.workspace + os.sep + tr)
            except:
                files_fail['hist'].append(tr)
                print arcpy.GetMessages()
                print 'Calculate historical average failed: %s at %s' % (tr, month_countor)

        avg_raster = avg_raster / avg_months
        avg_raster.save(save_raster)
        print 'Calculate historical average successed: %s' % month_countor
        month_countor += 1


# 请看名字，名字就是意义
def DateSet(yrs, yre):
    # 设置日期
    date_info = {}
    date_info['start'] = datetime.date(yrs, 1, 1)
    date_info['end'] = datetime.date(yre, 12, 31)

    return date_info


# 从数据库提取的列表中找到限定年份段内的数据
# 第一个参数是listraster返回的列表。
# 第二个参数是DataSet返回的那个起始和结尾年份的字典
# 返回一个经过筛选后的列表
def SelectInYear(rasterslist, tdate):
    # 这里存在另一个快速筛选的思路：
    # 如果rasterlist出来的数据就是按照顺序排列好的，可以直接用列表切片的方法。
    # 如果顺序不是有序的，那么，这个思路将先排序一次耗费大量时间。
    # 有待进一步思考。

    temp_rasterlist = rasterslist[:]
    temp_re = []
    for rl in temp_rasterlist:
        if int(rl[-6:-2]) >= tdate['start'].year:
            if int(rl[-6:-2]) <= tdate['end'].year:
                temp_re.append(rl)

    return temp_re


# 提取一个月内的数据
# 第一个参数是listraster返回的列表。
# 第二个参数是一个完整的datetime，但是请确保月是正确的
# 返回一个经过筛选后的列表
def SelectInMonth(rasterslist, tmonth):
    # 另一筛选思路参见上一个函数。

    temp_rasterlist = rasterslist[:]
    temp_re = []
    for rl in temp_rasterlist:
        # 暴力筛选年份和月份
        if int(rl[-8:-4]) == tmonth.year:
            if int(rl[-4:-2]) == tmonth.month:
                temp_re.append(rl)

    return temp_re


# 用于计算平均态的筛选函数
def SelectInMonthHist(rasterslist, tmonth):
    temp_rasterlist = rasterslist[:]
    temp_re = []
    for rl in temp_rasterlist:
        if int(rl[-2:]) == tmonth:
            temp_re.append(rl)

    return temp_re


# ============================================================================
# ============================================================================
# MISCELLANEOUS FUNCTIONS
# ============================================================================
# ============================================================================
# 从文件名中提取必要信息
def nc_read_info(ncfile):
    temp_ncinfo = {}

    temp_ncinfo['name'] = ncfile
    temp_str = ncfile.split('\\')[-1]
    temp_str = temp_str.split('_')

    temp_ncinfo['type'] = temp_str[0]
    temp_ncinfo['btype'] = temp_str[1]
    temp_ncinfo['byrs'] = int(temp_str[2])
    temp_ncinfo['byre'] = int(temp_str[3])
    temp_ncinfo['model'] = temp_str[4]
    temp_ncinfo['rcp'] = temp_str[5]
    temp_ncinfo['yrstart'] = int(temp_str[6].split('-')[0])
    temp_ncinfo['yrend'] = int(temp_str[6].split('-')[-1][:-3])

    return temp_ncinfo


# 从nc转换为raster的主函数
def nc_make_raster(ncinfo):
    # 设置日期
    date_start = datetime.date(ncinfo['yrstart'], 1, 1)
    date_end = datetime.date(ncinfo['yrend'], 12, 31)
    date_add = datetime.timedelta(1)

    # make netcdf to rasters loop
    date = date_start
    while date <= date_end:
        try:
            # Set local variables
            inNetCDFFile = ncinfo['name']
            variable = ncinfo['type']
            XDimension = 'lon'
            YDimension = 'lat'
            # 注意保存文件名的时候，arcgis保存入数据库是不允许出现‘-’符合的，所以要去除
            outRasterLayer = arcpy.env.workspace + os.sep + ncinfo['type'] + '_' + ncinfo['rcp'] + '_' + date.isoformat().replace('-', '') + '_nc'
            bandDimmension = ''
            dimensionValues = 'time %s' % date.isoformat()
            valueSelectionMethod = 'BY_VALUE'

            # Execute MakeNetCDFRasterLayer
            arcpy.MakeNetCDFRasterLayer_md(inNetCDFFile,
                                           variable,
                                           XDimension, YDimension,
                                           outRasterLayer,
                                           bandDimmension, dimensionValues,
                                           valueSelectionMethod)
            
            # save raster dataset
            temp_save = arcpy.env.workspace + '\\' + ncinfo['type'] + '_' + ncinfo['rcp'] + '_' + date.isoformat().replace('-', '')
            Raster(outRasterLayer).save(temp_save)
            print 'Read netCDF successed: %s at %s' % (ncinfo['name'], date.isoformat())

        except:
            files_fail['nc'].append(ncinfo['name'] + ' at ' + date.isoformat())
            print arcpy.GetMessages()
            print 'Read netCDF failed: %s at %s' % (ncinfo['name'], date.isoformat())

        # 时间+1，完成万物的循环~~~
        date += date_add


# nc 文件读取情况
def ffl_nc():
    print "======================================================================"
    print "======================================================================"
    print "Congratulations! Finished reading nc files!"
    print "Report:"
    print "\tFailed: %s" % len(files_fail['nc'])
    print "\tFailed list:"
    print files_fail['nc']
    print "======================================================================"
    print "======================================================================"


# 平均温度计算情况
def ffl_avg():
    print "======================================================================"
    print "======================================================================"
    print "Congratulations! Finished calculate raster files!"
    print "Report:"
    print "\tFailed: %s" % len(files_fail['raster'])
    print "\tFailed list:"
    print files_fail['raster']
    print "======================================================================"
    print "======================================================================"


# 平均温度计算情况
def ffl_his():
    print "======================================================================"
    print "======================================================================"
    print "Congratulations! Finished calculate historical averages!"
    print "Report:"
    print "\tFailed: %s" % len(files_fail['hist'])
    print "\tFailed list:"
    print files_fail['hist']
    print "======================================================================"
    print "======================================================================"
# ============================================================================
# ============================================================================
# MAIN SCRIPT
# ============================================================================
# ============================================================================
# 设置arcpy临时工作空间
arcpy.env.workspace = 'E:\\workplace\\CarbonProject\\geodatabase\\carbon_temp.gdb'
# arcpy.env.workspace = 'D:\\dk\\CMIP5_database\\cimp_hist.gdb'
# 检查arcgis空间分析扩展许可
arcpy.CheckOutExtension("Spatial")

# 失败文件目录
files_fail = {'nc': [], 'raster': [], 'hist': []}

# 列出目录下所有.nc文件
nc_path = "E:\\workplace\\CarbonProject\\DATA\\rcp2p6"
# nc_path = "D:\\dk\\CMIP5_database\\data\\historical"
nc_files = [os.path.join(nc_path, f) for f in os.listdir(nc_path) if os.path.isfile(os.path.join(nc_path, f))]

# Main method starts
# read nc files as raster dataset
for nc_file in nc_files:
    # 保存nc文件信息
    nc_info = nc_read_info(nc_file)

    # make raster
    nc_make_raster(nc_info)

    # 恭喜你成功处理了一个nc文件
    print "Successed read nc: %s " % nc_info['name']

# print process report
ffl_nc()

# calculate avg
# historical 
TCalculator('tas', 'hist', 1961, 1990)
# # print process report
ffl_avg()

# average
THistoricalAvg()
ffl_his()
# Main method ends
