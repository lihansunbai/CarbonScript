# -*- coding: utf-8 -*-

import os
import arcpy
from arcpy import env
from arcpy.sa import *
import tqdm
from tqdm import tqdm

__metaclass__ = type

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


class EDGAR_spatial:
    ## 构造函数部分
    ## 注意：这里需要两类构造函数：
    ##      1.默认构造函数：不需要传入任何参数。所有计算用到的参数均
    ##        为默认值。
    ##      2.带有数据位置的构造函数：需要传入一个
    def __init__(self, workspace, sector={}, colormap={}, st_year=1970, en_year=2018):
        # arcgis 工作空间初始化
        ## 必须明确一个arcgis工作空间！
        ## 初始化构造需要明确arcgis工作空间或者一个确定的数据为
        ## 检查输入是否为空值
        if workspace == '':
            print 'Spatial direction or database path error! Please check your input!'
            return

        ## 为工作空间进行赋值
        ### 这里需要为两个参数赋值：第一个参数是系统中arcpy environment workspace 参数，
        ###  该参数保证了进行arcgis空间运算的“空间分析扩展”检查通过；第二个参数是为了
        ###  缩短代码中“arcpy.env.workspace”属性的书写长度而设置的代用变量。
        self.__workspace = workspace
        arcpy.env.workspace = workspace
        # 利用栅格计算器进行栅格代数计算时需要先检查是否开启了空间扩展
        arcpy.CheckOutExtension('Spatial')
        arcpy.env.parallelProcessingFactor = "100%"

        # EDGAR_sector 参数初始化部分
        ## 检查输入参数类型
        ## 默认情况下使用默认参数初始化
        ## 为EDGAR_sector参数赋值
        if type(sector) != dict:
            print 'Error! EDGAR_sector only accept a dictionary type input.'
            return
        elif sector == {}:
            self.EDGAR_sector = self.__default_EDGAR_sector
        else:
            self.EDGAR_sector = sector

        # EDGAR_sector_colormap 参数初始化部分
        ## 检查参数输入类型
        ## 默认情况下使用默认参数初始化
        ## 为EDGAR_sector_colormap 参数赋值
        if type(colormap) != dict:
            print 'Error! EDGAR_sector_colormap only accept a dictionary type input.'
            return
        elif sector == {}:
            self.EDGAR_sector_colormap = self.__default_EDGAR_sector_colormap
        else:
            self.EDGAR_sector_colormap = colormap

        # year_range 参数初始化部分
        ## 这里需要初始化计算的起始和结束
        if (type(st_year) != int) or (type(en_year) != int):
            print 'Error! Proccessing starting year and ending year must be int value'
            return
        elif st_year < 1970 or en_year > 2018:
            print 'Error! Proccessing year range out of data support! The year must containt in 1970 to 2018'
        else:
            self.start_year, self.end_year = st_year, en_year

    # Default values:
    ## Arcgis workspace
    __workspace = ''

    ## EDGAR sector dicts & colormap dicts
    EDGAR_sector = {}
    EDGAR_sector_colormap = {}
    __default_EDGAR_sector = {'ENE': 'ENE',
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

    __default_EDGAR_sector_colormap = {'ENE': 1,
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

    # 时间范围
    __default_start_year = 1970
    __default_end_year = 2018

    start_year = 0
    end_year = 0

    __background_flag = True
    __background_lable = 'BA'

    # 特殊变量，用于保存所有部门排放的总和
    __raster_sum = ''

    # 保存部门排放的累加结果
    __raster_overlay = ''

    # 想要自定义或者修改处理的部门排放需要使用特殊的set函数
    def set_EDGAR_sector(self, sector):
        if type(sector) != dict:
            print 'Error type! EDGAR sectors should be dictionary!'
            return

        self.EDGAR_sector = sector

    def get_EDGAR_sector(self):
        print self.EDGAR_sector

    sector = property(get_EDGAR_sector, set_EDGAR_sector)

    def set_EDGAR_sector_colormap(self, sector_colormap):
        if type(sector_colormap) != dict:
            print 'Error type! EDGAR sectors colormap should be diectionary!'
            return

        self.EDGAR_sector_colormap = sector_colormap

    def get_EDGAR_sector_colormap(self):
        print self.EDGAR_sector_colormap

    sector_colormap = property(get_EDGAR_sector_colormap, set_EDGAR_sector_colormap)

    def set_year_range(self, start_end=(1970, 2018)):
        self.start_year, self.end_year = start_end

    def get_year_range(self):
        print 'Start year: %s\nEnd year: %s' % (self.start_year, self.end_year)

    year_range = property(get_year_range, set_year_range)

    # TODO
    # 这里缺一个筛选需要进行运算的栅格数据的方法
    # 原因：一个数据库中的文件命名可能只包含了'ENE_2010'类似字段。或者是混合了其他数据，比如结果生成的数据
    #       所以，需要使用ListRaster把这需要工作的栅格筛选出来
    ## 问题：我生成的数据中包含了有背景0值和背景为空值null的两类栅格。这种情况下应该如何筛选？需要添加标识符参数？
    ##  或者是排除字符参数？

    # 栅格图像背景值设置和查看属性/函数
    def set_background_value_flag(self, flag, flag_lable):
        # 检查flag参数并赋值
        try:
            __background_flag = bool(flag)
        except:
            print 'Background value flag set failed! Please check the flag argument input.'
        
        # 检查flag_lable参数并
        if type(flag_lable) == str:
            __background_lable = flag_lable
        else:
            print 'Background value flag lable set failed! Please check the flag argument input.'


    def get_background_value_flag(self):
        print 'Background value enabled: %s' % self.__background_flag
        print 'Background lable: %s' % self.__background_lable

    background_value_flag = property(get_background_value_flag, set_background_value_flag)

    def list_working_rasters(self, background_flag, sector, year):
        pass

    def generate_working_environment(self):
        pass

    def check_working_environment(self):
        pass



















    def raster_overlay_add(self, add_sector):
        # 利用栅格计算器进行栅格代数计算时需要先检查是否开启了空间扩展
        arcpy.CheckOutExtension('Spatial')

        # 栅格叠加的结果保存在__raster_overlay 中
        # self.__raster_overlay = Raster()

        # 临时变量，防止突发崩溃
        temp_raster = Raster()

        # 叠加栅格
        ## 这里调用了 tqdm 库进行进度显示
        for r in tqdm(add_sector):
            temp_raster = Raster(temp_raster) + Raster(add_sector[r])

    def calculate_sum(self, year):
        # 计算总量的本质就是把所有部门都加起来。
        self.raster_overlay_add(self.EDGAR_sector)
        self.__raster_sum = self.__raster_overlay

        # 这里的路径需要修改
        # 可能需要引入很多个保存文件的输出位置
        temp_out_path = self.__workspace + '%s' % year
        self.__raster_sum.save(temp_out_path)
        print 'Total emission saved!\n'

    def weight_calculate(self, year, sector, output_weight_point):
        for i in tqdm(sector):
            # 计算部门排放相对于全体部门总排放的比例
            output_weight_raster = sector[i] / self.__raster_sum
            ## 保存栅格权重计算结果
            temp_output_weight_raster_path = self.__workspace + '\\' + '%s_weight_raster_%s' % (i, year)
            output_weight_point.save(temp_output_weight_raster_path)
            print 'Sector emission weight saved: %s\n' % i

            # 栅格数据转点对象。转为点对象后可以实现计算比例并同时记录对应排放比例的部门名称
            output_weight_point[i] = self.__workspace + '\\' + '%s_weight_point_%s' % (i, year)

            try:
                # transform to point features
                arcpy.RasterToPoint_conversion(output_weight_raster, output_weight_point, 'VALUE')

                # rename value field
                arcpy.AddField_management(output_weight_point[i], i, 'DOUBLE')
                arcpy.CalculateField_management(output_weight_point[i], i, '!grid_code!', 'PYTHON_9.3')

                # 删除表链接结果结果中生成的统计字段'pointid'和'grid_code'
                arcpy.DeleteField_management(output_weight_point[i], 'pointid')
                arcpy.DeleteField_management(output_weight_point[i], 'grid_code')
                print 'Categories finished: %s' % i
            except:
                print 'Failed categories to point : %s' % i
                print arcpy.GetMessages()

        print 'Categories to point finished of %s' % year

    def weight_joint(self, year, weight_point):
        # 理解这个函数中的操作需要将temp_pointer_a, temp_pointer_b视为指针一样的东西
        # 通过不停的改变他们指向的对象，来完成空间链接的操作。
        # C/C++万岁！！！指针天下第一！！！

        # 复制这个函数操作中需要用到的字典
        ## 复制传入的参数字典
        temp_emi_weight = weight_point.copy()

        # 输出路径
        save_shp = self.__workspace + '\\categories_%s' % year

        # # 函数内的全局循环计数
        # iter_counter = 1

        # 构造三个特殊变量来完成操作和循环的大和谐~、
        # 因为SpatialJoin函数需要一个输出表，同时又不能覆盖替换另一个表
        # 所以需要用前两个表生成第一个循环用的表
        # 在程序的结尾用最后（其实可以是任意一个表）来完成年份的输出
        temp_first = temp_emi_weight.popitem()
        temp_second = temp_emi_weight.popitem()
        temp_final = temp_emi_weight.popitem()

        # 连接第一个表和第二个表(temp_first and temp_second)
        temp_pointer_a = self.__workspace + '\\iter_%s_%s' % (year, temp_second[1])
        try:
            print 'Spatial join start:'
            arcpy.SpatialJoin_analysis(temp_first,
                                       temp_second,
                                       temp_pointer_a,
                                       'JOIN_ONE_TO_ONE', 'KEEP_ALL')
            ## 删除表中的链接结果的字段
            arcpy.Delete_management(temp_pointer_a, 'Join_Count')
            arcpy.Delete_management(temp_pointer_a, 'TARGET_FID')

            # ## 循环计数增1
            # iter_counter += 1
            print 'Spatial join complete: %s %s with %s' % (year, temp_first[1], temp_second[2])
        except:
            print 'Spatia join failed: %s and %s' % (temp_first, temp_second)
            print arcpy.GetMessages()

        # loop begain
        for i in tqdm(temp_emi_weight):
            temp_pointer_b = self.__workspace + '\\iter_%s_%s' % (year, i)
            try:
                arcpy.SpatialJoin_analysis(temp_pointer_a,
                                           temp_emi_weight[i],
                                           temp_pointer_b,
                                           'JOIN_ONE_TO_ONE', 'KEEP_ALL')

                ## 交换指针，使b指针成为下一次循环的链接目标
                temp_pointer_a = temp_pointer_b

                ## 删除表中的链接结果的字段
                arcpy.Delete_management(temp_pointer_a, 'Join_Count')
                arcpy.Delete_management(temp_pointer_a, 'TARGET_FID')

                # ## 循环计数增1
                # iter_counter += 1
                print 'Spatial join complete: %s with %s' % (year, i)
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

            ## 删除表中的链接结果的字段
            arcpy.Delete_management(save_shp, 'Join_Count')
            arcpy.Delete_management(save_shp, 'TARGET_FID')
        except:
            print 'Spatia join failed: %s' % temp_final
            print arcpy.GetMessages()

        print 'Finished categories to point features: %s' % save_shp

    # 导出不同年份最大权重栅格
    def weight_raster(self, year):
        temp_point = self.__workspace + '\\categories_%s' % year
        save_raster_categories = self.__workspace + '\\main_emi_%s' % year
        save_raster_weight = self.__workspace + '\\main_emi_weight_%s' % year

        # 向point feature中添加列
        # 1.权重最大值 wmax
        # 2.权重最大值名称 wmaxid
        # 3.将权重最大值名称映射为一个整数，方便输出为栅格 wraster
        # 4.统计一个栅格中共计有多少个部门排放
        # 并计算添加字段的值
        temp_new_fields = ['wmax','wmaxid','wraster','sector_counts']
        try:
            # wmax
            arcpy.AddField_management(temp_point,
                                      temp_new_fields[0],
                                      'DOUBLE', '#', '#', '#', '#',
                                      'NULLABLE', '#', '#')

            # wmaxid
            arcpy.AddField_management(temp_point,
                                      temp_new_fields[1],
                                      'TEXT', '#', '#', '#', '#',
                                      'NULLABLE', '#', '#')

            # wraster
            arcpy.AddField_management(temp_point,
                                      temp_new_fields[2],
                                      'SHORT', '#', '#', '#', '#',
                                      'NULLABLE', '#', '#')

            # sector_counts
            arcpy.AddField_management(temp_point,
                                      temp_new_fields[3],
                                      'DOUBLE', '#', '#', '#', '#',
                                      'NULLABLE', '#', '#')
        except:
            print 'Add field to point faild: %s' % temp_point
            print arcpy.GetMessages()
            return

        self.sector_max(year, temp_point, temp_new_fields)

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

    # 用arcpy.da.cursor类进行操作
    # 在一行中同时实现找到最大值，最大值对应的id，最大值对应的colormap
    def sector_max(self, sector_points, calculate_fields):
        temp_sector = self.sector.copy()
        temp_sector_colormap = self.sector_colormap.copy()
        temp_working_sector = sector_points

        # 构造需要操作的字段
        ## 神奇的python赋值解包
        temp_cursor_fileds = [i for i in temp_sector]

        # 按照calculate_fields 参数追加需要进行计算的字段
        ## 输出结果的四个字段：最大值、最大值部门、colormap、部门数量
        ### 部门字段的数量
        sector_counts = len(temp_sector)
        ### 计算字段的数量
        calculate_fields_counts = len(calculate_fields)

        # 添加需要计算的字段到游标提取字段的list中
        temp_cursor_fileds.extend(calculate_fields)

        # 构造游标，开始逐行操作
        with arcpy.da.UpdateCursor(temp_working_sector, temp_cursor_fileds) as cursor:
            for row in cursor:
                max_weight = max(row[0:-calculate_fields_counts])
                max_id = temp_cursor_fileds(row.index(max_weight))
                max_colormap = temp_sector_colormap[max_id]
                emitted_sectors = len([i for i in row[0:-calculate_fields_counts] if i != 0])

                row[-1] = emitted_sectors
                row[-2] = max_colormap
                row[-3] = max_id
                row[-4] = max_weight

                cursor.updateCursor(row)

    def print_start_year(year):
        print '=============================='
        print '=============================='
        print 'Processing start of year %s' % year
        print '=============================='
        print '=============================='

    def print_finish_year(year):
        print '=============================='
        print '=============================='
        print 'Congratulations!'
        print 'Finished processing data of year %s' % year
        print '=============================='
        print '=============================='


if __name__ == '__main__':
    pass
