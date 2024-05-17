# -*- coding: utf-8 -*-

# 路径处理模块
# System path processing module
import os

# Arcpy 相关模块
# Arcpy module
import arcpy
from arcpy import env
from arcpy.sa import *

# 其他相关模块
# other functional modules
import re
import copy
import tqdm
from tqdm import tqdm
import tabulate
from tabulate import tabulate
import logging
import csv
import math
import collections
import numpy
import interval
import numbers
import shortuuid
import h5py
import json

# # 性能测试相关模块
# import cProfile
# import pytest
# import faulthandler

# faulthandler.enable()

__metaclass__ = type

# ======================================================================
# ======================================================================
# Memorandum:
# 备忘录：
#       TODO 叠加栅格的过程中需要先清楚栅格中的小于0值，
#            防止对数操作中的各种问题。
# ======================================================================
# ======================================================================

# ======================================================================
# ======================================================================
# SPATIAL OPERATIONS CLASS
# ======================================================================
# ======================================================================


class EDGAR_spatial(object):
    ############################################################################
    ############################################################################
    # 构造函数部分
    # 注意：这里需要两类构造函数：
    # 1.默认构造函数：不需要传入任何参数。所有计算用到的参数均为默认值。
    # 2.专门构造函数：包括只处理进行合并栅格数据的构造函数和只进行中心提取和
    #   统计的构造函数。
    ############################################################################
    ############################################################################
    def __init__(self,
                 workspace,
                 st_year=1970,
                 en_year=2018,
                 log_path='EDGAR.log'):
        # 初始化logger记录类的全体工作
        # ES_logger为可使用的logging实例
        # 类使用的logger
        self.ES_logger = logging.getLogger()
        self.ES_logger.setLevel(level=logging.DEBUG)
        ES_logger_file = logging.FileHandler(log_path)
        ES_logger_formatter = logging.Formatter(
            '%(asctime)s-[%(levelname)s]-[%(name)s]-[%(funcName)s]-%(message)s')
        ES_logger_file.setFormatter(ES_logger_formatter)
        self.ES_logger.addHandler(ES_logger_file)

        self.ES_logger.info('==========EDGAR_Spatial start==========')

        # 初始化累需要使用的实例变量
        # 使用“实例变量”而不是“类变量”的原因请参见：以下链接的9.3.5节内容
        # https://docs.python.org/zh-cn/3/tutorial/classes.html

        # year_range 参数初始化部分
        # 这里需要初始化计算的起始和结束
        if (type(st_year) != int) or (type(en_year) != int):
            print('Error! Processing starting year and ending year must be int value')
            self.ES_logger.info('Year setting type error.')
            self.ES_logger.error('Year setting error!')
            exit(1)
        elif st_year < self.__default_start_year or en_year > self.__default_end_year:
            print('Error! Processing year range out of data support! The year must contain in 1970 to 2018')
            self.ES_logger.info('Year settings are out of range.')
            self.ES_logger.error('Year setting error!')
            exit(1)
        else:
            self.year_range = (st_year, en_year)
            self.ES_logger.info('Year has set.')

        # 需要操作的栅格
        self.working_rasters = []

        # arcgis 工作空间初始化
        # 必须明确一个arcgis工作空间！
        # 初始化构造需要明确arcgis工作空间或者一个确定的数据为
        # 检查输入是否为空值
        if workspace == '':
            print('Spatial direction or database path error! Please check your input!')
            self.ES_logger.error('arcpy environment workspace set failed!')
            exit(1)

        # 为工作空间进行赋值
        # 这里需要为两个参数赋值：第一个参数是系统中arcpy environment workspace 参数，
        # 该参数保证了进行arcgis空间运算的“空间分析扩展”检查通过；第二个参数是为了
        # 缩短代码中“arcpy.env.workspace”属性的书写长度而设置的代用变量。
        self.__workspace = workspace
        arcpy.env.workspace = workspace
        self.ES_logger.info('workplace has set.')
        # 利用栅格计算器进行栅格代数计算时需要先检查是否开启了空间扩展
        arcpy.CheckOutExtension('Spatial')
        self.ES_logger.info('arcpy Spatial extension checked.')
        # 将多线程处理设置为100%
        #   吐槽：虽然没什么用，cpu利用率最多也只能达到5%
        arcpy.env.parallelProcessingFactor = "200%"
        self.ES_logger.info('arcpy parallelProcessingFactor set to {}.'.format(arcpy.env.parallelProcessingFactor))

        arcpy.env.overwriteOutput = True
        self.ES_logger.info('arcpy overwriteOutput set to True.')

        self.ES_logger.info('Root initialization finished.')

    # 只进行合并分部门栅格为点数据时的构造函数
    # TODO
    # 这里出现了一个逻辑错误，假设使用的是无背景模式background_flag=False，
    # 实际上构造函数无法从root_init的初始构造函数中获得变量信息。需要修改逻辑。
    # 但是，似乎这个参数不影响data_analysis构造。但是data_analysis的构造过程也需要进行类似排查。
    @classmethod
    def merge_sectors(cls,
                      workspace,
                      background_flag=True,
                      background_flag_label='BA',
                      background_raster='background',
                      default_raster_filter=True,
                      sectors={},
                      colormap={},
                      st_year=1970,
                      en_year=2018,
                      log_path='EDGAR.log'):
        '''
        只进行合并分部门栅格为点数据时的构造函数
        '''
        # 先调用init构造函数初始化类
        root_init = cls(workspace=workspace, log_path=log_path, st_year=st_year, en_year=en_year)

        # 数据库过滤标签
        root_init.raster_filter_wildcard = []

        # 构建数据库过滤标签的信息
        root_init.filter_label_dict = {}

        # 准备时间范围内的所有部门的栅格
        root_init.all_prepare_working_rasters = []

        # EDGAR_sectors 参数初始化部分
        # 检查输入参数类型
        # 默认情况下使用默认参数初始化
        # 为EDGAR_sectors参数赋值
        if type(sectors) != dict:
            print('Error! EDGAR_sectors only accept a dictionary type input.')
            root_init.ES_logger.info('EDGAR_sectors only accept a dictionary type input.')
            root_init.ES_logger.error('EDGAR_sectors type error.')
            exit(1)
        elif sectors == {}:
            root_init.sectors_handle = copy.deepcopy(root_init.__default_EDGAR_sectors)
            root_init.ES_logger.info('This run use default EDGAR sectors setting.')
            root_init.ES_logger.info('EDGAR_sectors has set.')
        else:
            root_init.sectors_handle = copy.deepcopy(sectors)
            root_init.ES_logger.info('EDGAR_sectors has set.')

        # EDGAR_sectors_colormap 参数初始化部分
        # 检查参数输入类型
        # 默认情况下使用默认参数初始化
        # 为EDGAR_sectors_colormap 参数赋值
        if type(colormap) != dict:
            print('Error! EDGAR_sectors_colormap only accept a dictionary type input.')
            root_init.ES_logger.info('EDGAR_sectors_colormap only accept a dictionary type input.')
            root_init.ES_logger.error('EDGAR_sectors_colormap type error.')
            exit(1)
        elif colormap == {}:
            root_init.sectors_colormap_handle = copy.deepcopy(
                root_init.__default_EDGAR_sectors_colormap)
            root_init.ES_logger.info('This run use default EDGAR sectors colormap setting.')
            root_init.ES_logger.info('EDGAR_sectors_colormap has set.')
        else:
            root_init.sectors_colormap_handle = copy.deepcopy(colormap)
            root_init.ES_logger.info('EDGAR_sectors_colormap has set.')

        # background 参数初始化部分
        # 这里要明确处理的数据是否包含背景0值
        # 检查并赋值label
        if bool(background_flag) == True:
            if type(background_flag_label) == str:
                root_init.background = {
                    'flag': bool(background_flag),
                    'label': background_flag_label,
                    'raster': background_raster
                }
                root_init.ES_logger.debug('Background has set.')
            else:
                print('Error: Please check background flag or label or raster.')
                root_init.ES_logger.error('Background setting error!')
                exit(1)
        elif bool(background_flag) == False:
            root_init.background = {'flag': bool(background_flag), 'label': '', 'raster': ''}
            root_init.ES_logger.debug('Background has set.')
        else:
            root_init.ES_logger.error('Background setting error!')
            exit(1)

        # raster_filter 参数初始化部分
        # 这里要将初始化传入的部门参数字典“sectors”进行列表化并赋值
        # 和起始、终止时间传入
        temp_init_filter_label = {
            'default':default_raster_filter,
            'background_label': root_init.background[1],
            'sectors': root_init.sectors_handle,
            'start_year': root_init.year_range[0],
            'end_year': root_init.year_range[1]
        }
        root_init.filter_label = temp_init_filter_label
        root_init.ES_logger.info('filter_label has set.')

        print('EDGAR_Spatial initialized! More debug information please check the log file.')
        root_init.ES_logger.info('Initialization finished.')

        # 返回初始化之后的类
        return root_init

    # 只进行排放量栅格数量峰值中心提取和其他分析操作时的构造函数
    @classmethod
    def data_analyze(cls, workspace, st_year=1970, en_year=2018, log_path='EDGAR.log'):
        '''
        只进行排放量栅格数量峰值中心提取和其他分析操作时的构造函数 
        '''
        # 先调用init构造函数初始化类
        root_init = cls(workspace=workspace, st_year=st_year, en_year=en_year, log_path=log_path)

        # 整合部门到分类的整合方式
        # 这个参数需要用property属性提供的方法构造
        root_init.gen_handle = root_init.__default_gen_handle

        # 为数据添加字段时使用的字段和属性的整合列表
        root_init.addField_list = []

        # 整合部门方法中执行数据库游标操作需要返回的字段名称
        # 这个参数需要用property属性提供的方法构造
        root_init.gen_field = []

        # 初始化分类编码
        root_init.generalization_encode = root_init.__default_gen_encode_list

        # 初始化排放峰值
        root_init.emission_peaks_time_series = {}

        # 初始化排放峰值总和
        root_init.emission_peaks_time_series_name_list = []

        # 初始化排放中心的列表
        root_init.emission_center_list = []

        print('EDGAR_Spatial initialized! More debug information please check the log file.')
        root_init.ES_logger.info('Initialization finished.')
        # 返回初始化之后的类
        return root_init

    ############################################################################
    ############################################################################
    # 默认类变量
    # Default class variances
    ############################################################################
    ############################################################################

    # Arcgis workspace
    __workspace = ''

    # Arcgis 默认栅格pixel_type
    __raster_pixel_type = {
        'U1': '1_BIT',
        'U2': '2_BIT',
        'U4': '4_BIT',
        'U8': '8_BIT_UNSIGNED',
        'S8': '8_BIT_SIGNED',
        'U16': '16_BIT_UNSIGNED',
        'S16': '16_BIT_SIGNED',
        'U32': '32_BIT_UNSIGNED',
        'S32': '32_BIT_SIGNED',
        'F32': '32_BIT_FLOAT',
        'F64': '64_BIT'
    }

    # EDGAR sectors dict & colormap dict
    __default_EDGAR_sectors = {
        'ENE': 'ENE',
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
        'FFF': 'FFF'
    }
    __default_EDGAR_sectors_colormap = {
        'ENE': 1,
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
        'FFF': 20
    }

    # 默认时间范围
    __default_start_year = 1970
    __default_end_year = 2018

    # 默认栅格数据背景零值标识和区分标签
    __background_flag = True
    __background_label = 'BA'
    __background_raster = 'background'

    # 默认过滤标签
    __default_filter_label_dict = {
        'default': True,
        'label': {
            'background_label': __background_label,
            'sectors': __default_EDGAR_sectors,
            'start_year': __default_start_year,
            'end_year': __default_end_year
        }
    }

    # 数据库栅格数据筛选过滤标签
    # 默认数据库过滤标签
    __default_raster_filter_wildcard = []

    # 默认部门分类字典：gen_handle
    __default_gen_handle = {
        'ENE': 'G_ENE',
        'REF_TRF': 'G_IND',
        'IND': 'G_IND',
        'TNR_Aviation_CDS': 'G_TRA',
        'TNR_Aviation_CRS': 'G_TRA',
        'TNR_Aviation_LTO': 'G_TRA',
        'TRO_noRES': 'G_TRA',
        'TNR_Other': 'G_TRA',
        'TNR_Ship': 'G_TRA',
        'RCO': 'G_RCO',
        'PRO': 'G_ENE',
        'NMM': 'G_IND',
        'CHE': 'G_IND',
        'IRO': 'G_IND',
        'NFE': 'G_IND',
        'NEU': 'G_IND',
        'PRU_SOL': 'G_IND',
        'AGS': 'G_AGS',
        'SWD_INC': 'G_WST',
        'FFF': 'G_ENE'
    }

    # 默认部门编码
    __default_gen_encode_list = ['G_ENE', 'G_IND', 'G_TRA', 'G_RCO', 'G_AGS', 'G_WST']

    ############################################################################
    ############################################################################
    # 通用参数、属性和方法
    ############################################################################
    ############################################################################

    # 想要自定义或者修改数据处理的年份时间范围的特殊property函数
    @property
    def year_range(self):
        '''
        显示当前默认的数据处理时间范围
        '''
        return (self.start_year, self.end_year)

    @year_range.setter
    def year_range(self, start_end=(1970, 2018)):
        '''
        自定义或者修改数据处理的年份时间范围
        '''
        self.start_year, self.end_year = start_end

        # logger output
        self.ES_logger.debug('year range changed to:{} to {}'.format(start_end[0], start_end[1]))

    def print_start_year(self, year):

        # logger output
        self.ES_logger.debug('Processing start of year {}'.format(year))

        print('==============================')
        print('==============================')
        print('Processing start of year {}'.format(year))
        print('==============================')
        print('==============================')

    def print_finish_year(self, year):

        # logger output
        self.ES_logger.debug('Finished processing data of year {}'.format(year))

        print('==============================')
        print('==============================')
        print('Congratulations!')
        print('Finished processing data of year {}'.format(year))
        print('==============================')
        print('==============================')

    # 简易导出栅格函数，并规定nodata的值
    def raster_quick_export(self, raster_list, nodata_value, output_path, output_formate):
        '''
        简易导出栅格函数，同时也能自定义导出数据的nodata值 
        '''
        # 检查输入的栅格列表是否存在
        if not raster_list:
            print('ERROR: input raster_list is empty. Please check the input.')

            # logger output
            self.ES_logger.error('input raster list is empty')
            exit(1)

        # 检查其余参数是否正确
        if not nodata_value or not output_formate or type(output_formate) != str:
            print('ERROR: nodata_value or output_formate argument error.')

            # logger output
            self.ES_logger.error('nodata_value or output_formate argument error')
            exit(1)

        # 对列表中的栅格逐个转化
        for raster in tqdm(raster_list):
            # 先检查栅格是否存在
            if not arcpy.Exists(raster):
                print('ERROR: input raster not found. Raster name: {}'.format(raster))

                # logger output
                self.ES_logger.error('ERROR: input raster not found. Raster name: {}'.format(raster))
                exit(1)

            # 先利用从con()函数将所有nodata定义为固定值
            temp_set_nodata_value = Con(
                in_conditional_raster=IsNull(raster),
                in_true_raster_or_constant=nodata_value,
                in_false_raster_or_constant=raster)

            # 生成输出文件名
            temp_output_name = '{}\\{}.{}'.format(output_path, raster, output_formate)

            arcpy.CopyRaster_management(
                in_raster=temp_set_nodata_value,
                out_rasterdataset=temp_output_name,
                format=output_formate)

    # 从HDF5中将数据转换为Arcgis raster
    # 使用这个函数要注意output_path参数，如果在构造函数初始化的过程中定义了工作空间，则可以直接传入不带后缀的保存文件名，
    # 输出的栅格将直接保存到工作空间中；如果，需要输出到非初始化过程中定义的工作空间位置，则应该传入完整的保存路径，
    # 这里的完整保存路径是指包括文件后缀的文件绝对路径。
    # 注意，这个方法相当简陋，只能导出不包含时间维度，即dim=2的数据。
    def hdf_to_raster(self, hdf_file_path, hierarchical_data_path, data_name, output_path):
        '''
        使用这个函数要注意output_path参数，如果在构造函数初始化的过程中定义了工作空间，则可以直接传入不带后缀的保存文件名，
        输出的栅格将直接保存到工作空间中；如果需要输出到非初始化过程中定义的工作空间位置，则应该传入完整的保存路径，
        这里的完整保存路径是指包括文件后缀的文件绝对路径。
        注意，这个方法相当简陋，只能导出不包含时间维度，即dim=2的数据。
        '''
        if not os.path.exists(hdf_file_path):
            print('ERROR: hdf file does not exist. Please check the input.')

            # logger output
            self.ES_logger.error('hdf file does not exist.')
            exit(1)
        
        # 打开hdf文件
        hdf = h5py.File(hdf_file_path, 'r')
        
        # 组合得到完整数据路径
        full_data_path = os.path.join(hierarchical_data_path, data_name)

        # 检查数据是否存在
        if full_data_path not in hdf:
            print('ERROR: data not exist. Please check the input.')

            # logger output
            self.ES_logger.error('data not exist.')
            exit(1)
        
        # 取得hdf中数据
        temp_numpy_data = hdf[full_data_path][...]

        # numpy 转 raster
        temp_raster = arcpy.NumPyArrayToRaster(temp_numpy_data, x_cell_size=0.1)
        # 为raster定义坐标系
        arcpy.DefineProjection_management(temp_raster, arcpy.SpatialReference(4326))
        # 保存raster
        temp_raster.save(output_path)

    # 为栅格添加背景值
    # 用途：添加一个背景值以保持栅格数据历史范围稳定。
    # 注意：如果指定了output_Raster参数，则会输出到该参数指定的栅格中。
    def mosaic_background_to_raster(self, inRaster, background, output_Raster=None):
        '''
        为栅附加一个范围以保持参与计算的栅格数据的历史范围稳定，栅格的范围通过背景值确定。
        注意：如果指定了output_Raster参数，则会输出到该参数指定的栅格中。
        '''
        if not inRaster or not background:
            print('ERROR: raster or background does not exist. Please check the input.')

            # logger output
            self.ES_logger.error(
                'Input raster or background does not exist. inRaster:{}; background:{}.'.format(inRaster, background))
            exit(1)

        if output_Raster:
            try:
                temp_output_path = str(output_Raster)
            except:
                print('ERROR: output raster name does not exist.')

                # logger output
                self.ES_logger.error('output raster name does not exist.')
                exit(1)

            # 确定输出栅格的pixel_type
            temp_pixel_type = self.__raster_pixel_type[arcpy.Raster(inRaster).pixelType]

            # Mosaic 所有背景值栅格和原始数据栅格到新栅格中
            arcpy.MosaicToNewRaster_management(
                input_rasters=[inRaster, background],
                output_location=self.__workspace,
                raster_dataset_name_with_extension=output_Raster,
                pixel_type=temp_pixel_type,
                number_of_bands=1,
                mosaic_method="FIRST",
                mosaic_colormap_mode="FIRST")
            print('Raster {} added background.'.format(temp_output_path))
            # logger output
            self.ES_logger.debug('New raster was mosaiced with background: {}'.format(temp_output_path))
        else:
            temp_inputs = '{};{}'.format(inRaster,background)
            arcpy.Mosaic_management(
                temp_inputs,
                target=inRaster,
                mosaic_type="FIRST",
                colormap="FIRST",
                mosaicking_tolerance=0.5)

            print('Raster {} added background.'.format(inRaster ))
            # logger output
            self.ES_logger.debug('Background was mosaiced into raster: {}'.format(inRaster))

    # 删除临时生成的图层文件
    def delete_temporary_feature_classes(self, feature_list):
        '''
        删除生成的临时要素类文件。当然，也可以用作于批量删除输入列表中的要素数据文件。
        '''
        print('Deleting temporary files')

        prepare_feature = [
            s for s in feature_list if arcpy.ListFeatureClasses(wild_card=s, feature_type=Point)
        ]

        for f in tqdm(prepare_feature):
            # 这里可能涉及一个arcpy的BUG。在独立脚本中使用删除图层工具时
            # 需要提供完整路径，即使你已经设置了env.workspace。
            # 而且在删除的时候不能使用deletefeature！
            # 需要使用delete_management.
            feature_fullpath = os.path.join(self.__workspace, f)
            arcpy.Delete_management(feature_fullpath)

            # logger output
            self.ES_logger.debug('Deleted feature:{}'.format(f))

        print('Deleting temporary files finished!')

    # 删除临时生成的图层文件
    def delete_temporary_raster(self, raster_list):
        '''
        删除生成的临时栅格文件。当然，也可以用作于批量删除输入列表中的栅格数据文件。
        '''
        print('Deleting temporary files')

        prepare_feature = self.do_arcpy_list_raster_list(wildcard_list=raster_list, wildcard_mode=False)

        for f in tqdm(prepare_feature):
            # 这里可能涉及一个arcpy的BUG。在独立脚本中使用删除图层工具时
            # 需要提供完整路径，即使你已经设置了env.workspace。
            # 而且在删除的时候不能使用deletefeature！
            # 需要使用delete_management.
            feature_fullpath = os.path.join(self.__workspace, f)
            arcpy.Delete_management(feature_fullpath)

            # logger output
            self.ES_logger.debug('Deleted feature:{}'.format(f))

        print('Deleting temporary files finished!')

    # 以下三个函数为神奇的、不知道为什么要写的函数、但是既然已经写了就不删除了
    # 为栅格添加背景值
    # 用途：添加一个背景值以保持栅格数据历史范围稳定。
    # 注意：如果指定了output_Raster参数，则会输出到该参数指定的栅格中。
    # 注意：
    #       函数可以通过两种方式完成添加背景：
    #       1、如果背景栅格通过nodata值和其他值区分，则直接为数据添加背景。使用该方法时，无需设置`null_background`和`background_value`两个参数。
    #       2、如果不是通过nodata进行区分，则需要在传入参数中指定背景值。使用该方法时，需设置`null_background`参数为False，并在`background_value`参数中提供要作为背景的值。
    def mosaic_background_to_raster_switcher(self, inRaster, background, null_background=True, background_value=None, output_Raster=None):
        '''
        用途：添加一个背景值以保持栅格数据历史范围稳定。
        注意：如果指定了output_Raster参数，则会输出到该参数指定的栅格中。
        注意：
           函数可以通过两种方式完成添加背景：
           1、如果背景栅格通过nodata值和其他值区分，则直接为数据添加背景。使用该方法时，无需设置`null_background`和`background_value`两个参数。
           2、如果不是通过nodata进行区分，则需要在传入参数中指定背景值。使用该方法时，需设置`null_background`参数为False，并在`background_value`参数中提供要作为背景的值。
        '''
        if not inRaster or not background:
            print('ERROR: raster or background does not exist. Please check the input.')

            # logger output
            self.ES_logger.error(
                'Input raster or background does not exist. inRaster:{}; background:{}.'.format(inRaster, background))
            exit(1)

        if null_background:
            self.do_mosaic_null_background_to_raster(inRaster=inRaster,
                                                    background=background,
                                                    output_Raster=output_Raster)
        else:
            self.do_mosaic_value_background_to_raster(inRaster=inRaster,
                                                    background=background,
                                                    background_value=background_value,
                                                    output_Raster=output_Raster)

    # 以下为神奇的、不知道为什么要写的函数
    def do_mosaic_null_background_to_raster(self, inRaster, background, output_Raster=None):
        '''
        用途：添加一个背景值以保持栅格数据历史范围稳定。
        注意：如果指定了output_Raster参数，则会输出到该参数指定的栅格中。
        注意：
           函数可以通过两种方式完成添加背景：
           1、如果背景栅格通过nodata值和其他值区分，则直接为数据添加背景。使用该方法时，无需设置`null_background`和`background_value`两个参数。
           2、如果不是通过nodata进行区分，则需要在传入参数中指定背景值。使用该方法时，需设置`null_background`参数为False，并在`background_value`参数中提供要作为背景的值。
        '''
        if output_Raster:
            if type(output_Raster) != str:
                print('ERROR: output raster name does not exist.')

                # logger output
                self.ES_logger.error('output raster name does not exist.')

            # 确定输出栅格的pixel_type
            temp_pixel_type = self.__raster_pixel_type[arcpy.Raster(inRaster).pixelType]

            # Mosaic 所有背景值栅格和原始数据栅格到新栅格中
            arcpy.MosaicToNewRaster_management(
                input_rasters=[inRaster, background],
                output_location=self.__workspace,
                raster_dataset_name_with_extension=output_Raster,
                pixel_type=temp_pixel_type,
                number_of_bands=1,
                mosaic_method="FIRST",
                mosaic_colormap_mode="FIRST")
            print('Raster {} added background.'.format(output_Raster))
            # logger output
            self.ES_logger.debug('New raster was mosaiced with background: {}'.format(output_Raster))
        else:
            arcpy.Mosaic_management(
                inputs=[inRaster, background],
                target=inRaster,
                mosaic_type="FIRST",
                colormap="FIRST",
                mosaicking_tolerance=0.5)

            print('Raster {} added background.'.format(inRaster))
            # logger output
            self.ES_logger.debug('Background was mosaiced into raster: {}'.format(inRaster))

    # 以下为神奇的、不知道为什么要写的函数
    # 注意以下这个方法可能会消耗大量的时间！
    # 请谨慎使用！
    # 这个方法有极大可能造成程序假死。
    # 相比于上一个null方法，这个方法可能会多处至少4*N的时间消耗。
    def do_mosaic_value_background_to_raster(self, inRaster, background, background_value, output_Raster=None):
        '''
        用途：添加一个背景值以保持栅格数据历史范围稳定。
        注意：如果指定了output_Raster参数，则会输出到该参数指定的栅格中。
        注意：
           函数可以通过两种方式完成添加背景：
           1、如果背景栅格通过nodata值和其他值区分，则直接为数据添加背景。使用该方法时，无需设置`null_background`和`background_value`两个参数。
           2、如果不是通过nodata进行区分，则需要在传入参数中指定背景值。使用该方法时，需设置`null_background`参数为False，并在`background_value`参数中提供要作为背景的值。
        注意以下这个方法可能会消耗大量的时间！
        请谨慎使用！
        这个方法有极大可能造成程序假死。
        '''
        if output_Raster:
            if type(output_Raster) != str:
                print('ERROR: output raster name does not exist.')

                # logger output
                self.ES_logger.error('output raster name does not exist.')

            # 确定输出栅格的pixel_type
            temp_pixel_type = self.__raster_pixel_type[arcpy.Raster(inRaster).pixelType]

            # Mosaic 所有背景值栅格和原始数据栅格到新栅格中
            arcpy.MosaicToNewRaster_management(
                input_rasters=[inRaster, background],
                output_location=self.__workspace,
                raster_dataset_name_with_extension=output_Raster,
                pixel_type=temp_pixel_type,
                number_of_bands=1,
                mosaic_method="FIRST",
                mosaic_colormap_mode="FIRST")
            print('Raster {} added background.'.format(output_Raster))
            # logger output
            self.ES_logger.debug('New raster was mosaiced with background: {}'.format(output_Raster))
        else:
            arcpy.Mosaic_management(
                inputs=[inRaster, background],
                target=inRaster,
                mosaic_type="FIRST",
                colormap="FIRST",
                mosaicking_tolerance=0.5)

            print('Raster {} added background.'.format(inRaster))
            # logger output
            self.ES_logger.debug('Background was mosaiced into raster: {}'.format(inRaster) )

    # 以下为神奇的、不知道为什么要写的函数
    # 注意以下这个方法可能会消耗大量的时间！
    # 请谨慎使用！
    # 这个方法有极大可能造成程序假死。
    # 相比于上一个null方法，这个方法可能会多处至少4*N的时间消耗。
    def do_mosaic_value_background_to_raster(self, inRaster, background, background_value, output_Raster=None):
        '''
        注意以下这个方法可能会消耗大量的时间！
        请谨慎使用！
        这个方法有极大可能造成程序假死。
        相比于上一个null方法，这个方法可能会多处至少4*N的时间消耗。
        '''
        if not isinstance(background_value, numbers.Number):
            print('ERROR: backgound value must be a number.')

            # logger output
            self.ES_logger.error('background value type error.')
            exit(1)

        # 从输入背景栅格生成不影响原始数据的背景值,
        # 即是获得一个背景值区域为0，其他区域为nodata的栅格。
        temp_setnull_where_clause = 'VALUE <> {}'.format(background_value)
        newBackground = SetNull(in_conditional_raster=background,
                                in_false_raster_or_constant=0,
                                where_clause=temp_setnull_where_clause)        

        # logger output
        self.ES_logger.debug('New background raster was built: {}'.format(newBackground) )

        # 因为考虑到保护原始数据和运行效率的原因，从这里开始就要分是否将结果保存到新栅格的问题：
        #   1、首先是保护原始数据的问题。如果不需要保存到新栅格，即意味着所有改动都发生在原始的数据栅格上，所以可以对原始数据进行操作。相反，如果要保存到新栅格即意味着使用者并不期望原始数据被修改，所以不能对原始数据进行操作。
        #   2、考虑到运行效率问题。保存到新栅格的操作比在原始数据上操作需要经历更多的步骤，且二者的很多操作是相互独立的，在并不通用的操作上都执行一遍会造成大量的时间浪费。
        if output_Raster:
            if type(output_Raster) != str:
                print('ERROR: output raster name does not exist.')

                # logger output
                self.ES_logger.error('output raster name does not exist.')

            # 确定输出栅格的pixel_type
            temp_pixel_type = self.__raster_pixel_type[arcpy.Raster(inRaster).pixelType]

            # 保存结果的临时栅格
            temp_result_raster = output_Raster

            # Mosaic 原始数据和新背景
            arcpy.MosaicToNewRaster_management(
                input_rasters=[inRaster, newBackground],
                output_location=self.__workspace,
                raster_dataset_name_with_extension=temp_result_raster,
                pixel_type=temp_pixel_type,
                number_of_bands=1,
                mosaic_method="FIRST",
                mosaic_colormap_mode="FIRST")

            # logger output
            self.ES_logger.debug('Raster was mosaiced with new background: {}'.format(temp_result_raster) )
        else:
            # 保存结果的临时栅格
            temp_result_raster = inRaster

            arcpy.Mosaic_management(
                inputs=[temp_result_raster, newBackground],
                target=temp_result_raster,
                mosaic_type="FIRST",
                colormap="FIRST",
                mosaicking_tolerance=0.5)

            # logger output
            self.ES_logger.debug('Raster was mosaiced with new background: {}'.format(temp_result_raster) )

        # 在新栅格中恢复背景值
        temp_result_raster = Con(temp_result_raster, temp_result_raster, 'VALUE = 0')
        
        # 补充原始背景中的其他像素信息，得到最终结果
        arcpy.Mosaic_management(inputs=[temp_result_raster, background],
                                target=temp_result_raster,
                                mosaic_type="FIRST",
                                colormap="FIRST",
                                mosaicking_tolerance=0.5)

        print('Raster {} added background.'.format(temp_result_raster) )
        # logger output
        self.ES_logger.debug('Raster was mosaiced with new background: {}'.format(temp_result_raster) )

    ############################################################################
    ############################################################################
    # EDGAR 原始数据合并为点数据部分
    ############################################################################
    ############################################################################

    ############################################################################
    # EDGAR 原始数据合并参数设定
    ############################################################################
    # 想要自定义或者修改处理的部门排放需要使用特殊的property函数
    @property
    def sectors_handle(self):
        return self.EDGAR_sectors

    @sectors_handle.setter
    def sectors_handle(self, sectors):
        if type(sectors) != dict:
            print('Error type! EDGAR sectors should be dictionary!')
            self.ES_logger.error('Error type! EDGAR sectors should be dictionary.')
            exit(1)

        self.EDGAR_sectors = sectors

        # logger output
        self.ES_logger.debug('EDGAR_sectors changed to:{}'.format(sectors) )

    # 想要自定义或者修改处理的部门对应栅格值需要使用特殊的property函数
    @property
    def sectors_colormap_handle(self):
        print(self.EDGAR_sectors_colormap)

    @sectors_colormap_handle.setter
    def sectors_colormap_handle(self, sectors_colormap):
        if type(sectors_colormap) != dict:
            print('Error type! EDGAR sectors colormap should be dictionary!')
            self.ES_logger.error('Error type! EDGAR sectors colormap should be dictionary.')
            exit(1)

        self.EDGAR_sectors_colormap = sectors_colormap

        # logger output
        self.ES_logger.debug('EDGAR_sectors_colormap changed to:{}'.format(sectors_colormap) )

    # 栅格图像背景值设置和查看属性/函数
    @property
    def background(self):
        # 这里直接返回一个元组，包括背景栅格的三个信息，开启，标签，空白栅格名称
        return (self.background_flag, self.background_label, self.background_raster)

    @background.setter
    def background(self, flag_label_raster_dict):
        # 检查flag参数并赋值
        # 关闭background，即栅格不包含背景0值
        if bool(flag_label_raster_dict['flag']) == False:
            try:
                self.background_flag = bool(flag_label_raster_dict['flag'])
                self.background_label = ''
                self.background_raster = ''
                print('Background value flag closed!')

                # logger output
                self.ES_logger.debug('Background closed.')
            except:
                print('Background flag set failed! Please check the flag argument input.')
                self.ES_logger.error(
                    'Background flag set failed! Please check the flag argument input.')
        # 开启background，即栅格包含背景0值
        elif bool(flag_label_raster_dict['flag']) == True:
            # 检查flag参数并赋值
            try:
                self.background_flag = bool(flag_label_raster_dict['flag'])

                # logger output
                self.ES_logger.debug('Background opened.')
            except:
                print('Background flag set failed! Please check the flag argument input.')
                self.ES_logger.error(
                    'Background flag set failed! Please check the flag argument input.')
                exit(1)

            # 检查flag_label参数并赋值
            if type(flag_label_raster_dict['label']) == str:
                self.background_label = flag_label_raster_dict['label']

                # logger output
                self.ES_logger.debug('Background label changed to:{}'.format(flag_label_raster_dict['label']))
            else:
                print('Background flag label set failed! Please check the flag argument input.')
                self.ES_logger.error(
                    'Background flag set failed! Please check the flag argument input.')
                exit(1)

            # 检查raster参数并赋值
            if type(flag_label_raster_dict['raster']) == str:
                if arcpy.Exists(flag_label_raster_dict['raster']):
                    self.background_raster = flag_label_raster_dict['raster']

                    # logger output
                    self.ES_logger.debug('Background label changed to:{}'.format(flag_label_raster_dict['raster']))
                else:
                    print('Background raster set failed! The background raster dose not exits.')
                    self.ES_logger.error(
                        'Background raster set failed! The background raster dose not exits.')
                    exit(1)
            else:
                print('Background flag label set failed! Please check the flag argument input.')
                self.ES_logger.error(
                    'Background flag set failed! Please check the flag argument input.')
                exit(1)

    # filter_label 构造方法：
    # filter_label字典组的结构如下：
    # 'default':接受一个符合布尔型数据的值，其中True表示使用默认方式构造筛选条件；
    # 'label':该元素是能够筛选出需要栅格的筛选条件，示例：1、符合Arcgis标准的wild_card；
    #          2、所需栅格的文件名；
    #          3、采用默认方式构造的label字典结构。
    #
    # 注意！！！：
    # 如果使用默认方式构造筛选条件，则label参数应该包含由以下标签构成的字典：
    # 'background_label'：可以用来筛选代表包含背景栅格的标签字符串
    # 'sectors'：部门标签列表list或者str
    # 'start_year'：起始年份
    # 'end_year'：结束年份
    @property
    def filter_label(self):
        return self.filter_label_dict

    @filter_label.setter
    def filter_label(self, filter_label):
        # 检查default set，区分是否使用默认方式构造
        if bool(filter_label['default']) == True:
            # 赋值'default'标签内容
            self.filter_label_dict['default'] = True
            # logger output
            self.ES_logger.debug('filter label will set by default.')

            # 初始化'label'字典内容
            self.filter_label_dict['label'] = {}
            # 检查background label 并赋值
            if type(filter_label['background_label']) == str:
                if filter_label['background_label'] == '':
                    self.filter_label_dict['label']['background_label'] = ''

                    # logger output
                    self.ES_logger.info('filter label will NOT contain background value.')
                else:
                    self.filter_label_dict['label']['background_label'] = filter_label[
                        'background_label']

                    # logger output
                    self.ES_logger.debug('filter label will contain background value.')
            else:
                print('background label set error. Please check background_label_set argument.')
                self.ES_logger.error(
                    'background label set error. The background_label_set need a dict type input. More information please refer the project readme.md files.'
                )

            # 检查sectors并赋值
            # 检查sectors是否为str或者dict
            if (type(filter_label['sectors']) == str) or (type(filter_label['sectors']) == dict):
                self.filter_label_dict['label']['sectors'] = filter_label['sectors']

                # logger output
                self.ES_logger.debug('filter_label changed to:{}'.format(filter_label['sectors']))
            else:
                print('filter_label: sectors setting error! sectors only accept string or dictionary type.')
                self.ES_logger.error(
                    'filter label set error. The filter_label need a dict or a list type input. More information please refer the project readme.md files.'
                )

            # 检查年份设置并赋值
            # 检查start_year 和 end_year
            if (type(filter_label['start_year']) != int) or (type(filter_label['end_year']) != int):
                print('filter_label: year setting error! please check year arguments')
                self.ES_logger.error(
                    'year error. The star year and end year must be integer. More information please refer the project readme.md files.'
                )
                exit(1)
            else:
                self.filter_label_dict['label']['start_year'] = filter_label['start_year']
                self.filter_label_dict['label']['end_year'] = filter_label['end_year']

                # logger output
                self.ES_logger.debug('filter_label year range changed to:{} to {}'.format(filter_label['start_year'], filter_label['end_year']))
        elif bool(filter_label['default']) == False:
            # logger output
            self.ES_logger.debug('filter label will set by costume.')
            # 赋值'default'标签内容
            self.filter_label_dict['default'] = False
            # 赋值'label'标签内容
            self.filter_label_dict['label'] = filter_label['label']
        else:
            print('default set error. Please check default_set argument.')
            self.ES_logger.error('default set error. default_set argument need a bool type input.')

    # 注意：这里需要为set函数传入一个filter_label字典
    @property
    def raster_filter(self):
        return self.raster_filter_wildcard

    @raster_filter.setter
    def raster_filter(self, filter_label):
        # 判断是否为默认标签，是则调用默认的构造
        if filter_label['default'] == True:
            # 这里使用python的**kwags特性，**操作符解包字典并提取字典的值。
            self.build_raster_filter_default(**filter_label['label'])

            # logger output
            self.ES_logger.debug('filter_label changed by default function.')
        # 判断是否为默认标签，否则直接赋值为标签数据
        elif filter_label['default'] == False:
            self.build_raster_filter_costume(filter_label['label'])

            # logger output
            self.ES_logger.debug('filter_label changed by costume function.')
        else:
            print('Error: raster filter arguments error.')

    # 类中提供了两个过滤标签的构造方法
    # 1. 本人生成的数据保存的格式，例如：‘BA_EDGAR_TNR_Aviation_CDS_2010’，其中‘BA’代表包含背景值，数据名结尾
    #    字符串为‘部门_年份’。
    # 2. 自定义标签格式。可以根据用户已有的数据的名称进行筛选。请注意：筛选字符串需要符合 Arcpy 中 wild_card定义的标准进行设定。
    def build_raster_filter_default(self, background_label, sectors, start_year, end_year):
        # 检查年份设定是否为整数。（其他参数可以暂时忽略，因为默认格式下基本不会改变）
        if (type(start_year) != int) or (type(end_year) != int):
            print('Error: Year setting error!')
            self.ES_logger.error(
                'Year setting error. Year settings must be integer and between 1970 to 2018.')
            exit(1)

        temp_time_range = range(start_year, end_year + 1)

        # 这里使用了python列表解析的方法来生成部门和年份逐一配的元组。
        # 生成的元组个数应该为‘部门数量’*‘年份数量’
        # 注意！！！
        # 这里生成的列表中的元素是元组，该元组中包含[0]号元素为部门，[1]号元素为年份
        temp_sectors_year_tuple_list = [(se, yr) for se in sectors for yr in temp_time_range]

        # 逐年逐部门生成筛选条件语句，并保存到raster_filter_wildcard中
        for i in temp_sectors_year_tuple_list:
            temp_raster_filter_wildcard = '{}*{}_{}'.format(background_label, i[0], i[1])
            self.raster_filter_wildcard.append(temp_raster_filter_wildcard),

        # logger output
        self.ES_logger.debug('raster_filter set by default.')

    def build_raster_filter_costume(self, custom_label):
        # 对于自定义筛选条件，只需要检查是否为字符串
        if type(custom_label) != str:
            print("arcpy.ListRasters() need a string for 'wild_card'.")
            self.ES_logger.error(
                'Wild_card set failed. The wild_card string must follow the standard of arcgis wild_card rules.'
            )
            exit(1)
        self.raster_filter_wildcard = custom_label

        # logger output
        self.ES_logger.debug('raster_filter set by costume.')

    # 生成需要处理数据列表
    # 这里需要注意第三个参数`wildcard_mode`：
    #       当该参数为True时，将认为wildcard_list中的元素为arcgis默认的wildcard，即查询条件，此时将
    #           会在使用时替换wildcard中的`*`为正则表达式的`.*`模式。
    #       当该参数为False时，将认为wildcard_list中的元素为正则表达式，此时将直接用该元素进行匹配。
    def prepare_working_rasters(self, raster_filter_wildcard, wildcard_mode=True):
        # 涉及arcpy操作，且所有数据都基于这步筛选，
        # 所以需要进行大量的数据检查。
        temp_type_check = type(raster_filter_wildcard)

        # 传入列表情况
        if temp_type_check == list:
            # 列表为空
            if raster_filter_wildcard == []:
                # 显示警告：这个操作会列出数据库中的所有栅格
                print('WARNING: No filter! All rasters will be list!')

                # 使用str方式列出所有栅格
                self.do_prepare_arcpy_list_raster_str(wildcard_str='')

                # logger output
                self.ES_logger.debug('rasters listed without filter.')
            # 列表不为空的情况
            else:
                # 直接将参数传入list方式的方法列出需要栅格
                self.do_arcpy_list_raster_list(wildcard_list=raster_filter_wildcard, wildcard_mode=wildcard_mode)

                # logger output
                self.ES_logger.debug('rasters listed.')
        # 传入单一字符串情况
        elif temp_type_check == str:
            # 显示筛选列表警告:
            # 如果为空值则警告可能会对所有栅格进行操作：
            if raster_filter_wildcard == '':
                print('WARNING: No filter! All rasters will be list!')

            self.do_prepare_arcpy_list_raster_str(wildcard_str=raster_filter_wildcard)

            # logger output
            self.ES_logger.debug('rasters listed without filter.')
        else:
            # 其他情况直接退出
            print('Error: No filter! Please check input raster filter!')
            self.ES_logger.error('No filter! Please check input raster filter!')
            exit(1)

    # 准备栅格时实际执行列出栅格的方法，这个为str方式
    def do_prepare_arcpy_list_raster_str(self, wildcard_str):
        self.all_prepare_working_rasters.extend(arcpy.ListRasters(wild_card=wildcard_str))

        # logger output
        self.ES_logger.debug('working rasters changed to:{}'.format(self.all_prepare_working_rasters)) 

    # 准备栅格时实际执行列出栅格的方法，这个为list方式
    # 这里需要注意第三个参数`wildcard_mode`：
    #       当该参数为True时，将认为wildcard_list中的元素为arcgis默认的wildcard，即查询条件，此时将
    #           会在使用时替换wildcard中的`*`为正则表达式的`.*`模式。
    #       当该参数为False时，将认为wildcard_list中的元素为正则表达式，此时将直接用该元素进行匹配。
    def deprecated_do_prepare_arcpy_list_raster_list(self, wildcard_list, wildcard_mode=True):
        # # 列出所有数据库中的栅格进行匹配
        # temp_all_rasters_in_path = arcpy.ListRasters()

        # # 通过正则表达在列表中搜索的方式筛选要进行操作的栅格
        # for wildcard in wildcard_list:
        #     # 检查wildcard_mode并指定合适的正则表达式编译模式
        #     if wildcard_mode:
        #         # 替换wildcard中的通配符*为正则表达的通配符‘.’
        #         # 然后构造正则表达匹配模式
        #         temp_wildcard_re = re.compile(wildcard.replace('*','.*'))
        #     else:
        #         # 直接使用wildcard_list中的元素构造正则表达式匹配模式
        #         temp_wildcard_re = re.compile(wildcard)

        #     # filter函数会返回一个列表，所以这里要用extend()方法
        #     self.all_prepare_working_rasters.extend(filter(temp_wildcard_re.match, temp_all_rasters_in_path))
        #     # 直接的对逐个项使用ListRasters()方法可能会消耗大量的时间，导致程序假死
        #     # 放弃使用以下方法！
        #     # self.all_prepare_working_rasters.extend(arcpy.ListRasters(wild_card=i))
        # # logger output
        # self.ES_logger.debug('working rasters changed to:{}'.format(self.all_prepare_working_rasters)) 
        pass

    # 实际执行列出栅格的方法，这个为str方式
    def do_arcpy_list_raster_str(self, wildcard_str):
        temp_working_rasters = []
        temp_working_rasters.extend(arcpy.ListRasters(wild_card=wildcard_str))

        # logger output
        self.ES_logger.debug('working rasters changed to:{}'.format(temp_working_rasters))
        return temp_working_rasters

    # 实际执行列出栅格的方法，这个为list方式
    def do_arcpy_list_raster_list(self, wildcard_list, wildcard_mode=True):
        # 临时存储列出的栅格
        temp_result_rasters = []

        # 列出所有数据库中的栅格进行匹配
        temp_all_rasters_in_path = arcpy.ListRasters()

        # 通过正则表达在列表中搜索的方式筛选要进行操作的栅格
        for wildcard in wildcard_list:
            # 检查wildcard_mode并指定合适的正则表达式编译模式
            if wildcard_mode:
                # 替换wildcard中的通配符*为正则表达的通配符‘.’
                # 然后构造正则表达匹配模式
                temp_wildcard_re = re.compile(wildcard.replace('*','.*'))
            else:
                # 直接使用wildcard_list中的元素构造正则表达式匹配模式
                temp_wildcard_re = re.compile(wildcard)

            # filter函数会返回一个列表，所以这里要用extend()方法
            temp_result_rasters.extend(filter(temp_wildcard_re.match, temp_all_rasters_in_path))

        # # 直接的对逐个项使用ListRasters()方法可能会消耗大量的时间，导致程序假死
        # # 放弃使用以下方法！
        # # 逐年份生成需要处理的数据列表
        # for i in wildcard_list:
        #     temp_raster_list = arcpy.ListRasters(wild_card=i)

        #     if temp_raster_list:
        #         self.working_rasters.extend(temp_raster_list)
        #     else:
        #         print 'WARNING: cant add raster to working_rasters list.'

        #         # logger output
        #         self.ES_logger.warning('raster not exists! raster name: %s' % i)

        # logger output
        self.ES_logger.debug('working rasters changed to:{}'.format(temp_result_rasters) )

        # 返回生成的结果
        return temp_result_rasters

    # 将部门key和对应的栅格文件组合为一个字典
    # 注意这个函数只能使用在确定了年份的列表中。
    # 如果暴力使用这个函数返回的字典将没有任何意义。
    def zip_sectors_rasters_to_dict(self, sectors_list, rasters_list):
        # 函数返回的字典
        sectors_rasters_dict = {}

        for s in sectors_list:
            temp_regex = re.compile('{}'.format(s))
            # 注意这里filter函数返回的是一个list，
            # 需要取出其中的值赋值到字典中
            temp_value = filter(temp_regex.search, rasters_list)
            sectors_rasters_dict[s] = temp_value.pop()

        return sectors_rasters_dict

    ############################################################################
    # 实EDGAR 原始数据合并际数据计算相关函数/方法
    ############################################################################
    # 生成需要计算的栅格列表
    def prepare_raster(self, filter_label):
        # 首先构造用于筛选raster用的wildcard
        self.raster_filter = filter_label
        # 通过arcpy列出需要的栅格
        self.prepare_working_rasters(self.raster_filter)

    # 栅格叠加的实际执行函数
    # 改进1：这个函数用到了tqdm显示累加进度
    # 改进2：修改参数的功能，如果提供了result_raster参数则保存到对应位置；
    #       如果不提供则只将生成的栅格返回。
    # 改进3: 对于arcgis一次性计算超过一定数量的栅格可能会触发ERROR:999998。
    #       所以改进为叠加一部分保存一部分，逐次递归得到最后的叠加结果。
    #       （所以，这其实很像二叉树的归并。可惜在这样一个小函数里写一个二叉树有过分了。）
    #       所以，借鉴二叉树归并的思想，采取递归的策略来完成叠加。
    def do_raster_add(self, raster_list, result_raster=None):
        # 用于保存递归中产生的临时栅格的列表
        temp_delete_rasters = []

        # 用于递归计算栅格求和的函数
        def merge_list(inList):
            # 递归终止条件
            if len(inList) == 2:
                temp_terminate_raster_name = 'temp_{}'.format(shortuuid.uuid())
                temp_terminate_raster = arcpy.Raster(inList[0]) + arcpy.Raster(inList[1])
                temp_terminate_raster.save(temp_terminate_raster_name)
                self.delete_temporary_raster(temp_delete_rasters)
                return temp_terminate_raster_name

            # 生成一个唯一的文件名用于储存临时累积结果
            temp_add_raster_name = 'temp_{}'.format( shortuuid.uuid())

            # 选择任意两个列表中的栅格进行叠加
            # 先将选择的两个栅格pop出列表
            temp_add_head = inList.pop()
            temp_add_tail = inList.pop()
            temp_add_raster = arcpy.Raster(temp_add_head) + arcpy.Raster(temp_add_tail)
            temp_add_raster.save(temp_add_raster_name)

            # 将栅格添加到待清理列表
            temp_delete_rasters.append(temp_add_raster_name)
            # 将栅格添加到递归列表
            inList.append(temp_add_raster_name)

            return merge_list(inList)

        if not raster_list:
            print('ERROR: in raster is empty. Please check the input')

            # loggger output
            self.ES_logger.error('in raster is empyt.')
            exit(1)
        
        print('Adding rasters...')
        # 递归执行累加
        temp_raster = merge_list(raster_list)

        # 如果提供了result_raster，则进行栅格保存操作。
        if result_raster:
            if type(result_raster) != str:
                print('Raster add: The output result raster path error.')

                # logger output
                self.ES_logger.error('The output result raster path error.')
                exit(1)

            # 保存生成的栅格
            arcpy.Raster(temp_raster).save(result_raster)

            # logger output
            self.ES_logger.debug('Raster saved: {}'.format(result_raster) )

        return temp_raster

        # 旧代码备份
        # # 将列表中的第一个栅格作为累加的起始栅格
        # temp_raster = arcpy.Raster(raster_list[0])
        # raster_list.pop(0)

        # # 累加剩余栅格
        # for r in tqdm(raster_list):
        #     temp_raster = temp_raster + arcpy.Raster(r)

        #     # logger output
        #     self.ES_logger.debug('Processing raster:%s' % r)

        # # logger output
        # self.ES_logger.info('Rasters added: %s' % raster_list)

        # # 如果提供了result_raster，则进行栅格保存操作。
        # if result_raster:
        #     if type(result_raster) != str:
        #         print 'Raster add: The output result raster path error.'

        #         # logger output
        #         self.ES_logger.error('The output result raster path error.')
        #         return

        #     # 保存生成的栅格
        #     temp_raster.save(result_raster)

        #     # logger output
        #     self.ES_logger.debug('Raster saved: %s' % result_raster)

        # return temp_raster

    # 函数需要传入一个包含需要叠加的部门列表list，
    # 以及执行操作的年份
    def year_sectors_merge(self, rasters_list, merge_sectors, year):
        # 筛选需要计算的部门
        # 这里是通过构造正则表达式的方式来筛选列表中符合的元素
        temp_sectors = ''
        for i in merge_sectors:
            temp_sectors = temp_sectors + '|{}'.format(i)

        temp_sectors = temp_sectors[1:len(temp_sectors)]
        temp_sectors_year = '({}).*{}'.format(temp_sectors, year)
        filter_regex = re.compile(temp_sectors_year)

        # 吐槽：神奇的python语法~~~
        temp_merge_rasters = [s for s in rasters_list if filter_regex.search(s)]

        # logger output
        self.ES_logger.debug('merging rasters: {}'.format(temp_merge_rasters) )

        # 此处输出的总量数据文件名不可更改！！！
        # 未来加入自定义文件名功能
        result_year = 'total_emission_{}'.format(year)

        # 执行栅格数据累加
        self.do_raster_add(temp_merge_rasters, result_year)

        # logger output
        self.ES_logger.info('year_sectors_merge finished!')

    # 实用（暴力）计算全年部门排放总和的函数
    def year_total_sectors_merge(self, year):
        # 列出全部门名称
        temp_sectors = list(self.EDGAR_sectors.values())

        # 执行部门累加
        self.year_sectors_merge(self.all_prepare_working_rasters, temp_sectors, year)

        print('Total emission of {} saved!\n'.format(year) )

        # logger output
        self.ES_logger.info('All sectors merged!')


    ######################################
    ######################################
    # 计算单个部门占年排放总量中的比例
    # 注意！！！
    # 这里的比例定义为：
    #       对每一个栅格：部门排放/该栅格的总量
    ######################################
    ######################################
    def sector_emission_percentage(self, sector, year, output_sector_point):
        # 尝试列出当年总量的栅格
        # 这里要注意，总量栅格的名称在year_sectors_merge()中写死了
        temp_year_total = arcpy.Raster('total_emission_{}'.format(year))
        temp_sector_wildcard = '{}*{}*{}'.format(self.background[1], sector, year)
        temp_sector_emission = arcpy.Raster(arcpy.ListRasters(wild_card=temp_sector_wildcard)[0])

        # 检查输入的部门栅格和总量栅格是否存在，如果不存在则报错并返回
        if not (arcpy.Exists(temp_sector_emission)) or not (arcpy.Exists(temp_year_total)):
            print('Sector_emission_percentage: Error! sector emission or year total emission raster does not exist.')

            # logger output
            self.ES_logger.error('sector emission or year total emission raster does not exist.')
            exit(1)

        # 计算部门排放相对于全体部门总排放的比例
        # 注意！！！
        # 这里涉及除法！0值的背景会被抹去为nodata。所以要再mosaic一个背景上去才能转化为点。
        temp_output_weight_raster = temp_sector_emission / temp_year_total

        # logger output
        self.ES_logger.debug('Sectoral raster weight calculated:{}'.format(sector) )

        # Mosaic 比例计算结果和0值背景
        # Mosaic 的结果仍然保存在temp_output_weight_raster中
        arcpy.Mosaic_management(
            inputs=[temp_output_weight_raster, self.background[2]],
            target=temp_output_weight_raster,
            mosaic_type="FIRST",
            colormap="FIRST",
            mosaicking_tolerance=0.5)

        # logger output
        self.ES_logger.debug('Sectoral raster weight mosaic to 1800*3600.')

        # 保存栅格格式权重计算结果
        temp_output_weight_raster_path = '{}_weight_raster_{}'.format(sector, year)
        temp_output_weight_raster.save(temp_output_weight_raster_path)

        #######################################################################
        #######################################################################
        # 注意！
        # 以下的del操作不能删除！
        # 删除del操作会导致arcpy.RasterToPoint_conversion()出错！
        # 具体表现形式为RasterToPoint会错误的引用一个已经被删除的匿名中间变量。
        # Warning!
        # CAN NOT remove the following `del` operation!
        # Removing the `del` operation will
        # product a error in arcpy.RasterToPoint_conversion().
        # The error will thrall an exception of NOT found table in raster,
        # because of the RasterToPoint_conversion miss include a deleted
        # anonymous temporary variable.
        #######################################################################
        #######################################################################
        del temp_output_weight_raster
        #######################################################################
        #######################################################################

        print('Sector emission weight raster saved: {}\n'.format(sector))

        # logger output
        self.ES_logger.info('Sector emission weight raster saved')

        # 栅格数据转点对象。转为点对象后可以实现计算比例并同时记录对应排放比例的部门名称
        # 这里用到了arcpy.AlterField_management()这个函数可能在10.2版本中没有
        try:
            # transform to point features
            arcpy.RasterToPoint_conversion(temp_output_weight_raster_path, output_sector_point,
                                           'VALUE')

            # logger output
            self.ES_logger.debug('Sector emission weight raster convert to point:{}'.format(sector) )

            # rename value field
            arcpy.AlterField_management(output_sector_point, 'grid_code', new_field_name=sector)
            # 删除表链接结果结果中生成的统计字段'pointid'和'grid_code'
            arcpy.DeleteField_management(output_sector_point, 'pointid')

            # logger output
            self.ES_logger.debug('Sector emission weight point fields cleaned:{}'.format(sector) )

            print('Sector raster convert to point finished: {} of {}'.format(sector, year))
        except:
            print('Failed sector to point : {}'.format(sector) )

            # logger output
            self.ES_logger.error('Raster weight converting to point failed:{}'.format(sector) )

            print(arcpy.GetMessages())

    # 计算一年中所有部门的比例
    def year_sectors_emission_percentage(self, year):
        for s in tqdm(self.EDGAR_sectors):
            # 设定输出点数据的格式
            output = '{}_weight_{}'.format(s, year)
            self.sector_emission_percentage(s, year, output)

    # 实际执行用点提取栅格中数据的函数
    # 这里的NewFieldName应该传入一个元组。
    # 元组中的第一个元素是需要修改的栅格名称，第二个元素是修改后名称。
    def do_ETP(self, ExtractPoint, ValueRaster, outPoint, NewFieldName=None):
        try:
            arcpy.sa.ExtractValuesToPoints(
                in_point_features=ExtractPoint,
                in_raster=ValueRaster,
                out_point_features=outPoint,
                interpolate_values='NONE',
                add_attributes='VALUE_ONLY')
            # logger output
            self.ES_logger.debug('Extract value in {} by {}'.format(ExtractPoint, ValueRaster))

            # 如果需要改名提取后的字段则执行改名操作
            if NewFieldName != None:
                if len(NewFieldName) == 2:
                    # 提取成功以后需要将RASTERVALU字段改为部门名称
                    arcpy.AlterField_management(
                        in_table=outPoint, field=NewFieldName[0], new_field_name=NewFieldName[1])
                # logger output
                self.ES_logger.debug('Rename field {} to {}'.format(NewFieldName[0], NewFieldName[1]))
        except:
            print('Error: Extract value to point failed!')

            # logger output
            self.ES_logger.error('Extract value to point failed!')

            print(arcpy.GetMessages())

    # 将同一年份的部门整合到同一个点数据图层中
    def year_weight_joint(self, year, sectors_list):
        #######################################################################
        #######################################################################
        # 这个函数的设计思想来源于指针和指针的操作。
        # 有若干个变量被当作指针进行使用，要注意这些变量在不同的位置被指向了
        # 不同的实际数据.通过不停的改变他们指向的对象，来完成空间链接的操作。
        # C/C++万岁！！！指针天下第一！！！
        #######################################################################
        #######################################################################

        # 初始化部分参数
        # 设定的保存的文件名的格式
        output_sectoral_weights = 'sectoral_weights_{}'.format(year) 
        # 设定需要删除的临时生成文件列表
        delete_temporary = []

        # 筛选需要计算的部门
        # 列出提取值的栅格
        temp_wildcard_pair = zip(sectors_list, [str(year)] * len(sectors_list))
        temp_wildcard = ['{}_weight_raster_{}'.format(i[0],i[1]) for i in temp_wildcard_pair]
        temp_working_rasters = self.do_arcpy_list_raster_list(wildcard_list=temp_wildcard)
        temp_extract_raster = self.zip_sectors_rasters_to_dict(sectors_list, temp_working_rasters)

        # logger output
        self.ES_logger.debug('Calculate weight in: {}'.format(str(temp_extract_raster)))

        # 首先弹出一个部门作为合并的起始指针
        temp_point_start_wildcard = '{}*{}'.format(temp_extract_raster.popitem()[0], year)
        # 这里的逻辑看似有点奇怪，其实并不奇怪。
        # 因为在sector_emission_percentage()中已经保存了完整的‘部门-年份’点数据
        # 所以可以用其中一个点数据作为提取的起点。
        temp_point_start = arcpy.ListFeatureClasses(
            wild_card=temp_point_start_wildcard, feature_type=Point).pop()

        # logger output
        self.ES_logger.debug('First weight extract point:{}'.format(temp_point_start) )

        # 构造三个特殊变量来完成操作和循环的大和谐~、
        # 因为sa.ExtractValuesToPoint函数需要一个输出表，同时又不能覆盖替换另一个表
        # 所以需要用前两个表生成第一个循环用的表
        # 在程序的结尾用最后（其实可以是任意一个表）来完成年份的输出
        temp_point_trigger = 'ETP_trigger'
        temp_point_iter_root = 'ETP_iter'
        temp_point_output = 'ETP_output'

        delete_temporary.append(temp_point_trigger)
        delete_temporary.append(temp_point_output)

        # 需要先进行一次提取操作，输入到EPT_iter中
        # 这里需要开启覆盖操作，或者执行一个del工作
        temp_ETP_1_sector, temp_ETP_1_raster = temp_extract_raster.popitem()
        self.do_ETP(
            ExtractPoint=temp_point_start,
            ValueRaster=temp_ETP_1_raster,
            outPoint=temp_point_trigger,
            NewFieldName=('RASTERVALU', temp_ETP_1_sector))

        # 逐个处理剩下的部门
        for sect in tqdm(temp_extract_raster):
            # 从字典中获得部门和对应的待提取值栅格
            temp_ETP_raster = temp_extract_raster[sect]
            temp_point_output = temp_point_iter_root + sect

            self.do_ETP(
                ExtractPoint=temp_point_trigger,
                ValueRaster=temp_ETP_raster,
                outPoint=temp_point_output,
                NewFieldName=('RASTERVALU', sect))

            # 交换temp_point_iter和temp_point_output指针
            temp_point_trigger = temp_point_output

            # 添加到删除名单
            delete_temporary.append(temp_point_output)

        # 这里应该加入合并单元格排放量的ETP过程。
        # 保存最后的输出结果
        # 应该用temp_point_output去提取total_emission_xxxx，生成结果。
        temp_total_emission = 'total_emission_{}'.format(year) 
        if arcpy.Exists(temp_total_emission):
            print('Saving sectoral weights and total emission...')
            self.do_ETP(
                ExtractPoint=temp_point_output,
                ValueRaster=temp_total_emission,
                outPoint=output_sectoral_weights,
                NewFieldName=('RASTERVALU', 'grid_total_emission'))

            # logger output
            self.ES_logger.debug('Sectoral weights saved:{}'.format(output_sectoral_weights))
        else:
            print('Saving sectoral weights and total emission failed!')

            # logger output
            self.ES_logger.error(
                'Saving sectoral weights and total emission failed! Please check year total emission input.'
            )
            exit(1)

        print('Sectoral weights finished:{}'.format(year) )

        # 删除临时生成的迭代变量
        self.delete_temporary_feature_classes(delete_temporary)

        # logger output
        self.ES_logger.debug('working_rasters cleaned!')

    # 导出不同年份最大权重栅格
    def max_weight_rasterize(self, year):
        temp_point = 'sectoral_weights_{}'.format(year) 
        save_raster_categories = 'main_emi_{}'.format(year) 
        save_raster_weight = 'main_emi_weight_{}'.format(year)

        # 向point feature中添加列
        # 1.权重最大值 wmax
        # 2.权重最大值名称 wmaxid
        # 3.将权重最大值名称映射为一个整数，方便输出为栅格 wraster
        # 4.统计一个栅格中共计有多少个部门排放
        # 并计算添加字段的值
        temp_new_fields = ['wmax', 'wmaxid', 'wraster', 'sector_counts']
        try:
            # wmax
            arcpy.AddField_management(temp_point, temp_new_fields[0], 'DOUBLE', '#', '#', '#', '#',
                                      'NULLABLE', '#', '#')

            # wmaxid
            arcpy.AddField_management(temp_point, temp_new_fields[1], 'TEXT', '#', '#', '#', '#',
                                      'NULLABLE', '#', '#')

            # wraster
            arcpy.AddField_management(temp_point, temp_new_fields[2], 'SHORT', '#', '#', '#', '#',
                                      'NULLABLE', '#', '#')

            # sector_counts
            arcpy.AddField_management(temp_point, temp_new_fields[3], 'DOUBLE', '#', '#', '#', '#',
                                      'NULLABLE', '#', '#')
            # logger output
            self.ES_logger.debug('Max-Classes fields added:{}'.format(temp_new_fields))
        except:
            print('Add field to point failed in: {}'.format(temp_point) )

            # logger output
            self.ES_logger.error('Add field to point failed in: {}'.format(temp_point) )

            print(arcpy.GetMessages())
            exit(1)

        # 这里的year参数可能需要删除aa
        self.do_sector_max_extract(temp_point, temp_new_fields)

        print('Field calculate finished: {} in wraster'.format(year))
        print('Add and calculate fields finished: {}'.format(temp_point))

        # 用wraster列转栅格
        try:
            arcpy.PointToRaster_conversion(temp_point, 'wraster', save_raster_categories,
                                           'MOST_FREQUENT', '#', '0.1')
            arcpy.PointToRaster_conversion(temp_point, 'wmax', save_raster_weight, 'MOST_FREQUENT',
                                           '#', '0.1')
            print('Create main emission raster: {}'.format(temp_point))

            # logger output
            self.ES_logger.debug('Max-Classes rasterize finished:{}'.format(year))
        except:
            print('Create main emission raster field: {}'.format(temp_point))

            # logger output
            self.ES_logger.error('Max-Classes rasterize failed:{}'.format(year))

            print(arcpy.GetMessages())

    # 用arcpy.da.cursor类进行操作
    # 在一行中同时实现找到最大值，最大值对应的id，最大值对应的colormap
    def do_sector_max_extract(self, sector_points, calculate_fields):
        temp_sector = copy.deepcopy(self.EDGAR_sectors)
        temp_sector_colormap = copy.deepcopy(self.EDGAR_sectors_colormap)
        temp_working_sector = sector_points

        # 构造需要操作的字段
        # 神奇的python赋值解包
        temp_cursor_fileds = [i for i in temp_sector]

        # 注意：
        # 根据arcpy文档给出的说明：
        # UpdateCursor 用于建立对从要素类或表返回的记录的读写访问权限。
        # 返回一组迭代列表。 列表中值的顺序与 field_names 参数指定的字段顺序相符。

        # 按照calculate_fields 参数追加需要进行计算的字段
        # 输出结果的四个字段：最大值、最大值部门、colormap、部门数量
        # 部门字段的数量
        sector_counts = len(temp_sector)
        # 计算字段的数量
        calculate_fields_counts = len(calculate_fields)

        # 添加需要计算的字段到游标提取字段的list中
        temp_cursor_fileds.extend(calculate_fields)

        # 构造游标，开始逐行操作
        with arcpy.da.UpdateCursor(temp_working_sector, temp_cursor_fileds) as cursor:
            for row in tqdm(cursor):
                # 统计栅格中的排放部门数量。
                # 如果没有排放，即排放部门数量为0。
                # 则将排放部门设为空。
                emitted_sectors = len([i for i in row[0:-calculate_fields_counts] if i != 0])

                if emitted_sectors == 0:
                    row[-1] = emitted_sectors
                    row[-2] = 0
                    row[-3] = 'NULL'
                    row[-4] = 0
                # 如果存在排放则找出最大的排放部门
                else:
                    max_weight = max(row[0:-calculate_fields_counts])
                    max_id = temp_cursor_fileds[row.index(max_weight)]
                    max_colormap = temp_sector_colormap[max_id]

                    row[-1] = emitted_sectors
                    row[-2] = max_colormap
                    row[-3] = max_id
                    row[-4] = max_weight

                # 更新行信息
                cursor.updateRow(row)

    # 处理给定年份范围内的工作
    # 批量处理可以使用这个函数
    def proccess_year(self, start_year, end_year):
        # 首先需要列出所有需要使用到的栅格
        temp_filter_label = self.filter_label
        temp_filter_label['label']['start_year'] = start_year
        temp_filter_label['label']['end_year'] = end_year
        self.prepare_raster(filter_label=temp_filter_label)

        # 逐年处理
        for yr in range(start_year, end_year + 1):
            self.print_start_year((yr))
            self.year_total_sectors_merge(yr)
            self.year_sectors_emission_percentage(yr)
            self.year_weight_joint(yr, self.EDGAR_sectors)
            self.max_weight_rasterize(yr)
            self.print_finish_year(yr)

    # 暴力处理所有年份
    def proccess_all_year(self):
        self.proccess_year(start_year=1970, end_year=2018)

    ############################################################################
    ############################################################################
    # 排放峰值和排放中心分析
    ############################################################################
    ############################################################################

    ############################################################################
    # emission_center 类和类相关的操作函数
    ############################################################################

    ############################################################################
    # 类定义
    ############################################################################
    class emission_center(object):
        '''
        Description of emission_center:
            A emission_center contains a ordered dictionary of sets of center peaks, which ascendent indexed by year as dictionary keys.
            User can customize a name to a emission_center, which the `emission_center.center_name` will return the string of center name.

        Structure of center peaks:
            The center peak is a dictionary contains following elements:
                {'peak_max': maximum emission,
                'peak_min': minimum emission,
                'peak_name': the middle point of peak_max value and peak_main value, which is a string that expressed with number characters,
                'year': year}
        '''

        # 初始化函数
        # 创建一个emission_center类必须提供一个名称用于标识类
        def __init__(self, outer_class, center_name='default_center'):
            # 需要检查是否输入EDGAR_spatial类,输入类的作用是保证共享的参数可以获取
            if not outer_class:
                print('ERROR: please input a EDGAR_spatial class.')

                exit(1)

            self.outer_class = outer_class

            # 中心的名字
            self.center_name = center_name

            # 排放量峰值，一个排序字典，用于区别不同年份的排放峰值区域。
            self.center_peaks = {}

            # 用于生成最终列表的暂时缓冲
            self.center_peaks_buffer = {}

        # 将emission_peak转换为center的元素
        def emission_peak_assembler(self, emission_peak):
            if not emission_peak:
                print("Error: emission peak is empty.")

                # logger output
                self.outer_class.ES_logger.error('Emission peak is empty.')
                exit(1)

            # 这里为peak中补充中心名称的信息
            emission_peak['center_name'] = self.center_name

            # 将peak 存入临时字典中，等待最后的排序
            self.center_peaks_buffer[emission_peak['year']] = emission_peak

        # 将临时的emission_peak元素组合成center
        def generate_center(self):
            # 检查center_peaks_buffer是否存在
            # 存在时则将其按年份排序，再组成一个排序字典
            if not self.center_peaks_buffer:
                print('ERROR: center peak is empty, please run emission_center.emission_peak_assembler to add peaks.')

                # logger output
                self.outer_class.ES_logger.error('center peaks is empty.')
                exit(1)
            # 若不存在则直接报错并返回
            else:
                # 重新排序center_peaks
                self.center_peaks = collections.OrderedDict(
                    sorted(self.center_peaks_buffer.items(), key=lambda t: t[0]))

                if self not in self.outer_class.emission_center_list:
                    # 将生成的字典名字添加到外部类的center列表中
                    self.outer_class.emission_center_list.append(self)

        # 不加修改的返回整个中心的数据内容
        def return_center(self):
            if not self.center_peaks:
                print('Center list has not been create in this work.')
                exit(1)
            else:
                return self.center_peaks

        # 返回一个峰值范围是元组的center表达形式
        def return_center_range_style(self):
            temp_peaks = self.return_center()

            for val in temp_peaks.values():
                val['peak_range'] = (val['peak_min'], val['peak_max'])
                del val['peak_min']
                del val['peak_max']

            return temp_peaks

    ############################################################################
    # 操作类的函数
    ############################################################################
    # 返回完整的排放中心数据

    def return_emission_center(self, emission_center):
        # 检查输入的emission_center是否存在，不存在则直接返回
        if not emission_center:
            print('ERROR: emission center does not exist.')

            # logger output
            self.ES_logger.error('input emission center does not exist.')
            exit(1)

        return emission_center.return_center()

    # 删除center中的某个peak
    # 只需要提供年份即可
    def remove_peak(self, emission_center, year):
        # 检查输入的emission_center是否存在，不存在则直接返回
        if not emission_center:
            print('ERROR: emission center does not exist.')

            # logger output
            self.ES_logger.error('input emission center does not exist.')
            exit(1)

        if not year or year > self.end_year or year < self.start_year:
            print('ERROR: removing peak failed, please assign a correct year to index the peak.')

            # logger output
            self.ES_logger.error('Input year is empty.')
            exit(1)

        emission_center.center_peaks.pop(year)

    # 修改某个peak的内容
    def edit_peak(self, emission_center, emission_peak):
        # 检查输入的emission_center是否存在，不存在则直接返回
        if not emission_center:
            print('ERROR: emission center does not exist.')

            # logger output
            self.ES_logger.error('input emission center does not exist.')
            exit(1)

        # 从peak_buffer 中删除待修改数据
        emission_center.center_peaks_buffer.pop(emission_peak['year'])

        # 将新数据添加入assembler
        emission_center.emission_peak_assembler(emission_peak)

        # 更新center的内容
        emission_center.generate_center()

    # 利用排放范围和年份时间构建排放峰值
    def emission_peak(self, emission_peak_range, year):
        # 如果输入年份是单一年份，则直接构建emission_peak并返回
        if type(year) == int:
            return self.do_emission_peak(emission_peak_range=emission_peak_range, year=year)
        # 如果输入年份是一组年份，则逐个构建该组年份中的时间，并返回一个列表
        elif isinstance(year, collections.Iterable):
            temp_peaks_list = []

            for yr in year:
                temp_peaks_list.append(
                    self.do_emission_peak(emission_peak_range=emission_peak_range, year=yr))

            return temp_peaks_list
        else:
            print('ERROR: input year error.')

            # logger output
            self.ES_logger.error('input year error.')
            exit(1)

    # 实际执行构建排放峰值
    def do_emission_peak(self, emission_peak_range, year):
        # 生成排放峰值的相关信息
        if type(emission_peak_range) == tuple:
            if len(emission_peak_range) == 2:
                temp_peak_upper_bound = max(emission_peak_range)
                temp_peak_lower_bound = min(emission_peak_range)
                temp_peak_value = (temp_peak_lower_bound + temp_peak_upper_bound) / 2
                if temp_peak_value < 0:
                    temp_peak = '{:.3f}'.format(temp_peak_value).replace('.','').replace('-','N')
                else:
                    temp_peak = '{:.3f}'.format(temp_peak_value).replace('.','')
            else:
                print("Error: emission peak require maximum and minimum range.")

                # logger output
                self.ES_logger.error('Emission peak range error.')
                exit(1)
        else:
            print("Error: emission peak range require a tuple. Please check the input.")

        # 年份变量检查
        # if year < self.start_year or year > self.end_year:
        #     print("Error: emission peak require a correct year.")

        #     # logger output
        #     self.ES_logger.error('Emission peak year error.')
        #     exit(1)

        # 这里实际上定义了emission_peak的结构。
        return {
            'peak_max': temp_peak_upper_bound,
            'peak_min': temp_peak_lower_bound,
            'peak_name': temp_peak,
            'year': year
        }

    # 创建一个仅包含名称的emission_center实例
    def create_center(self, outer_class, emission_center_name):
        # 检查排放中心的名称是否存在，不存在则直接返回
        if not emission_center_name or type(emission_center_name) != str:
            print('ERROR: center name is empty or not a string')

            # logger output
            self.ES_logger.error('center name type error.')
            exit(1)

        # 创建一个仅包含名称的emission_center实例
        return self.emission_center(outer_class=outer_class, center_name=emission_center_name)

    # 向中心中添加排放峰值数据
    def add_emission_peaks(self, emission_center, peaks_list):
        # 检查输入的emission_center是否存在，不存在则直接返回
        if not emission_center:
            print('ERROR: emission center does not exist.')

            # logger output
            self.ES_logger.error('input emission center does not exist.')
            exit(1)

        # 检查输入的peak_list是否存在，不存在则直接返回
        if not peaks_list:
            print('ERROR: peak list center does not exist.')

            # logger output
            self.ES_logger.error('input peak list does not exist.')
            exit(1)

        # 支持将emission_peak或者由它组成的列表传入类中
        if type(peaks_list) == list:
            for pl in peaks_list:
                emission_center.emission_peak_assembler(pl)
        elif type(peaks_list) == dict:
            emission_center.emission_peak_assembler(peaks_list)
        else:
            print('ERROR: emission peak type error, please run emission_peak function to generate a emission peak or a list of emission peaks.')

            # logger output
            self.ES_logger.error('emission peak type or structure error.')
            exit(1)

        # 添加emission_peak后重新整理emission_center的内容
        emission_center.generate_center()

    # 生成所有排放中心的名字列表
    def return_emission_center_list(self):
        return self.emission_center_list

    # 从json文件中批量生成center
    def create_center_from_json(self, input_json_file):
        '''
        通过json生成对应的中心类。
        json文件的格式和要求：
            1、json文件中的所有内容应该包括在一个对象中，且除了这个对象外不应该包含其他内容。
            2、最顶层的对象中应包含一个或若干个中心对象。
                2.1 这些对象应该以center_name:object的形式保存，其中center_name为中心名称，object为保存emission_center类的相关信息。
                2.2 保存emission_center的object因包含该中心的所有center_peak信息的object.
                2.3 保存center_peak的object需要包含以下几个内容：
                    2.3.1 时间信息：year
                        year为一个array列表，只需要包括时间的起止，不需要逐个列出年份。如果对应center_peak的时间信息只包括一年，
                        则year的array列表的起止时间相同。
                    2.3.1 center_peak的范围：peak_range
                        center_peak为一个array列表，只需要包括center_peak范围的最大值和最小值。
            3、一个json文件的示例：
                {
                    "center_name_1":{
                        "peak_1":{
                            "year":[1970,2018],
                            "peak_range":[5,9]
                        }
                    },
                    "center_name_2":{
                        "peak_1":{
                            "year":[1970,2000],
                            "peak_range":[3,4.5]
                        },
                        "peak_2":{
                            "year":[2001,2018],
                            "peak_range":[3,4]
                        }
                    }
                }
        '''
        # 内置临时函数：
        #   用于转换json中的列表为python tuple
        def temp_list_to_tuple(inList):
            if not inList:
                print('ERROR: input list does not exist. Please check the json file.')

                exit(1)
            
            return (min(inList), max(inList))

        # 内置临时函数：
        #   用于生成emission_peak 所用的年份列表
        def temp_year_list(inList):
            if not inList:
                print('ERROR: input list does not exist. Please check the json file.')

                exit(1)
            
            return range(min(inList), max(inList)+1)

        # create_center_form_json函数功能正式开始
        if not input_json_file:
            print('ERROR: input json file does not exist. json file: \'{}\''.format(input_json_file))

            # logger output
            self.ES_logger.error('input json file does not exist. json file: \'{}\''.format(input_json_file))
            exit(1)
        
        # 尝试解包输入的json文件内容
        with open(input_json_file, "r") as json_file:
            json_centers = json.loads(json_file.read())
        
        # 输出函数运行状态信息
        print('Generating emission centers form json file...')
        # logger output
        self.ES_logger.info('Generating emission centers form json file...')

        # 逐个生成中心
        for jc in json_centers.items():
            # 添加中心名称
            temp_center = self.create_center(outer_class=self, emission_center_name=str(jc[0]))
            # 逐个处理emission_peak信息
            temp_peak_list = []
            for ep in jc[1].items():
                temp_peak_list.extend(self.emission_peak(emission_peak_range=temp_list_to_tuple(ep[1]['peak_range']), year=temp_year_list(ep[1]['year'])))
            
            self.add_emission_peaks(emission_center=temp_center, peaks_list=temp_peak_list)
                
    ############################################################################
    # 排放峰值和排放中心分析计算相关函数/方法
    ############################################################################
    # 这个函数实际执行从一个年份的排放总量数据栅格中提取中心操作
    # 本质上这个函数就是按照一定的排放量范围，在栅格中提取属于这个排放量范围的栅格
    def do_raster_make_center_and_mask(self,
                            emission_center_peak,
                            total_emission_raster,
                            output_center_name='',
                            saveMask=True):
        '''
        这个函数将返回一个元组，这个元组的第一个元素是arcgis raster类型的中心，
            第二个元素是arcgis raster类型的中心掩膜。
        '''
        # 检查是否给出了中心的排放量范围
        if not emission_center_peak:
            print('ERROR: emission center does not exist.')

            # logger output
            self.ES_logger.error('input emission center does not exist.')
            exit(1)

        # 检查输入的栅格是否存在
        if not (arcpy.Exists(total_emission_raster)):
            print('Error: input raster does not exist')

            # logger output
            self.ES_logger.error('input raster not found.')
            exit(1)

        # 将大于上界和小于下界范围的栅格设为nodata
        # Set local variables
        whereClause = "VALUE < {} OR VALUE > {}".format(emission_center_peak['peak_min'],
                                                    emission_center_peak['peak_max'])

        # 利用setnull的结果作为提取中心的mask
        # Execute SetNull
        temp_center = SetNull(total_emission_raster, total_emission_raster, whereClause)

        # 判断是否按默认center命名格式保存提取的center文件名
        if output_center_name=='':
            temp_center_output = 'center_{}_{}'.format(emission_center_peak['peak_name'],
                                                        emission_center_peak['year'])
        else:
            temp_center_output = output_center_name

        # 保存总量中心的raster
        temp_center.save(temp_center_output)

        # 通过saveMask参数
        # 在这里控制保存一个提取结果的mask（掩膜）结果
        if saveMask:
            # 判断是否按默认center命名格式保存提取的center文件名
            if output_center_name=='':
                temp_center_mask_path = 'center_mask_{}_{}'.format(emission_center_peak['peak_name'],
                                                                    emission_center_peak['year'])
            else:
                temp_center_mask_path = '{}_mask'.format(output_center_name)
            
            # 生成中心mask
            temp_center_mask = Con(temp_center, 1, '')
            # 保存中心mask
            temp_center_mask.save(temp_center_mask_path)

            # 返回提取后的中心结果和mask结果
            return (temp_center,temp_center_mask)
        else:
            # 返回提取后的中心结果，并保留mask结果为none
            return (temp_center,None)

    # 提取总排放量、最大排放部门和最大排放部门比例的函数
    def extract_raster_center_basic_info(self, emission_center, isLog):
        # 检查输入的emission_center是否存在，不存在则直接返回
        if not emission_center:
            print('ERROR: emission center does not exist.')

            # logger output
            self.ES_logger.error('input emission center does not exist.')
            exit(1)

        # 从传入参数中获得peaks。
        # 注意：这里的peaks是一个字典
        temp_peaks = emission_center.return_center()

        # 临时变量
        temp_year_list = temp_peaks.keys()
        temp_start_year = temp_year_list[0]
        temp_end_year = temp_year_list[-1]

        # 逐年处理
        for yr in tqdm(range(temp_start_year, temp_end_year + 1)):
            # 生成total_emission
            if bool(isLog) == True:
                temp_total_emission = 'total_emission_{}_log'.format(yr)
            elif bool(isLog) == False:
                temp_total_emission = 'total_emission_{}'.format(yr)
            else:
                print('Error: Please set the isLog flag.')

                # logger output
                self.ES_logger.error('isLog flag check failed.')
                exit(1)

            # 检查输入的栅格是否存在
            # 检查total_emission
            if not (arcpy.Exists(temp_total_emission)):
                print('Error: input total emission raster does not exist')

                # logger output
                self.ES_logger.error('input total emission not found.')
                exit(1)

            # 检查对应年份的主要排放部门栅格
            temp_main_sector = 'main_emi_{}'.format(yr)

            if not (arcpy.Exists(temp_main_sector)):
                print('Error: input total emission raster does not exist')

                # logger output
                self.ES_logger.error('input total emission not found.')
                exit(1)

            # 检查对应年份的主要排放部门权重栅格
            temp_main_sector_weight = 'main_emi_weight_{}'.format(yr)

            if not (arcpy.Exists(temp_main_sector_weight)):
                print('Error: input total emission weight raster does not exist')

                # logger output
                self.ES_logger.error('input total emission not found.')
                exit(1)

            temp_center_peak = temp_peaks[yr]
            temp_center_peak_name = temp_center_peak['peak_name']

            # 提取中心的总排放属性
            # set the output
            temp_center_path = 'center_{}_{}'.format(temp_center_peak_name, yr)

            temp_mask = self.do_raster_make_center_and_mask(
                emission_center_peak=temp_center_peak,
                total_emission_raster=temp_total_emission,
                saveMask=True)[1]

            # 提取中心的最大排放部门类型属性
            temp_center_main_output = 'center_main_sector_{}_{}'.format(temp_center_peak_name, yr)
            temp_center_main = arcpy.Raster(temp_main_sector) * temp_mask
            temp_center_main.save(temp_center_main_output)

            # 提取中心的最大排放部门占比属性
            temp_center_main_weight_output = 'center_main_sector_weight_{}_{}'.format(
                temp_center_peak_name, yr)

            temp_center_main_weight = arcpy.Raster(temp_main_sector_weight) * temp_mask
            temp_center_main_weight.save(temp_center_main_weight_output)

    def extract_point_center(self,
                             emission_center_list,
                             inPoint,
                             log_emission_field,
                             output_field_name,
                             year,
                             log_calculate_flag=False,
                             total_emission_field=None):
        if not emission_center_list or not inPoint or not log_emission_field or not output_field_name or not year:
            print('ERROR: input arguments not exist, please check the inputs.')

            # logger output
            self.ES_logger.error('some input arguments do not exist.')
            exit(1)

        if log_calculate_flag:
            if not total_emission_field:
                print('ERROR: total emission field does not exist, please check the input.')

                # logger output
                self.ES_logger.error('total emission field does not exist.')
                exit(1)

            # 先计算出总排放量的对数值
            # 第一步尝试列出log_emission_field，如果不存在则添加一列
            temp_field_checker = arcpy.ListFields(dataset=inPoint, wild_card=log_emission_field)

            # 如果不存在则添加一列
            if not temp_field_checker:
                temp_gen_fieldList = [{'field_name': log_emission_field, 'field_type': 'DOUBLE'}]

                self.addField_to_inPoint(inPoint=inPoint, genFieldList=temp_gen_fieldList)

            # 构造游标计算log值
            temp_field_names = [total_emission_field, log_emission_field]

            with arcpy.da.UpdateCursor(inPoint, temp_field_names) as cursor:
                for row in tqdm(cursor):
                    # 检查栅格排放值是否为0，为0则直接将所有值赋值为0
                    if row[0] == 0 or not row[0]:
                        # row.setNull(row[1])
                        row[1] = None
                    else:
                        row[1] = numpy.log10(row[0])

                    # 更新行信息
                    cursor.updateRow(row)

            # 调用do_point_center_assign
            self.do_point_center_assign(
                emission_center_list=emission_center_list,
                inPoint=inPoint,
                log_emission_field=log_emission_field,
                output_field_name=output_field_name,
                year=year)
        else:
            self.do_point_center_assign(
                emission_center_list=emission_center_list,
                inPoint=inPoint,
                log_emission_field=log_emission_field,
                output_field_name=output_field_name,
                year=year)

    # 在点数据中为每个点标记所属的中心
    def do_point_center_assign(self, emission_center_list, inPoint, log_emission_field,
                               output_field_name, year):
        # 检查输入是否为空。为空则直接返回。
        if not inPoint or not log_emission_field or not output_field_name or not year:
            print('ERROR: input inPoint or output_field_name is empty. Please check the inputs.')

            # logger output
            self.ES_logger.error('point center assign input arguments were empty.')
            exit(1)

        if emission_center_list == []:
            print('WARNING: emission_center_list is empty.')

            # logger output
            self.ES_logger.error('emission center list is empty.')

        # 这里要生成一个peaks值的列表，用于筛选每一个栅格应该归属于哪个范围。
        # 这里可能要从emission_center 类中重新写一个返回函数，返回一个方便使用的字典。
        temp_peaks = []

        # 取出每个中心中对应年份的peak信息
        for center in emission_center_list:
            temp_peaks.append(center.return_center()[year])

        # 在每个peak中生成interval
        for peak in temp_peaks:
            peak['peak_range'] = interval.Interval(peak['peak_min'], peak['peak_max'])

        # 构造游标需要的列名称
        # 注意：
        # 根据arcpy文档给出的说明：
        # UpdateCursor 用于建立对从要素类或表返回的记录的读写访问权限。
        # 返回一组迭代列表。 列表中值的顺序与 field_names 参数指定的字段顺序相符。
        temp_working_fields = [log_emission_field, output_field_name]

        with arcpy.da.UpdateCursor(inPoint, temp_working_fields) as cursor:
            for row in tqdm(cursor):
                for peak in temp_peaks:
                    # 检查是否属于该中心
                    if row[0] in peak['peak_range']:
                        # 如果属于中心则为名称字段更新中心名字
                        row[1] = peak['center_name']
                        # 更新行信息
                        cursor.updateRow(row)
                        # 跳出查找所属中心的循环
                        break

                    # 如果不属于某个中心则保留中心名称字段为NULL
                    # row.setNull(row[1])
                    row[1] = None
                    # 更新行信息
                    cursor.updateRow(row)

    def extract_point_center_basic_info(self, emission_center, isLog):
        pass

    def extract_raster_categories_center(self, emission_center):
        # 检查输入的emission_center是否存在，不存在则直接返回
        if not emission_center:
            print('ERROR: emission center does not exist.')

            # logger output
            self.ES_logger.error('input emission center does not exist.')
            exit(1)

        # 从传入参数中获得peaks。
        # 注意：这里的peaks是一个字典
        temp_peaks = emission_center.return_center()

        # 临时变量
        temp_year_list = temp_peaks.keys()
        temp_start_year = temp_year_list[0]
        temp_end_year = temp_year_list[-1]

        # 逐年处理
        for yr in tqdm(range(temp_start_year, temp_end_year + 1)):
            temp_center_peak = temp_peaks[yr]
            temp_center_peak_name = temp_center_peak['peak_name']

            # 检查输入的mask栅格是否存在
            temp_center_mask = 'center_mask_{}_{}'.format(temp_center_peak_name, yr)

            if not (arcpy.Exists(temp_center_mask)):
                print('Error: input center mask raster does not exist')

                # logger output
                self.ES_logger.error('input mask raster not found.')
                exit(1)

            # 检查对应年份的待提取的排放分类栅格是否存在
            temp_categories_raster = 'sorted_categories_{}'.format(yr)

            if not (arcpy.Exists(temp_categories_raster)):
                print('Error: input categories raster does not exist')

                # logger output
                self.ES_logger.error('input categories raster not found.')
                exit(1)

            # 提取分类栅格的中心
            temp_categories_center_output = 'sorted_categories_center_{}_{}'.format(
                temp_center_peak_name, yr)

            temp_categories_center = arcpy.Raster(temp_categories_raster) * arcpy.Raster(
                temp_center_mask)
            temp_categories_center.save(temp_categories_center_output)

    # 根据提供的排放中心生成该中心的全部排放的分布
    # 这个函数需要进一步修改：
    #   首先，所有列出的栅格raster都应该是对应年份的总碳排放量栅格！
    def generate_center_geographical_extend(self, emission_center_list,  background_raster):
        if not emission_center_list or not background_raster :
            print('ERROR: input argument is empty, please check the input')

            # logger output
            self.ES_logger.error('raster list is empty')
            exit(1)

        # 传入一个包括中心的列表
        if type(emission_center_list) == list:
            # 逐个处理传入的排放中心
            for emission_center in emission_center_list:
                # 生成输出栅格的文件名
                temp_output_name_fmt = '{}_geographical_extend'.format(emission_center.center_name)

                # 如果看不懂下面的python解包操作，就看注释里的这段代码。
                # If developers were confused about the following unpack list, please read the code block in following comments.
                #
                #     for (year, peak) in emission_center.return_center().items():
                #         temp_raster = 'center_mask_%s_%s' % (peak['peak_name'], year)

                # 列出所有总量的栅格
                temp_raster_list = [
                    'total_emission_{}'.format(year)
                    for year in emission_center.return_center().items()
                ]

                # 执行生成范围的
                self.do_generate_center_geographical_extend(
                    raster_list=temp_raster_list,
                    center=emission_center,
                    output_name_fmt=temp_output_name_fmt)
        else:    # 如果只是传入单一中心，且没有用列表包括该中心
            # 生成输出栅格的文件名
            temp_output_name_fmt = '{}_geographical_extend'.format(emission_center_list.center_name)
            # 列出该中心的所有栅格
            temp_raster_list = [
                'center_mask_{}_{}'.format(peak['peak_name'], year)
                for (year, peak) in emission_center_list.return_center().items()
            ]
            # 调用实际执行的do_generate_extend函数
            self.do_generate_center_geographical_extend(
                raster_list=temp_raster_list,
                center=emission_center_list,
                background_raster=background_raster,
                output_name_fmt=temp_output_name_fmt)

    # 叠加时间序列上所有发生排放的区域得到全部排放的分布
    #   中心的历史范围提取：
    #       1.1 利用碳排放总量，按照中心排放你量值的范围，提取每年的中心空间分布
    #       1.2 Mosaic所有年份总量的历史范围，得到中心的总范围
    # 注意：
    #     该函数传入的raster_list是对应要提取年份的总量栅格
    def do_generate_center_geographical_extend(self,
                                        raster_list,
                                        center,
                                        output_name_fmt='extend_{}'):
        # 检查是否传入总量栅格
        if not raster_list:
            print('ERROR: input total emission raster does not exist, please check the inputs.')

            # logger output
            self.ES_logger.error('input total emission raster does not exist.')
            exit(1)

        # 检查是否传入中心
        if not center:
            print('ERROR: input center does not exist, please check the inputs.')

            # logger output
            self.ES_logger.error('input center does not exist.')
            exit(1)

        # 保存每年中心的分布范围的列表
        temp_year_emission_center_list = []
        for raster in raster_list:
            # 1.1 利用碳排放总量，按照中心排放你量值的范围，提取每年的中心空间分布
            # 确定栅格的年份
            # 通过正则表达式的方式进行
            temp_year_re = r'\d{4}'
            temp_re_search = re.search(temp_year_re, raster)
            # 以下使用了解包操作，如果调试有困难就该写成for...loop
            temp_year = int(raster[temp_re_search.span()[0]:temp_re_search.span()[1]])

            # 检查传入的输出文件名格式是否正确
            # 保存生成的两个结果
            # 生成待统计的栅格名称
            try:
                temp_save_extend = output_name_fmt.format(temp_year,'null')
            except Exception as e:
                print('background formatting failed. Save raster name formatting failed. raster name formate was {}.')

                # logger output
                self.ES_logger.error('save raster name formatting failed. raster name formate was {}.'.format(output_name_fmt))

                exit(1)

            # 直接调用do_raster_make_center_and_mask生成每个年份的中心
            # 根据函数的返回值，只需要元组中第二个返回栅格即可。即其中范围为1值，其余为null的栅格
            temp_center_extend = self.do_raster_make_center_and_mask(emission_center_peak=center.return_center()[temp_year],
                                                total_emission_raster=raster,
                                                output_center_name=temp_save_extend,
                                                saveMask=True)[1]

            # 将生成值添加到待叠加合并列表
            # 注意这里只需要给出栅格的名字而不用给出整个arcpy.Raster对象
            temp_year_emission_center_list.append(temp_center_extend.catalogPath)

        # 1.3 Mosaic所有年份总量的历史范围，得到中心的总范围
        # 初步mosaic栅格的文件名
        temp_mosaic = 'mosaic_' + center.center_name + '_EOF_geographical_extend_null_mask'
        # 确定输出栅格的pixel_type
        temp_pixel_type = '32_BIT_FLOAT'
        # Mosaic 所有年份的排放量区域栅格到新栅格中
        arcpy.MosaicToNewRaster_management(
            input_rasters=temp_year_emission_center_list,
            output_location=self.__workspace,
            raster_dataset_name_with_extension=temp_mosaic,
            pixel_type=temp_pixel_type,
            number_of_bands=1,
            mosaic_method="FIRST",
            mosaic_colormap_mode="FIRST")

        # 将所有1值转换为0值，作为eof的背景使用
        temp_save_extend_con = Con(arcpy.Raster(temp_mosaic), 0.0, '')
        temp_save_extend_con.save(temp_mosaic[7:])

        # logger output
        self.ES_logger.info('geographical extend generated')

        # 删除临时栅格
        self.delete_temporary_raster(temp_year_emission_center_list)
        self.delete_temporary_raster([temp_mosaic])

    # !!! 注意：这个函数已经被废弃，请勿使用！
    # 叠加时间序列上所有发生排放的区域得到全部排放的分布
    # 这个方法会生成两个类型的栅格：
    #   1、输出文件名加后缀`mask`，该栅格中`1`值表示排放范围，`0`值表示其他；
    #   2、输出文件名加后缀`null_mask`，该栅格中`1`值表示排放范围，nodata值表示其他；
    def deprecated_do_generate_center_geographical_extend(self,
                                        raster_list,
                                        background_raster,
                                        output_name_fmt='extend_{}'):
        # 测试生成的文件名是否可用
        # 列出待计算栅格的列表
        temp_working_rasters = self.do_arcpy_list_raster_list(raster_list)

        # 用于保存临时数据的列表
        temp_mosaic_background = []

        # 第一步：为栅格mosaic零值背景
        for raster in tqdm(temp_working_rasters):
            temp_new_mosaic = 'temp_new_mosaic_{}'.format(raster)
            
            self.mosaic_background_to_raster(inRaster=raster, background=background_raster,output_Raster=temp_new_mosaic)

            temp_mosaic_background.append(temp_new_mosaic)

        # 第二步：叠加所有栅格
        temp_extend = self.do_raster_add(temp_mosaic_background)

        # 第三步：栅格非零值赋值为1
        # 这里需要注意栅格计算中输入的栅格参数是str还是arcpy栅格对象。
        # 如果遇到ERROR:999998错误，可以尝试检查输入的对象是否栅格化为arcpy栅格对象。
        temp_extend = Con(arcpy.Raster(temp_extend), 1, 0, "VALUE <> 0")

        # logger output
        self.ES_logger.info('extend set to 1 mask')

        # 第四步：零值设为空值
        temp_null_extend = SetNull(temp_extend, 0, "VALUE = 0")
        temp_null_extend = Con(temp_null_extend == 1, 0, temp_null_extend)
        self.ES_logger.info('extend set to background mask')

        # 保存生成的两个结果
        # 生成待统计的栅格名称
        try:
            temp_save_extend = output_name_fmt.format('mask')
            temp_save_null_extend = output_name_fmt.format('null_mask')
        except Exception as e:
            print('background formatting failed.')

            # logger output
            self.ES_logger.error('save raster name formatting failed. raster name formate was {}.'.format(output_name_fmt))

            exit(1)

        # 执行保存
        temp_extend.save(temp_save_extend)
        temp_null_extend.save(temp_save_null_extend)
        # logger output
        self.ES_logger.info('geographical extend generated')

        # 删除临时栅格
        self.delete_temporary_raster(temp_mosaic_background)

    # 旧函数不建议使用！！！
    # 提取总排放量、最大排放部门和最大排放部门比例的函数
    def deprecated_do_extract_center_area(self, center_range, total_emission_raster, year):
        # 临时变量
        # temp_center_upper_bound = 0
        # temp_center_lower_bound = 0
        # temp_center = 0

        # # 变量检查
        # if type(center_range) == tuple:
        #     if len(center_range) == 2:

        #         temp_center_upper_bound = max(center_range)
        #         temp_center_lower_bound = min(center_range)
        #         temp_center = str(
        #             (temp_center_lower_bound + temp_center_upper_bound) / 2).replace('.', '')
        #     else:

        #         print("Error: center range require a start year and a end year.")

        #         # logger output
        #         self.ES_logger.error('Center range year error.')
        #         return
        # else:
        #     print("Error: center range require a tuple. Please check the input.")

        #     # logger output
        #     self.ES_logger.error('Center range type error.')
        #     return

        # # 检查输入的栅格是否存在
        # # 检查total_emission
        # if not (arcpy.Exists(total_emission_raster)):
        #     print('Error: input total emission raster does not exist')

        #     # logger output
        #     self.ES_logger.error('input total emission not found.')
        #     return

        # # 检查对应年份的主要排放部门栅格
        # temp_main_sector = 'main_emi_{}'.format(year)

        # if not (arcpy.Exists(temp_main_sector)):
        #     print('Error: input total emission raster does not exist')

        #     # logger output
        #     self.ES_logger.error('input total emission not found.')
        #     return

        # # 检查对应年份的主要排放部门权重栅格
        # temp_main_sector_weight = 'main_emi_weight_{}'.format(year)

        # if not (arcpy.Exists(temp_main_sector_weight)):
        #     print('Error: input total emission weight raster does not exist')

        #     # logger output
        #     self.ES_logger.error('input total emission not found.')
        #     return

        # # 将大于上界和小于下界范围的栅格设为nodata
        # # Set local variables
        # whereClause = "VALUE < {} OR VALUE > {}".format(temp_center_lower_bound,
        #                                             temp_center_upper_bound)

        # # Execute SetNull
        # outSetNull = SetNull(total_emission_raster, total_emission_raster, whereClause)

        # # Save the output
        # temp_center_path = 'center_{}_{}'.format(temp_center, year)
        # outSetNull.save(temp_center_path)

        # # 防止BUG删除outSetNull
        # del outSetNull

        # # 生成中心的mask
        # # Execute Con
        # outCon = Con(temp_center_path, 1, '')

        # # Save the outputs
        # temp_center_mask_path = 'center_mask_{}_{}'.format(temp_center, year)
        # outCon.save(temp_center_mask_path)

        # # 防止BUG删除outCon
        # del outCon

        # # 生成中心主要排放部门栅格
        # outMain = arcpy.Raster(temp_center_mask_path) * \
        #     arcpy.Raster(temp_main_sector)

        # # Save the output
        # temp_center_main = 'center_main_sector_{}_{}'.format(temp_center, year)
        # outMain.save(temp_center_main)

        # # 防止BUG删除outCon
        # del outMain

        # # 生成中心主要排放部门比重栅格
        # outMainWeight = arcpy.Raster(temp_center_mask_path) * arcpy.Raster(temp_main_sector_weight)

        # # Save the output
        # temp_center_main_weight = 'center_main_sector_weight_{}_{}'.format(temp_center, year)
        # outMainWeight.save(temp_center_main_weight)

        # del outMainWeight
        pass

    # 旧函数不建议使用！！！
    # 这里要求可以center_range和year_range是一个元组
    def deprecated_extract_center_area(self, center_range, year_range, isLog):
        # 临时变量
        # temp_start_year = self.start_year
        # temp_end_year = self.end_year

        # # 检查输入年份变量参数是否合规
        # if (type(year_range[0]) != int) or (type(year_range[1]) != int):
        #     print('Error! Processing starting year and ending year must be int value')
        #     self.ES_logger.info('Year setting type error.')
        #     self.ES_logger.error('Year setting error!')
        #     return
        # elif min(year_range) < self.__default_start_year or max(
        #         year_range) > self.__default_end_year:
        #     print('Error! Processing year range out of data support! The year must contain in 1970 to 2018')
        #     self.ES_logger.info('Year settings are out of range.')
        #     self.ES_logger.error('Year setting error!')
        #     return
        # else:
        #     temp_start_year, temp_end_year = min(year_range), max(year_range)
        #     self.ES_logger.info('Year has set.')

        # # 列出总排放量栅格
        # # 需要区分栅格中的总量数据是否已经进行了对数换算
        # if bool(isLog) == True:
        #     temp_wild_card = [
        #         'total_emission_{}_log'.format( s ) for s in range(temp_start_year, temp_end_year + 1)
        #     ]
        # elif bool(isLog) == False:
        #     temp_wild_card = [
        #         'total_emission_{}'.format(s)for s in range(temp_start_year, temp_end_year + 1)
        #     ]
        # else:
        #     print('Error: Please set the isLog flag.')

        #     # logger output
        #     self.ES_logger.error('isLog flag check failed.')
        #     return

        # # 列出需要的total emission 栅格
        # temp_working_rasters = self.do_arcpy_list_raster_list(temp_wild_card)

        # # 逐年处理
        # for yr in tqdm(range(temp_start_year, temp_end_year + 1)):
        #     temp_total_emission = [s for s in temp_working_rasters if str(yr) in s].pop()

        #     self.do_extract_center_area(
        #         center_range=center_range, total_emission_raster=temp_total_emission, year=yr)

        pass

    # 这个函数已经被废弃
    def deprecated_do_raster_extract_center_area(self,
                                          emission_center_peak,
                                          extract_raster,
                                          output,
                                          saveMask=False):
        # # 检查输入的emission_center_peak是否存在，不存在则直接返回
        # if not emission_center_peak:
        #     print('ERROR: emission center does not exist.')

        #     # logger output
        #     self.ES_logger.error('input emission center does not exist.')
        #     return

        # # 检查输入的栅格是否存在
        # if not (arcpy.Exists(extract_raster)):
        #     print('Error: input raster does not exist')

        #     # logger output
        #     self.ES_logger.error('input raster not found.')
        #     return

        # # 检查输出路径是否存在
        # if not output or type(output) != str:
        #     print('Error: output path does not exist')

        #     # logger output
        #     self.ES_logger.error('output path does not exist.')
        #     return

        # # 将大于上界和小于下界范围的栅格设为nodata
        # # Set local variables
        # whereClause = "VALUE < {} OR VALUE > {}".format(emission_center_peak['peak_min'],
        #                                             emission_center_peak['peak_max'])

        # # 利用setnull的结果作为提取中心的mask
        # # Execute SetNull
        # temp_SetNull = SetNull(extract_raster, extract_raster, whereClause)

        # temp_mask = Con(temp_SetNull, 1, '')
        # # 通过saveMask参数
        # # 在这里控制保存一个提取结果的mask（掩膜）结果
        # if saveMask:
        #     temp_center_mask_path = 'center_mask_{}_{}'.format(emission_center_peak['peak_name'],
        #                                                    emission_center_peak['year'])
        #     temp_mask.save(temp_center_mask_path)

        # temp_result = temp_mask * extract_raster
        # temp_result.save(output)

        pass

    ############################################################################
    # 统计制表相关功能
    ############################################################################
    # TODO
    # 需要继续改进，加入以下功能：
    # 1、统计要素加入交叉制表，这个要实现为do_xxxxx函数
    # 3、统计函数需要加入统计栅格的自定义表达，以适应不同投影和自定义中心名称。

    # 实际执行TabulateArea
    def do_tabulate_area(self, in_zone_data, zone_field, in_class_data, out_table):
        # 获得输入栅格的像素尺寸
        temp_cell_size = arcpy.Describe(in_class_data).meanCellWidth

        # 执行TabulateArea
        TabulateArea(
            in_zone_data=in_zone_data,
            zone_field=zone_field,
            in_class_data=in_class_data,
            class_field='VALUE',
            out_table=out_table,
            processing_cell_size=temp_cell_size)

    # 实际执行zonal statistic
    def do_zonal_statistic_to_table(self, inZoneData, zoneField, inValueRaster, outTable):
        # Execute ZonalStatisticsAsTable
        # 很奇怪吧，这里用了一个临时值接收ZSAT的返回值。只是因为参考手册就是这么写的。
        outZSaT = ZonalStatisticsAsTable(inZoneData, zoneField, inValueRaster, outTable, "DATA",
                                         "ALL")

        # logger output
        self.ES_logger.debug('Statistics finished.')

    # 实际执行将统计的结果转化为csv输出
    def do_zonal_table_to_csv(self, table, year, outPath):
        temp_table = table

        # --first lets make a list of all of the fields in the table
        fields = arcpy.ListFields(table)
        field_names = [field.name for field in fields]
        # 追加年份在最后一列
        field_names.append('year')

        # 获得输出文件的绝对路径
        temp_outPath = os.path.abspath(outPath)

        with open(temp_outPath, 'wt') as f:
            w = csv.writer(f)
            # --write all field names to the output file
            w.writerow(field_names)

            # --now we make the search cursor that will iterate through the rows of the table
            for row in arcpy.SearchCursor(temp_table):
                field_vals = [row.getValue(field.name) for field in fields]
                field_vals.append(str(year))
                w.writerow(field_vals)
            # del row

        # logger output
        self.ES_logger.debug('Convert {}\'s statistics table to csv file:{}'.format(year, temp_outPath))

    def zonal_center_info_statistics(self,
                                     emission_center,
                                     inZone,
                                     zoneField,
                                     outPath,
                                     inRaster_fmt='center_{}_{}',
                                     do_tabulate_area=False):
        # 获得保存路径
        temp_out_csv_path = os.path.abspath(outPath)

        # 检查输入的分区是否存在
        if not (arcpy.Exists(inZone)):
            print('Error: inZone not found.')

            # logger output
            self.ES_logger.error('inZone does not exist.')

            exit(1)

        # 获取inZoned的投影信息
        inZone_prj = arcpy.Describe(inZone).spatialReference.factoryCode

        if not emission_center:
            print('ERROR: input emission center does not exist.')

            # logger output
            self.ES_logger.error('input emission center does not exist.')
            exit(1)

        for peak in tqdm(emission_center.return_center().values()):
            # 生成待统计的栅格名称
            try:
                temp_inRaster = inRaster_fmt.format(peak['peak_name'], peak['year'])
            except Exception as e:
                # logger output
                self.ES_logger.error(
                    'temp raster name formatting failed. raster name formate was {}.'.format(inRaster_fmt))

                exit(1)

            if not (arcpy.Exists(temp_inRaster)):
                print('Error: inRaster not found. inRaster name: {}'.format(temp_inRaster))

                # logger output
                self.ES_logger.error('inRaster does not exist. inRaster name: {}'.format(temp_inRaster))

                exit(1)

            if arcpy.Describe(temp_inRaster).spatialReference.factoryCode != inZone_prj:
                print('ERROR: inzone and raster spatial reference does not same!')

                # logger output
                self.ES_logger.error('Spatial reference was different.')
                exit(1)

            # 统计zonal_statistic_to_table
            temp_ZST_outTable = 'table_ZST_' + temp_inRaster

            self.do_zonal_statistic_to_table(
                inZoneData=inZone,
                zoneField=zoneField,
                inValueRaster=temp_inRaster,
                outTable=temp_ZST_outTable)

            # logger output
            self.ES_logger.debug('Zonal statistics finished:{}'.format(temp_inRaster))

            temp_ZST_outCsv = os.path.join(temp_out_csv_path, temp_ZST_outTable + '.csv')
            self.do_zonal_table_to_csv(
                table=temp_ZST_outTable, year=peak['year'], outPath=temp_ZST_outCsv)

            # logger output
            self.ES_logger.debug('Zonal statistics convert to csv. Csv name: {}'.format(temp_ZST_outCsv))

            # 统计Tabulate_area
            if do_tabulate_area:
                temp_TA_outTable = 'table_TA_' + temp_inRaster

                self.do_tabulate_area(
                    in_zone_data=inZone,
                    zone_field=zoneField,
                    in_class_data=temp_inRaster,
                    out_table=temp_TA_outTable)

                # logger output
                self.ES_logger.debug('Tabulate area finished:{}'.format(temp_inRaster))

                temp_TA_outCsv = os.path.join(temp_out_csv_path, temp_TA_outTable + '.csv')
                self.do_zonal_table_to_csv(
                    table=temp_TA_outTable, year=peak['year'], outPath=temp_TA_outCsv)

                # logger output
                self.ES_logger.debug('Zonal statistics convert to csv. Csv name: {}'.format(temp_TA_outCsv))

    def zonal_center_merge_info_statistics(self,
                                           emission_center,
                                           inZone,
                                           zoneField,
                                           outPath,
                                           inRaster_fmt='center_merge_{}',
                                           do_tabluate_area=False):
        # 获得保存路径
        temp_out_csv_path = os.path.abspath(outPath)

        # 检查输入的分区是否存在
        if not (arcpy.Exists(inZone)):
            print('Error: inZone not found.')

            # logger output
            self.ES_logger.error('inZone does not exist.')

            exit(1)

        # 获取inZoned的投影信息
        inZone_prj = arcpy.Describe(inZone).spatialReference.factoryCode

        if not emission_center:
            print('ERROR: input emission center does not exist.')

            # logger output
            self.ES_logger.error('input emission center does not exist.')
            exit(1)

        for year in tqdm(emission_center.return_center().keys()):
            # 生成待统计的栅格名称
            try:
                temp_inRaster = inRaster_fmt.format(year)
            except Exception as e:
                # logger output
                self.ES_logger.error(
                    'temp raster name formatting failed. raster name formate was {}.'.format(inRaster_fmt))

                exit(1)

            if not (arcpy.Exists(temp_inRaster)):
                print('Error: inRaster not found. inRaster name: {}'.format(temp_inRaster))

                # logger output
                self.ES_logger.error('inRaster does not exist. inRaster name: {}'.format(temp_inRaster))

                exit(1)

            if arcpy.Describe(temp_inRaster).spatialReference.factoryCode != inZone_prj:
                print('ERROR: inzone and raster spatial reference does not same!')

                # logger output
                self.ES_logger.error('Spatial reference was different.')
                exit(1)

            # 统计zonal_statistic_to_table
            temp_ZST_outTable = 'table_ZST_' + temp_inRaster

            self.do_zonal_statistic_to_table(
                inZoneData=inZone,
                zoneField=zoneField,
                inValueRaster=temp_inRaster,
                outTable=temp_ZST_outTable)

            # logger output
            self.ES_logger.debug('Zonal statistics finished:{}'.format(temp_inRaster))

            temp_ZST_outCsv = os.path.join(temp_out_csv_path, temp_ZST_outTable + '.csv')
            self.do_zonal_table_to_csv(table=temp_ZST_outTable, year=year, outPath=temp_ZST_outCsv)

            # logger output
            self.ES_logger.debug('Zonal statistics convert to csv. Csv name: {}'.format(temp_ZST_outCsv))

            # 统计Tabulate_area
            if do_tabluate_area:
                temp_TA_outTable = 'table_TA_' + temp_inRaster

                self.do_tabulate_area(
                    in_zone_data=inZone,
                    zone_field=zoneField,
                    in_class_data=temp_inRaster,
                    out_table=temp_TA_outTable)

                # logger output
                self.ES_logger.debug('Tabulate area finished:{}'.format(temp_inRaster))

                temp_TA_outCsv = os.path.join(temp_out_csv_path, temp_TA_outTable + '.csv')
                self.do_zonal_table_to_csv(
                    table=temp_TA_outTable, year=year, outPath=temp_TA_outCsv)

                # logger output
                self.ES_logger.debug('Zonal statistics convert to csv. Csv name: {}'.format(temp_TA_outCsv))

    # 这里的year_range和center_range都是一个二元元组
    def zonal_center_basic_info_statistics(self, emission_center, inZone, outPath):
        # 获得保存路径
        temp_out_csv_path = os.path.abspath(outPath)

        # 检查输入的分区是否存在
        if not (arcpy.Exists(inZone)):
            print('Error: inZone not found.')

            # logger output
            self.ES_logger.error('inZone does not exist.')

            exit(1)

        if not emission_center:
            print('ERROR: input emission center does not exist.')

            # logger output
            self.ES_logger.error('input emission center does not exist.')
            exit(1)

        # 逐年处理
        for peak in emission_center.return_center().values():
            # 生成总量中心的栅格名称
            temp_emission_inRaster = 'center_{}_{}'.format(peak['peak_name'], peak['year'])

            # 检查输入的待统计值
            if not (arcpy.Exists(temp_emission_inRaster)):
                print('Error: inRaster not found. inRaster name: {}'.format(temp_emission_inRaster))

                # logger output
                self.ES_logger.error('inRaster does not exist. inRaster name: {}'.format(temp_emission_inRaster))

                exit(1)

            temp_outTable = 'table_' + temp_emission_inRaster

            self.do_zonal_statistic_to_table(
                inZoneData=inZone,
                zoneField='ISO_A3',
                inValueRaster=temp_emission_inRaster,
                outTable=temp_outTable)
            # logger output
            self.ES_logger.debug('Zonal statistics finished:{}'.format(temp_emission_inRaster))

            temp_outCsv = os.path.join(temp_out_csv_path, temp_emission_inRaster + '.csv')
            self.do_zonal_table_to_csv(table=temp_outTable, year=peak['year'], outPath=temp_outCsv)

            # logger output
            self.ES_logger.debug('Zonal statistics convert to csv.')

            # 生成部门中心的栅格名称
            temp_main_inRaster = 'center_main_sector_{}_{}'.format(peak['peak_name'], peak['year'])

            # 检查输入的待统计值
            if not (arcpy.Exists(temp_main_inRaster)):
                print('Error: inRaster not found. inRaster name: {}'.format(temp_main_inRaster))

                # logger output
                self.ES_logger.error('inRaster does not exist. inRaster name: {}'.format(temp_main_inRaster))

                exit(1)

            temp_outTable = 'table_' + temp_main_inRaster

            self.do_zonal_statistic_to_table(
                inZoneData=inZone,
                zoneField='ISO_A3',
                inValueRaster=temp_main_inRaster,
                outTable=temp_outTable)
            # logger output
            self.ES_logger.debug('Zonal statistics finished:{}'.format(temp_main_inRaster))

            temp_outCsv = os.path.join(temp_out_csv_path, temp_main_inRaster + '.csv')
            self.do_zonal_table_to_csv(table=temp_outTable, year=peak['year'], outPath=temp_outCsv)

            # logger output
            self.ES_logger.debug('Zonal statistics convert to csv.')

            # 生成中心权重的栅格名称
            temp_main_weight_inRaster = 'center_main_sector_weight_{}_{}'.format(peak['peak_name'],
                                                                             peak['year'])
            # 检查输入的待统计值
            if not (arcpy.Exists(temp_main_weight_inRaster)):
                print('Error: inRaster not found. inRaster name: {}'.format(temp_main_weight_inRaster))

                # logger output
                self.ES_logger.error('inRaster does not exist. inRaster name: {}'.format(temp_main_weight_inRaster))
                exit(1)

            temp_outTable = 'table_' + temp_main_weight_inRaster

            self.do_zonal_statistic_to_table(
                inZoneData=inZone,
                zoneField='ISO_A3',
                inValueRaster=temp_main_weight_inRaster,
                outTable=temp_outTable)
            # logger output
            self.ES_logger.debug('Zonal statistics finished:{}'.format(temp_main_weight_inRaster))

            temp_outCsv = os.path.join(temp_out_csv_path, temp_main_weight_inRaster + '.csv')
            self.do_zonal_table_to_csv(table=temp_outTable, year=peak['year'], outPath=temp_outCsv)

            # logger output
            self.ES_logger.debug('Zonal statistics convert to csv.')

    def zonal_center_categories_info_statistics(self, emission_center, inZone, outPath):
        # 获得保存路径
        temp_out_csv_path = os.path.abspath(outPath)

        # 检查输入的分区是否存在
        if not (arcpy.Exists(inZone)):
            print('Error: inZone not found.')

            # logger output
            self.ES_logger.error('inZone does not exist.')

            exit(1)

        if not emission_center:
            print('ERROR: input emission center does not exist.')

            # logger output
            self.ES_logger.error('input emission center does not exist.')
            exit(1)

        # 逐年处理
        for peak in emission_center.return_center().values():
            # 生成分类中心的栅格名称
            temp_categories_inRaster = 'sorted_categories_center_{}_{}'.format(peak['peak_name'],
                                                                           peak['year'])

            # 检查输入的待统计值
            if not (arcpy.Exists(temp_categories_inRaster)):
                print('Error: inRaster not found. inRaster name: {}'.format(temp_categories_inRaster))

                # logger output
                self.ES_logger.error('inRaster does not exist. inRaster name: {}'.format(temp_categories_inRaster))

                exit(1)

            temp_outTable = 'table_' + temp_categories_inRaster

            self.do_zonal_statistic_to_table(
                inZoneData=inZone,
                zoneField='ISO_A3',
                inValueRaster=temp_categories_inRaster,
                outTable=temp_outTable)
            # logger output
            self.ES_logger.debug('Zonal statistics finished:{}'.format(temp_categories_inRaster))

            temp_outCsv = os.path.join(temp_out_csv_path, temp_categories_inRaster + '.csv')
            self.do_zonal_table_to_csv(table=temp_outTable, year=peak['year'], outPath=temp_outCsv)

            # logger output
            self.ES_logger.debug('Zonal statistics convert to csv.')

    def deprecated_zonal_year_statistics(self, year_range, inZone, center_range, outPath):
        # # 获得保存路径
        # temp_out_csv_path = os.path.abspath(outPath)

        # # 检查输入的分区是否存在
        # if not (arcpy.Exists(inZone)):
        #     print('Error: inZone not found.')

        #     # logger output
        #     self.ES_logger.error('inZone does not exist.')

        #     return

        # # 生成中心点
        # temp_center = str((min(center_range) + max(center_range)) / 2).replace('.', '')

        # for yr in tqdm(range(min(year_range), max(year_range) + 1)):
        #     # 生成中心的栅格名称
        #     temp_main_inRaster = 'center_main_sector_{}_{}'.format(temp_center, yr)
        #     # 检查输入的待统计值
        #     if not (arcpy.Exists(temp_main_inRaster)):
        #         print('Error: inRaster not found.')

        #         # logger output
        #         self.ES_logger.error('inRaster does not exist.')

        #         return

        #     temp_outTable = 'table_' + temp_main_inRaster

        #     self.do_zonal_statistic_to_table(
        #         year=yr,
        #         inZoneData=inZone,
        #         zoneField='ISO_A3',
        #         inValueRaster=temp_main_inRaster,
        #         outTable=temp_outTable)
        #     # logger output
        #     self.ES_logger.debug('Zonal statistics finished:{}'.format(temp_main_inRaster))

        #     temp_outCsv = os.path.join(temp_out_csv_path, temp_main_inRaster + '.csv')
        #     self.do_zonal_table_to_csv(table=temp_outTable, year=yr, outPath=temp_outCsv)

        #     # logger output
        #     self.ES_logger.debug('Zonal statistics convert to csv.')

        #     # 生成中心权重的栅格名称
        #     temp_main_weight_inRaster = 'center_main_sector_weight_{}_{}'.format(temp_center, yr)
        #     # 检查输入的待统计值
        #     if not (arcpy.Exists(temp_main_weight_inRaster)):
        #         print('Error: inRaster not found.')

        #         # logger output
        #         self.ES_logger.error('inRaster does not exist.')
        
        pass

    ############################################################################
    ############################################################################
    # 部门排放合并至分类排放
    ############################################################################
    ############################################################################

    ############################################################################
    # 部门排放合并至分类排放参数设定
    ############################################################################
    # 实际执行添加字段时使用的字段属性检查函数
    # 注意：
    # 这个函数需要输入一个字典，
    # 这个字典至少需要包含‘field_name’和‘field_type’两个参数，
    # 其余arcpy AddField_management要求的参数为可选参数，这些可参数需要
    # 符合arcpy的相应规定。
    # 如果不符合arcpy的规定则会返回一个空字典。
    def do_field_attributes_check(self, fieldAttributes):
        if not fieldAttributes:
            print('ERROR: input field and its attributes are empty. Please check the input')

            # logger output
            self.ES_logger.error('input field is empty.')
            exit(1)

        # 保存返回值的字典
        return_fieldAttributes = {}

        # 一系列类型检查，检查的基于arcpy中的定义
        # field name check
        if fieldAttributes['field_name'] == '' or type(fieldAttributes['field_name']) != str:
            print('ERROR: field name should be string type.')

            # logger output
            self.ES_logger.error('Field name type error.')
            return {}
        else:
            return_fieldAttributes['field_name'] = fieldAttributes['field_name'] 


        # field type check
        if fieldAttributes['field_type'] == '' or type(fieldAttributes['field_type']) != str:
            print('ERROR: field type should be string type.')

            # logger output
            self.ES_logger.error('Field type argument type error.')
            return {}
        else:
            return_fieldAttributes['field_type'] = fieldAttributes['field_type'] 

        # field precision check
        if 'field_precision' in fieldAttributes:
            if fieldAttributes['field_precision'] < 0 or type(
                    fieldAttributes['field_precision']) != int:
                print('ERROR: field precision should be positive integer type.')

                # logger output
                self.ES_logger.error('Field precision argument type error.')
                return {}
            else:
                return_fieldAttributes['filed_precision'] = fieldAttributes['field_precision'] 
        else:
            return_fieldAttributes['field_precision'] = ''
            # fieldAttributes['field_precision'] = '#'

        # field scale check
        if 'field_scale' in fieldAttributes:
            if fieldAttributes['field_scale'] < 0 or type(fieldAttributes['field_scale']) != int:
                print('ERROR: field scale should be positive integer type.')

                # logger output
                self.ES_logger.error('Field scale argument type error.')
                return {}
            else:
                return_fieldAttributes['filed_scale'] = fieldAttributes['field_scale'] 
        else:
            return_fieldAttributes['field_scale'] = ''
            #fieldAttributes['field_scale'] = '#'

        # field length check
        if 'field_length' in fieldAttributes:
            if fieldAttributes['field_length'] < 0 or type(fieldAttributes['field_length']) != int:
                print('ERROR: field length should be positive integer type.')

                # logger output
                self.ES_logger.error('Field length argument type error.')
                exit(1)
            else:
                return_fieldAttributes['filed_length'] = fieldAttributes['field_length'] 
        else:
            return_fieldAttributes['field_length'] = ''
            #fieldAttributes['field_length'] = '#'

        # field alias check
        if 'field_alias' in fieldAttributes:
            if fieldAttributes['field_alias'] == '' or type(fieldAttributes['field_alias']) != str:
                print('ERROR: field alias should be string type.')

                # logger output
                self.ES_logger.error('Field alias argument type error.')
                return {}
            else:
                return_fieldAttributes['filed_alias'] = fieldAttributes['field_alias'] 
        else:
            return_fieldAttributes['field_alias'] = ''
            #fieldAttributes['field_alias'] = '#'

        # field is nullable check
        if 'field_is_nullable' in fieldAttributes:
            if not fieldAttributes['field_is_nullable'] == 'NULLABLE' or not fieldAttributes[
                    'field_is_nullable'] == 'NON_NULLABLE':
                print('ERROR: field is nullable should be "NULLABLE" or "NON_NULLABLE".')

                # logger output
                self.ES_logger.error('Field is nullable argument type error.')
                return {}
            else:
                return_fieldAttributes['filed_is_nullable'] = fieldAttributes['field_is_nullable'] 
        else:
            return_fieldAttributes['field_is_nullable'] = ''
            #fieldAttributes['field_is_nullable'] = '#'

        # field is required check
        if 'field_is_required' in fieldAttributes:
            if not fieldAttributes['field_is_required'] == 'NON_REQUIRED' or not fieldAttributes[
                    'field_is_required'] == 'REQUIRED':
                print('ERROR: field is required should be "NON_REQUIRED" or "REQUIRED"')

                # logger output
                self.ES_logger.error('Field is required argument type error.')
                return {}
            else:
                return_fieldAttributes['filed_is_required'] = fieldAttributes['field_is_required'] 
        else:
            return_fieldAttributes['field_is_required'] = ''
            #fieldAttributes['field_is_required'] = '#'

        if 'field_domain' in fieldAttributes:
            if fieldAttributes['field_domain'] == '' or type(
                    fieldAttributes['field_domain']) != str:
                print('ERROR: field domain should be string type.')

                # logger output
                self.ES_logger.error('Field domain argument type error.')
                return {}
            else:
                return_fieldAttributes['filed_domain'] = fieldAttributes['field_domain'] 
        else:
            return_fieldAttributes['field_domain'] = ''
            #fieldAttributes['field_domain'] = '#'

        return return_fieldAttributes

    # 执行添加字段时使用的字段属性检查函数
    def field_attributes_checker(self, fields):
        # 保存返回数据的列表
        return_fields = []

        if not fields:
            print('ERROR: fields are empty. Please check input.')

            # logger output
            self.ES_logger.error('input fields are empty.')
            exit(1)
        # 处理列表形式的添加字段属性的合规性
        elif type(fields) == list:
            print('Checking fields attributes...\n')

            # logger output
            self.ES_logger.info('Checking fields attributes.')
            for field in tqdm(fields):
                return_fields.append(self.do_field_attributes_check(field))
        # 处理单个字典形式的添加字段属性的合规性
        elif type(fields) == dict:
            return_fields.append(self.do_field_attributes_check(fields))

        return return_fields

    # 如果需要添加多个字段，则可以利用以下这个函数生成一个待添加字段列表
    # 直接调用这个函数将返回现有的字段列表
    # 生成列表时，则需要传入一个字典，字典中需要包括两个参数：
    #       第一个参数键名为‘in_table’，值为需要添加的数据名或者表名；
    #       第二个参数键名为‘field_attributes_checker’，值为需要检查的字段属性的字典或者字典的列表，
    #
    # 如果传入参数是一个字典的列表则会向同一张表中一次性添加多个字段。
    # 注意：
    #    这里可以一次性向一张表中添加多个字段，也就是一个表名，添加字段的列表含有多个字典。
    #    如果需要向多张表中添加字段则需要多次调用setter函数
    @property
    def addField_list_assembler(self):
        return self.addField_list

    @addField_list_assembler.setter
    def addField_list_assembler(self, args):
        if not args['field_attributes_checker']:
            print('ERROR: fields are empty. Please check input.')

            # logger output
            self.ES_logger.error('input fields are empty.')
            exit(1)
        elif type(args['field_attributes_checker']) == list:
            print('Assembling attributes to data table name...\n')

            # logger output
            self.ES_logger.info('Assembling attributes to table name.')
            for field in tqdm(args['field_attributes_checker']):
                field['in_table'] = args['in_table']
                self.addField_list.append(field)
        elif type(args['field_attributes_checker']) == dict:
            args['field_attributes_checker']['in_table'] = args['in_table']
            self.addField_list.append(args['field_attributes_checker'])

    @addField_list_assembler.deleter
    def addField_list_assembler(self):
        self.addField_list = []

    # 实际执行为点数据添加需要归类整合的字段
    def do_add_fields(self, addField_list):
        # 检查添加的字段列表是不是空
        if not addField_list:
            print('ERROR: add field is empty. Please check input')

            # logger output
            self.ES_logger.error('add field is empty.')
            exit(1)

        print('Adding fields...\n')

        for field in tqdm(addField_list):
            # 开始执行添加字段
            # logger output
            self.ES_logger.info('Adding {} to {}'.format(field['field_name'], field['in_table']))
            try:
                arcpy.AddField_management(field['in_table'], field['field_name'],
                                          field['field_type'], field['field_precision'],
                                          field['field_scale'], field['field_length'],
                                          field['field_alias'], field['field_is_nullable'],
                                          field['field_is_required'], field['field_domain'])
                # logger output
                self.ES_logger.debug('Fields added:{}'.format(field['field_name']))
            except:
                print('Add field failed: {}'.format(field['field_name']))

                # logger output
                self.ES_logger.error('Add field failed: {}'.format(field['field_name']))

                print(arcpy.GetMessages())
                exit(1)

    # 执行为点数据添加需要归类整合的字段
    def addField_to_inPoint(self, inPoint, genFieldList):
        if type(genFieldList) != list:
            print('ERROR: result fields should be a list that values are dict that key-value are fields and attributes.')

            # logger output
            self.ES_logger.error(
                'genFieldList should be a list that values are dict that key-value fields and attributes.'
            )
            exit(1)
        elif genFieldList == []:
            self.ES_logger.info('skipped add fields.')
            return
        else:
            # 这里使用了属性修饰器
            # 所以又需要把setter的参数通过字典传进去......
            temp_addField_list_assembler_dict = {
                'in_table': inPoint,
                'field_attributes_checker': self.field_attributes_checker(genFieldList)
            }
            self.addField_list_assembler = temp_addField_list_assembler_dict
            self.do_add_fields(self.addField_list_assembler)
            del self.addField_list_assembler

    # 这个属性用于返回和生成需要整合和统计的部门和它整合后的对应类型。
    # 该属性返回一个字典，字典的键‘key’为需要整合的部门，值‘value’为整合后对应的类型。
    # gen_handle的示例可以参见__default_gen_handle
    @property
    def generalization_handle(self):
        return self.gen_handle

    # 属性的setter函数只负责检查输入的参数是否为字典。
    # 这里只设置为检查为字典的原因是归类方式完全为自定，无法做进一步的检查和限制。
    @generalization_handle.setter
    def generalization_handle(self, gen_handle):
        # 检查输入参数gen_handle的类型是否为dict
        if type(gen_handle) != dict:
            print('Generalizing sectors need input a dict gen_handle.')

            # logger output
            self.ES_logger.error('Input gen_handle type error.')
            return

        return gen_handle

    # 这里似乎写了一个重复的方法，
    # 生成排序后的列表是多余的。
    # 应该利用arcpy.listFields的结果，从这里确定位置和序列。
    # 所以不需要排序，只需要知道位置。
    #
    # 生成需要统计的部门排序后的列表
    # @property
    # def generalization_fields(self):
    #     return self.gen_field

    # @generalization_fields.setter
    # def generalization_fields(self, gen_handle):
    #     self.gen_field = list(gen_handle.keys()).sort()

    # 获得和设置统计的部门分类结果字段的名称
    @property
    def generalization_results(self):
        return self.gen_results

    @generalization_results.setter
    def generalization_results(self, gen_handle):
        # 利用set方法可以去重复的属性，将gen_handle中的唯一值都列出。
        handle_fields = list(set(gen_handle.values()))
        # 将每个唯一值作为字段名添加到结果列表中。
        self.gen_results = ['sorted_sectors'] + handle_fields

    # 生成需要统计的部门分类字段和排序后字段的名称
    @property
    def generalization_method(self):
        return self.gen_method

    # 需要为setter函数传入一个字典，字典中需要包含两个键值对：
    # 第一个键值对：key：‘gen_handle’；value: 一个字典其需要符合__default_gen_handle。
    # 第二个键值对：key：‘FieldsinTable’；value：一个列表其是获得的数据表中的所有已有的字段。
    # setter函数实质上实现了如下功能：
    # 1、生成一个gen_method字典；
    # 2、为该字典中添加需要若干组键值对（key-value），
    #   每一个键值对中的键（key）为分类的名称，
    #   值（value）为构成这个分类的每个部门（sector）在一个arcpy.cursor游标中的位置，这些位置通过列表的方式保存。
    @generalization_method.setter
    def generalization_method(self, args):
        # 创建一个新的gen_method字典
        self.gen_method = {}

        # 在gen_handle字典中逐个处理
        # gen_handle字典的内容：键是部门（sector）、值是分类（category），
        # 因此这里可以直接由items()方法获得所需信息
        for sector, category in args['gen_handle'].items():
            # 如果分类还未创建则添加一个新项到字典中
            if not category in self.gen_method:
                self.gen_method[category] = [args['FieldsinTable'].index(sector)]
            # 分类已存在则在值中追加一个新的部门（sector）位置
            else:
                self.gen_method[category].append(args['FieldsinTable'].index(sector))

    # 分类的编码
    # 返回一个分类名称的元组，元组中元素的位置代表了这个部门的编码。
    # 元组中预留了0号位置为未分类定义。
    @property
    def generalization_encode(self):
        return self.gen_encode

    # 分类的编码方式：
    # 分类的编码方式，也就是自定义的一种排序方式，同时这个编码方式也确定了部门的对应编码。
    # 利用对应编码为代号，结合计算得到占比总和，对分类进行排序并赋予对应顺序的编码代号，得到栅格的分类排放比例排序。
    @generalization_encode.setter
    def generalization_encode(self, encode_list):
        if not encode_list or encode_list == []:
            print('ERROR: encode list is empty. Please check the input is a list with values.')

            # logger output
            self.ES_logger.error('input encode list is empty.')
            exit(1)

        # 将自定义的编码合并。同时将其转化为元组，保持元素的顺序。
        self.gen_encode = tuple((['uncatalogued'] + encode_list))

    ############################################################################
    # 部门排放合并至分类排放相关函数/方法
    ############################################################################
    # 打印分类和对应编码的表格
    def print_categories(self, generalization_encode):
        print('Following table shows the categories and assigned codes for sectoral emission generalization.')

        # 设置打印格式
        temp_table_header_fmt = ['Categories', 'Code']

        # 打印表格
        print(tabulate(
            list(zip(self.gen_encode, range(len(self.gen_encode)))),
            headers=temp_table_header_fmt,
            tablefmt="grid"))

    # 这里需要传入两个参数：第一个参数，已经统计得到的分类和对应的排放量比例字典；第二个参数，自定义的编码方式
    # 函数将返回一个整数，这个整数的不同位置上的数字代表了对应部门的代码，同时整数的位数也表明了栅格部门的排放有多少个分类。
    def generalization_category_encoding(self, category_percentages, encode):
        # 检查两个输入变量是否为空，若为空则直接返回空字典
        if not category_percentages or not encode:
            print('ERROR: input category percentages or encode method is empty. Please check the input.')

            # logger output
            self.ES_logger.error('input category percentages or encode method is empty.')
            exit(1)

        # 排序字典结果，生成排序结果
        # 这里需要引入排序字典以保证字典的顺序不会发生变化
        temp_sorted = collections.OrderedDict(
            sorted(category_percentages.items(), key=lambda item: item[1], reverse=True))

        # 初始化编码
        temp_encode = ''

        # 从encode中找到位置然后顺序赋值
        for category, percentages in temp_sorted.items():
            # 如果分类为0，则不用赋值代码直接返回现有代码
            # 注意这里可能会由于栅格的排放比例为极小的小数，从而产生误判，需要特殊处理
            if percentages == 0:
                # 特殊情况：如果最大排放（第一个元素）就是0，则直接返回0。
                # 这里主要处理可能存在的0排放格网的漏网之鱼。
                if temp_encode == '':
                    return 0
                # 从现有位开始剩余位置均返回0
                else:
                    temp_sorted_position = list(temp_sorted.keys()).index(category)
                    # 之后位数赋值为0的本质就是乘上10，100，1000...这样的10的幂次的数
                    fill_suffix = math.pow(10, len(temp_sorted) - temp_sorted_position)
                    temp_encode = int(temp_encode) * fill_suffix
                    return int(temp_encode)
            else:
                # 从encode中找到元素的对应位置，位置即为代码
                temp_index = encode.index(category)
                temp_encode += str(temp_index)

        # 返回结果
        return int(temp_encode)

    # 统计分类排放量比例总和的函数。
    # 需要传入一个arcpy.cursor游标。
    # 函数返回一个字典，键为分类的名称，值为排放比例。
    def generalization_summarize(self, arcpyCursor, categories, generalization_method):
        # 检查两个输入变量是否为空，若为空则直接返回空字典
        if not categories or not generalization_method:
            print('ERROR: input categories or generalization method is empty. Please check the input.')

            # logger output
            self.ES_logger.error('input categories or generalization method is empty.')
            exit(1)

        # 临时存储分类比例加和结果的字典，其中的键为分类名称，值为比例加和结果
        results = {}

        # 第一步统计各个分类的部门排放总和
        # print 'Summarizing sectoral emission percentages into categories percentages... ...\n'

        # # logger output
        # self.ES_logger.info('Summarizing sectoral emission percentages into categories percentages.')
        for category in categories:
            # 从self.generalization_method获得需要统计加和的字段位置
            position_of_sectors = generalization_method[category]
            # 神奇的解包操作
            values_of_sectors = [arcpyCursor[position] for position in position_of_sectors]
            # 将结果保存到字典中
            results[category] = math.fsum(values_of_sectors)

        return results

    # 执行对数据表中的行数据内容进行分类整合的函数
    # 注意：
    # 这个函数要返回结果？
    # 这个函数要返回一个字典，其键为对应arcpy的字段名，值为统计后的结果
    def sectors_generalize_processe(self, arcpyCursor):
        # 初始化临时变量
        # 获得需要进行的分类名称
        # 先从表格中获得结果字段然后删除排序结果字段就是需要的分类名称
        temp_category = copy.deepcopy(self.generalization_results)
        temp_category.remove('sorted_sectors')
        # 获得分类排序的编码规则
        temp_encode = self.generalization_encode

        # 统计分类的排放量
        # 这里self.generalization_summarize将返回一个字典，内容是分类和对应的比例
        cate_percents = self.generalization_summarize(
            arcpyCursor=arcpyCursor,
            categories=temp_category,
            generalization_method=self.generalization_method)

        # 第二步排序字典结果，生成排序结果
        cate_percents['sorted_sectors'] = self.generalization_category_encoding(
            category_percentages=cate_percents, encode=temp_encode)

        # 返回结果字典
        return cate_percents

    # 实际执行单个栅格的部门类型归类
    # 这里传入的genFieldList参数是指需要在数据表中添加的用于结果生成的字段组成的列表。所以，列表中应该由若干字典组成。
    # 这些字典中的键值这里需要满足field_attributes_checker的条件，也就是要满足arcpy为数据表添加字段的要求。
    # 如果数据表中已经存在了对应的统计结果生成的字段，则可以传入一个空列表参数以跳过添加字段过程。
    def do_sectors_generalize(self, inPoint, genFieldList, gen_handle):
        # 尝试为数据表添加统计结果字段
        # 这里会检查genField参数，如果传入空字典则跳过添加字段步骤，如果为有内容的字典则进行字段添加，如果为其他类型的则报错
        self.addField_to_inPoint(inPoint=inPoint, genFieldList=genFieldList)

        # 首先列出点数据中的所有字段并提取出也在gen_handle存在的字段
        # --first lets make a list of all of the fields in the table
        fields = arcpy.ListFields(inPoint)
        field_names = [field.name for field in fields]
        # 从已有的数据表中找到对应部门的位置，并存入统计方法中
        self.generalization_method = {'gen_handle': gen_handle, 'FieldsinTable': field_names}
        # 获得统计结果字段的名称
        self.generalization_results = gen_handle

        # 注意：
        # 根据arcpy文档给出的说明：
        # UpdateCursor 用于建立对从要素类或表返回的记录的读写访问权限。
        # 返回一组迭代列表。 列表中值的顺序与 field_names 参数指定的字段顺序相符。
        # 构造游标，开始逐行操作
        with arcpy.da.UpdateCursor(inPoint, field_names) as cursor:
            for row in tqdm(cursor):
                # 检查栅格排放值是否为0，为0则直接将所有值赋值为0
                if row[field_names.index('grid_total_emission')] == 0:
                    # 检查最大部门排放是否为0
                    # 二次确认
                    if row[field_names.index('wmax')] == 0:
                        # 对genField给出的所有位置都赋值
                        for result in self.generalization_results:
                            row[field_names.index(result)] = 0
                else:
                    temp_generalization = self.sectors_generalize_processe(row)
                    # 对genField得到结果中给出的所有位置都赋值
                    for result, value in temp_generalization.items():
                        row[field_names.index(result)] = value

                # 更新行信息
                cursor.updateRow(row)

    # 对点数据中的栅格执行部门类型归类
    def sectors_generalize(self, inPoint, gen_handle, gen_fieldList):
        # 检查输入是否为空。为空则直接返回。
        if not inPoint or not gen_handle:
            print('ERROR: input inPoint or gen_handle is empty. Please check the inputs.')

            # logger output
            self.ES_logger.error('sectors generalize input arguments were empty.')
            exit(1)

        if gen_fieldList == []:
            print('WARNING: gen_fieldList is empty. Process will not add new field to data table.')

            # logger output
            self.ES_logger.info('No new fields were added.')

        # 调用实际执行函数进行归类
        self.do_sectors_generalize(
            inPoint=inPoint, genFieldList=gen_fieldList, gen_handle=gen_handle)

    # 处理一定时间范围内的部门排放进行类型归类
    def year_sectors_generalize(self, year_range, gen_handle, gen_fieldList):
        # 检查输入年份变量参数是否合规
        if (type(year_range[0]) != int) or (type(year_range[1]) != int):
            print('Error! Processing starting year and ending year must be int value')
            self.ES_logger.info('Year setting type error.')
            self.ES_logger.error('Year setting error!')
            exit(1)
        elif min(year_range) < self.__default_start_year or max(year_range) > self.__default_end_year:
            print('Error! Processing year range out of data support! The year must contain in 1970 to 2018')
            self.ES_logger.info('Year settings are out of range.')
            self.ES_logger.error('Year setting error!')
            exit(1)
        else:
            temp_start_year, temp_end_year = min(year_range), max(year_range)
            self.ES_logger.info('Year has set.')

        for year in range(temp_start_year, temp_end_year + 1):
            temp_inPoint = 'sectoral_weights_{}'.format(year)
            self.print_start_year(year=year)
            self.sectors_generalize(
                inPoint=temp_inPoint, gen_handle=gen_handle, gen_fieldList=gen_fieldList)
            self.print_finish_year(year=year)

    def sorted_categories_rasterize(self, year, fieldName):
        temp_point = 'sectoral_weights_{}'.format(year)
        save_raster_categories = 'sorted_categories_{}'.format(year)

        # 用wraster列转栅格
        try:
            arcpy.PointToRaster_conversion(temp_point, fieldName, save_raster_categories,
                                           'MOST_FREQUENT', '#', '0.1')

            print('Create categories sorts raster: {}'.format(temp_point))

            # logger output
            self.ES_logger.debug('Categories sorts rasterize finished:{}'.format(year))
        except:
            print('Create categories sorts raster field: {}'.format(temp_point))

            # logger output
            self.ES_logger.error('categories sorts rasterize failed:{}'.format(year))

            print(arcpy.GetMessages())

    # 这里inRaster是需要提取数据的原始栅格；inZone是用来分类的区域范围；center_range是生成分类兴趣区域的标准；outPath是最后csv文件的输出位置
    def zonal_sectors_generalize(self, year_range, inZone, center_range, zoneField, outPath):
        # 获得保存路径
        temp_out_csv_path = os.path.abspath(outPath)

        # 生成中心点
        temp_center = str((min(center_range) + max(center_range)) / 2).replace('.', '')

        # 检查所用的分类区域范围是否存在
        if not (arcpy.Exists(inZone)):
            print('Error: input inZone not found.')

            # logger output
            self.ES_logger.error('input inZone does not exist.')
            exit(1)

        for yr in tqdm(range(min(year_range), max(year_range) + 1)):
            # 生成需要提取数据的原始栅格
            # 生成需要提取数据的inRaster名称
            temp_inRaster = 'sorted_categories_{}'.format(yr)
            # 检查所用的中心mask是否存在
            if not (arcpy.Exists(temp_inRaster)):
                print('Error: input sorted categories raster not found.')

                # logger output
                self.ES_logger.error('input sorted categories raster does not exist.')

                exit(1)

            # 生成中心的栅格名称
            # 生成提取所用的中心mask名称
            temp_mask_inRaster = 'center_mask_{}_{}'.format(temp_center, yr)
            # 检查所用的中心mask是否存在
            if not (arcpy.Exists(temp_mask_inRaster)):
                print('Error: input mask raster not found.')

                # logger output
                self.ES_logger.error('input mask raster does not exist.')

                exit(1)

            temp_outTable_path = 'table_sorted_categories_{}_{}'.format(temp_center, yr)
            temp_outRaster_path = 'raster_sorted_categories_{}_{}'.format(temp_center, yr)

            # 这里要注意！！！
            # 为了提取最大的分类，这里采用了“//”整除计算，整除100000会获得最高位的数值。
            temp_zonalRaster = (Raster(temp_inRaster) * Raster(temp_mask_inRaster)) // 100000
            temp_zonalRaster.save(temp_outRaster_path)

            self.do_zonal_statistic_to_table(
                year=yr,
                inZoneData=inZone,
                zoneField=zoneField,
                inValueRaster=temp_zonalRaster,
                outTable=temp_outTable_path)

            # logger output
            self.ES_logger.debug('Zonal statistics finished:{}'.format(temp_inRaster))

            temp_outCsv = os.path.join(temp_out_csv_path,
                                       'sorted_categories_{}_{}.csv'.format(temp_center, yr))

            self.do_zonal_table_to_csv(table=temp_outTable_path, year=yr, outPath=temp_outCsv)

            # logger output
            self.ES_logger.debug('Zonal statistics convert to csv.')

    # 实际执行从点数据中生成不同分类排放栅格
    # 注意！！！
    # 这个函数会产生一系列的数量众多的栅格，它们的命名逻辑为`category_year`。
    # 如果要使用这里的栅格结果请参考以上命名逻辑找到所需栅格
    # 
    # 注意第四个参数`return_results`，当此参数为True时函数会返回一个生成栅格的结果的列表；
    # 当此参数为False时，函数将不返回任何结果而结束。
    def generate_categories_emission_raster(self, inPoint, category_field_list, return_results=True):
        '''
        实际执行从点数据中生成不同分类排放量格
        注意！！！
        这个函数会产生一系列的数量众多的栅格，它们的命名逻辑为`category_year`。
        如果要使用这里的栅格结果请参考以上命名逻辑找到所需栅格
     
        注意第四个参数`return_results`，当此参数为True时函数会返回一个生成栅格的结果的列表；
        当此参数为False时，函数将不返回任何结果而结束。
        '''
        if not category_field_list :
            print('ERROR: please specify the center_list and the category_list')

            # logger output
            self.ES_logger.error('input center list or category list does not exist.')

            exit(1)

        if not arcpy.Exists(inPoint):
            print('ERROR: input point data does not exist')

            # logger output
            self.ES_logger.error('input point data does not exist.')

            exit(1)
        
        # 向点数据中添加临时字段
        # 这里选择一次性添加所有临时字段。
        # 考虑到如果在计算的过程中不停的添加和删除字段会极大的消耗计算时间，
        # 所以采取统一添加统一删除的策略
        try:
            # 逐一添加临时字段
            temp_add_field = ['export_{}'.format(category) for category in category_field_list]
            for field in temp_add_field:
                arcpy.AddField_management(inPoint, field, 'DOUBLE', '#', '#', '#', '#','NULLABLE', '#', '#')

            # logger output
            self.ES_logger.debug('export raster: temporary field added.')
        except:
            print('export raster: Add field to point failed in: {}'.format(inPoint))

            # logger output
            self.ES_logger.error( 'Export raster: Add field to point failed in: {}'.format(inPoint))

            print(arcpy.GetMessages())
            exit()
        
        # 储存可能需要返回的结果列表
        temp_return_rasters = []

        # 对每个分类进行栅格导出
        for category in category_field_list:
            # 构造游标需要的列名称
            # 注意：
            # 根据arcpy文档给出的说明：
            # UpdateCursor 用于建立对从要素类或表返回的记录的读写访问权限。
            # 返回一组迭代列表。 列表中值的顺序与 field_names 参数指定的字段顺序相符。
            # 准备需要列出的字段的名称
            # 需要列出的字段包括：储存临时计算结果的字段：EOF_TEMP总排放量对数值字段：grid_log_total_emission、各个分类的比例字段：从category_field_list中获取
            temp_result_field = 'export_{}'.format(category)
            temp_field_list = ['grid_log_total_emission', category, temp_result_field] 

            # 计算分类排放的排放量
            with arcpy.da.UpdateCursor(in_table=inPoint, field_names=temp_field_list) as cursor:
                for row in tqdm(cursor):
                    # 计算每个每个分类的排放量，通过“总量*分类比例”得到需要值。
                    # 注意:
                    #       这里由于使用的是对数值，所以实际的进行的运算是加法运算。
                    # 注意2:
                    #       由于很多部门的排放是0，在计算log10的时候会出错。
                    #       并且就现实中的情况来说，一个区域的某一部门排放是0那就意味着这个地区没有这个部门，
                    #       也就没有必要对这个部门进行计算了
                    
                    # 该栅格没有包含排放的情况
                    if row[0] is None:
                        row[2] = None
                        cursor.updateRow(row)
                        continue

                    # 该栅格没有某一部门排放的情况
                    if row[1] == 0:
                        row[2] = None
                    # 该栅格存在某一部门排放
                    else:
                        temp_category_property = numpy.log10(row[1])
                        row[2] = row[0] + temp_category_property

                    # 更新行信息
                    cursor.updateRow(row)

            # 保存列数据为栅格
            save_raster = '{}_{}'.format(temp_result_field, inPoint[-4:])

            ################################################################################
            ################################################################################
            # 以下部分是一段python segmentation fault debug 使用的代码
            # arcpy.PointToRaster_conversion(inPoint,temp_result_field, save_raster,'MOST_FREQUENT', '#', '0.1')
            # arcpy.PointToRaster_conversion(in_features=inPoint,
            #                                 value_field=temp_result_field,
            #                                 out_rasterdataset=save_raster,
            #                                 cell_assignment="MOST_FREQUENT",
            #                                 cellsize=0.1)

            # # 添加转换结果到待返回列表
            # temp_return_rasters.append(save_raster)

            # # logger output
            # self.ES_logger.debug('EOF rasterize finished:%s' % save_raster)
            # debug 结束
            ################################################################################
            ################################################################################

            try:
                arcpy.PointToRaster_conversion(inPoint,temp_result_field, save_raster,'MOST_FREQUENT', '#', '0.1')

                # 添加转换结果到待返回列表
                temp_return_rasters.append(save_raster)

                # logger output
                self.ES_logger.debug('EOF rasterize finished:{}'.format(save_raster))
            except:
                print('Create raster field: {}'.format(save_raster))

                # logger output
                self.ES_logger.error('EOF rasterize failed:{}'.format(save_raster))

                print(arcpy.GetMessages())

        # 从点数据中删除添加的临时列
        print('Deleting temporary fields...')
        arcpy.DeleteField_management(inPoint, temp_add_field)
        # logger output
        self.ES_logger.info('Deleted temporary fields.')
        print('Deleted temporary fields.')
        
        # 决定返回模式
        if return_results:
            # 返回一个包含生成栅格名称的列表
            return temp_return_rasters
        else:
            return
            
    ############################################################################
    ############################################################################
    # 合并同一年不同排放中心至一个栅格
    ############################################################################
    ############################################################################
    def year_emission_center_raster_merge(self, wild_card_fmt, emission_center_list, year_range,
                                          output_fmt):
        if not wild_card_fmt or type(
                wild_card_fmt) != str or not emission_center_list or not year_range:
            print('ERROR: input emission_center_list or year is empty.')

            # logger output
            self.ES_logger.error('emission_center_list or year is empty.')
            exit(1)

        if len(year_range) != 2:
            print('ERROR: year range require a two ints tuple.')

            # logger output
            self.ES_logger.error('year range error.')
            exit(1)

        if max(year_range) > self.end_year or min(year_range) < self.start_year:
            print('ERROR: year is out range.')

            # logger output
            self.ES_logger.error('year range error.')
            exit(1)

        for yr in tqdm(range(min(year_range), max(year_range) + 1)):
            self.do_emission_center_raster_merge(
                wild_card_fmt=wild_card_fmt,
                emission_center_list=emission_center_list,
                year=yr,
                output_fmt=output_fmt)

    def do_emission_center_raster_merge(self,
                                        wild_card_fmt,
                                        emission_center_list,
                                        year,
                                        output_fmt='categories_center_merge_{}'):
        if not wild_card_fmt or type(wild_card_fmt) != str or not emission_center_list or not year:
            print('ERROR: input emission_center_list or year is empty.')

            # logger output
            self.ES_logger.error('emission_center_list or year is empty.')

        temp_center_peaks = []

        # 生成多个中心的年份列表
        for center in emission_center_list:
            # 列出需要合并的栅格列表
            # 这里需要用try...catch...来处理输入的output_fmt是无法格式化的。
            try:
                temp_raster_name = wild_card_fmt.format(center.return_center()[year]['peak_name'],
                                                    center.return_center()[year]['year'])
            except Exception as e:
                # logger output
                self.ES_logger.info('temp raster name formatting failed. raster name was {}.'.format(wild_card_fmt))

            temp_center_peaks.append(temp_raster_name)

        # 列出需要合并的栅格列表
        temp_working_rasters = self.do_arcpy_list_raster_list(temp_center_peaks)

        # 这里需要用try...catch...来处理输入的output_fmt是无法格式化的。
        try:
            temp_output = output_fmt.format(year)
        except Exception as e:
            temp_output = output_fmt

            # logger output
            self.ES_logger.info('temp raster name formatting failed. raster name was {}.'.format(temp_raster_name))

        # 进行合并之前需要先获得原始栅格的像素深度参数。
        # 原因如下：MosaicToNewRaster_management（）函数中如果不设置像素类型，将使用默认值 8 位，而输出结果可能会不正确。

        temp_pixel_type = self.__raster_pixel_type[arcpy.Raster(temp_working_rasters[0]).pixelType]

        # Mosaic 所有中心的结果到新的栅格中
        arcpy.MosaicToNewRaster_management(
            input_rasters=temp_working_rasters,
            output_location=self.__workspace,
            raster_dataset_name_with_extension=temp_output,
            pixel_type=temp_pixel_type,
            number_of_bands=1,
            mosaic_method="FIRST",
            mosaic_colormap_mode="FIRST")

    # 这里需要传入一个center_colormap字典
    def year_emission_center_mask_raster_merge(self, wild_card_fmt, emission_center_list,
                                               center_colormap, year_range, output_fmt):
        if not wild_card_fmt or type(
                wild_card_fmt
        ) != str or not emission_center_list or not year_range or not center_colormap:
            print('ERROR: input arguments were empty.')

            # logger output
            self.ES_logger.error('input arguments were empty.')
            exit(1)

        if len(year_range) != 2:
            print('ERROR: year range require a two ints tuple.')

            # logger output
            self.ES_logger.error('year range error.')
            exit(1)

        if max(year_range) > self.end_year or min(year_range) < self.start_year:
            print('ERROR: year is out range.')

            # logger output
            self.ES_logger.error('year range error.')
            exit(1)

        for yr in tqdm(range(min(year_range), max(year_range) + 1)):
            temp_center_peaks = []
            temp_raster_centers_name = {}

            # 生成多个中心的年份列表
            for center in emission_center_list:
                # 列出需要合并的栅格列表
                # 这里需要用try...catch...来处理输入的output_fmt是无法格式化的。
                try:
                    temp_raster_name = wild_card_fmt.format(center.return_center()[yr]['peak_name'],
                                                        center.return_center()[yr]['year'])
                except Exception as e:
                    temp_raster_name = wild_card_fmt

                    # logger output
                    self.ES_logger.info('temp raster name formatting failed. raster name was {}.'.format(temp_raster_name))

                temp_center_peaks.append(temp_raster_name)
                temp_raster_centers_name[temp_raster_name] = center.return_center(
                )[yr]['center_name']

            # 列出需要合并的栅格列表
            temp_working_rasters = self.do_arcpy_list_raster_list(temp_center_peaks)

            # 这里将修改每个中心的栅格值为传入的color_map 中的对应值；
            # 同时生成真正待合并的栅格列表
            temp_mosaic_list = []
            for raster in temp_working_rasters:
                temp_false_value = center_colormap[temp_raster_centers_name[raster]]
                temp_con = Con(raster, temp_false_value)

                temp_mosaic_list.append(temp_con)

            # 这里需要用try...catch...来处理输入的output_fmt是无法格式化的。
            try:
                temp_output = output_fmt.format(yr)
            except Exception as e:
                temp_output = output_fmt

                # logger output
                self.ES_logger.info('temp raster name formatting failed. raster name was {}.'.format(temp_raster_name))

            # 进行合并之前需要先获得原始栅格的像素深度参数。
            # 原因如下：MosaicToNewRaster_management（）函数中如果不设置像素类型，将使用默认值 8 位，而输出结果可能会不正确。

            temp_pixel_type = self.__raster_pixel_type['S8']

            arcpy.MosaicToNewRaster_management(
                input_rasters=temp_mosaic_list,
                output_location=self.__workspace,
                raster_dataset_name_with_extension=temp_output,
                pixel_type=temp_pixel_type,
                number_of_bands=1,
                mosaic_method="FIRST",
                mosaic_colormap_mode="FIRST")

    ############################################################################
    ############################################################################
    # 为EOF分析进行的栅格数据准备
    ############################################################################
    ############################################################################
    # 为特定的排放中心栅格叠加历史排放区域
    def EOF_center_mosaic_extend(self, center_list, center_raster_fmt='center_{}_{}', background_fmt='{}_geographical_extend_null_mask'):
        if not center_list:
            print('ERROR: input arguments does not exist, please check the inputs.')

            # logger output
            self.ES_logger.error('input arguments does not exist.')
            exit(1)

        # 传入一个包括中心的列表
        if type(center_list) == list:
            # 逐个处理传入的排放中心
            for emission_center in center_list:
                # 获得中心的背景栅格
                try:
                    temp_background = background_fmt.format(emission_center.center_name)
                except Exception as e:
                    print('background raster formatting failed.')
                    # logger output
                    self.ES_logger.error('get background raster name formatting failed. raster name formate was {}.'.format(temp_background))

                    exit(1)

                # 如果看不懂下面的python解包操作，就看注释里的这段代码。
                # If developers were confused about the following python unpacking list, please read the code block in following comments.
                #
                #     for (year, peak) in emission_center.return_center().items():
                #         temp_raster = 'center_mask_%s_%s' % (peak['peak_name'], year)
                # 列出该中心的所有栅格
                try:
                    temp_raster_list = [
                        center_raster_fmt.format(peak['peak_name'], year)
                        for (year, peak) in emission_center.return_center().items()
                    ]
                except Exception as e:
                    print('center raster formatting failed.')
                    # logger output
                    self.ES_logger.error('mosaicing center raster name formatting failed. raster name formate was {}.'.format(center_raster_fmt))

                    exit(1)

                # 执行叠加背景
                self.do_EOF_mosaic_extend(raster_list=temp_raster_list,
                                        background=temp_background)
        else:    # 如果只是传入单一中心，且没有用列表包括该中心
            # 获得中心的背景栅格
            try:
                temp_background = background_fmt.format(emission_center.center_name)
            except Exception as e:
                print('background raster formatting failed.')

                # logger output
                self.ES_logger.error('get background raster name formatting failed. raster name formate was {}.'.format(temp_background))

                exit(1)

            # 如果看不懂下面的python解包操作，就看注释里的这段代码。
            # If developers were confused about the following unpack list, please read the code block in following comments.
            #
            #     for (year, peak) in emission_center.return_center().items():
            #         temp_raster = 'center_mask_%s_%s' % (peak['peak_name'], year)
            # 列出该中心的所有栅格
            try:
                temp_raster_list = [
                    center_raster_fmt.format(peak['peak_name'], year)
                    for (year, peak) in emission_center.return_center().items()
                ]
            except Exception as e:
                print('center raster formatting failed.')
                # logger output
                self.ES_logger.error('mosaicing center raster name formatting failed. raster name formate was {}.'.format(center_raster_fmt))

                exit(1)

            # 执行叠加背景
            self.do_EOF_mosaic_extend(raster_list=temp_raster_list,
                                    background=temp_background)

    # 为一个中心里所有分类单独添加该分类对应的空间围背景
    def EOF_center_category_mosaic_extend(self, center_name, category_list, background_fmt='{}_EOF_geographical_extend_null_mask'):
        if not center_name or not category_list:
            print('ERROR: input arguments does not exist, please check the inputs.')

            # logger output
            self.ES_logger.error('input arguments does not exist.')
            exit(1)

        # 获得中心的背景栅格
        try:
            temp_background = background_fmt.format(center_name)
        except Exception as e:
            print('background raster formatting failed.')

            # logger output
            self.ES_logger.error('get background raster name formatting failed. raster name formate was {}.'.format(background_fmt))

            exit(1)

        for category in category_list:
            # 列出所有需要添加背景的栅格
            temp_wildcard = ['{}_EOF_{}_\d+'.format(center_name, category)]
            temp_working_rasters = self.do_arcpy_list_raster_list(wildcard_list=temp_wildcard, wildcard_mode=False)

            # 这里还要加一个判断待添加背景的栅格是否是所需时间段内的栅格
            temp_working_rasters = [raster for raster in temp_working_rasters if int(raster[-4:]) >= self.start_year and int(raster[-4:]) <= self.end_year ]

            # 执行叠加背景
            self.do_EOF_mosaic_extend(raster_list=temp_working_rasters,
                                    background=temp_background)
            
    # 执行为任意栅格叠加背景值的
    # 注意！
    # 执行这个操作将改变输入栅格。请不要在原始数据上执行！
    # 注意！！
    # 这个函数得出的排放量栅格是非对数值的栅格
    #   由于对数计算在使用0值为背景存在现实意义的错误，所以EOF计算的过程应该采用一般数量进行
    def do_EOF_mosaic_extend(self, raster_list, background):
        if not raster_list or not background:
            print('ERROR: input arguments does not exist, please check the inputs.')

            # logger output
            self.ES_logger.error('input arguments does not exist.')
            exit(1)

        # 列出待补充背景的栅格
        temp_working_rasters = self.do_arcpy_list_raster_list(wildcard_list=raster_list)

        # 逐个对栅格执行mosaic背景栅格
        for raster in tqdm(temp_working_rasters):
            # 1、将输入的对数栅格转换为一般数量的栅格
            #   准备arcgis地图代数幂运算需要的10值栅格
            temp_power_background = Con(raster, 10.0, '')
            #   转换对数值为一般数量
            temp_linear_raster = Power(temp_power_background, raster)
            #   临时存储一般排放量的位置
            temp_linear_raster_path = raster + '_linear'
            temp_linear_raster.save(temp_linear_raster_path)
            # 这里要进行一个栅格的交换：
            # 首先删除原来的raster，再将内存中的temp_linear_raster固定到raster的位置
            self.delete_temporary_raster([raster])
            arcpy.Raster(temp_linear_raster_path).save(raster)

            # 2、为栅格添加0值背景
            self.mosaic_background_to_raster(inRaster=raster, background=background)
            # 删除临时栅格
            self.delete_temporary_raster([temp_linear_raster_path])

    # 从点数据中生成各个中心中的不同分类的排放分量栅格
    # 这里的设计思路是只传入一个点数据。因为，如果一次传入一组点数据，很可能导致这个函数一旦进入就
    # 无法停止下来。
    # 这里可以使用return_results参数来控制是否返回一个结果的字典。
    #   这个字典的键为中心名字，值为该中心下所有分类的生成栅格的列表。
    #   注意字典值中的列表不再做进一步划分，将包含所有的栅格。
    # def EOF_generate_center_categories_emission_raster(self, inPoint, center_list, category_field_list, add_background=True, background_raster='background', return_result=True):
    def EOF_generate_center_categories_emission_raster(self, inPoint, center_list, category_field_list,  return_result=True):
        if not inPoint or not center_list or not category_field_list:
            print('ERROR: The inputs does not exist. Please check the inputs.'  )

            # logger output
            self.ES_logger.error('input does not exist.')
            exit(1)

        # 存储待返回结果的字典
        temp_return = {}

        # 逐个对中心处理
        for center in center_list:
            # 临时存储倒出到numpy的栅格列表
            temp_results = []
            
            # 逐个对中心进行分类EOF提取的操作
            temp_results = self.do_EOF_generate_center_categories_emission_raster(inPoint=inPoint,
                                                                    center=center,
                                                                    category_field_list=category_field_list,
                                                                    return_results=True)

            # 把结果保存到字典的对应键中
            temp_return[center.center_name] = temp_results

        # # TODO 这里存在重大逻辑问题！
        # # 应该是给每个生成的栅格加一次背景，还是针对所有时间的同一排放类型的栅格进行背景叠加操作？
        # # 按需要为数据添加历史排放背景
        # if add_background:
        #     # 为生成的初步提取栅格生成背景
        #     self.EOF_generate_center_categories_geographical_extend(center=center,
        #                                                             category_field_list=category_field_list,
        #                                                             background_raster=background_raster)

        #     # 为生成的初步提取栅格添加背景，并组合成EOF分析可用的数据
        #     self.EOF_center_category_mosaic_extend(center=center,
        #                                             category_list=category_field_list,
        #                                             background_fmt='%s_EOF_geographical_extend_null_mask')
        
        # 如果需要函数返回则在这里返回字典，如果不需要返回则函数直接跳过后结束。
        if return_result:
            return temp_return
        
    # 实际执行从点数据中生成某一个中心里的不同分类的排放量栅格
    # 注意！！！
    # 这个函数会产生一系列的数量众多的栅格，它们的命名逻辑为`centerName_category_year`。
    # 如果要使用这里的栅格结果请参考以上命名逻辑找到所需栅格
    # 
    # 注意第四个参数`return_results`，当此参数为True时函数会返回一个生成栅格的结果的列表；
    # 当此参数为False时，函数将不返回任何结果而结束。
    def do_EOF_generate_center_categories_emission_raster(self, inPoint, center, category_field_list, return_results=True):
        '''
        实际执行从点数据中生成某一个中心里的不同分类的排放量栅格
        注意！！！
        这个函数会产生一系列的数量众多的栅格，它们的命名逻辑为`centerName_category_year`。
        如果要使用这里的栅格结果请参考以上命名逻辑找到所需栅格
     
        注意第四个参数`return_results`，当此参数为True时函数会返回一个生成栅格的结果的列表；
        当此参数为False时，函数将不返回任何结果而结束。
        '''
        if not center or not category_field_list :
            print('ERROR: please specify the center_list and the category_list')

            # logger output
            self.ES_logger.error('input center list or category list does not exist.')

            exit(1)

        if not arcpy.Exists(inPoint):
            print('ERROR: input point data does not exist')

            # logger output
            self.ES_logger.error('input point data does not exist.')

            exit(1)
        
        # 向点数据中添加临时字段
        # 这里选择一次性添加所有临时字段。
        # 考虑到如果在计算的过程中不停的添加和删除字段会极大的消耗计算时间，
        # 所以采取统一添加统一删除的策略
        try:
            # 逐一添加临时字段
            temp_add_field = ['EOF_{}'.format(category) for category in category_field_list]
            for field in temp_add_field:
                arcpy.AddField_management(inPoint, field, 'DOUBLE', '#', '#', '#', '#','NULLABLE', '#', '#')

            # logger output
            self.ES_logger.debug('EOF: temporary field added.')
        except:
            print('EOF: Add field to point failed in: {}'.format(inPoint))

            # logger output
            self.ES_logger.error( 'EOF Add field to point failed in: {}'.format(inPoint))

            print(arcpy.GetMessages())
            exit()
        
        # 储存可能需要返回的结果列表
        temp_return_rasters = []

        ################################################################################
        ################################################################################
        ## 以下是游标算法的更新思路：
        #   首先如果每次只进行一个字段（也就是部门）的计算，会极大的减慢计算的速度。
        #   所以应该考虑在一次游标操作中，实现计算所有字段。
        ################################################################################
        ################################################################################
        # 构建cursor需要的字段
        # 这里字段的结构设计为：[总量，部门比例_1，EOF_部门结果_1,...,部门比例_n，EOF_部门结果_n].
        # 所以，每个EOF部门结果，EOF_部门结果_n是field_list[2*n] = field_list[0] + log10(field_list[2n-1])

        # 注意：所以添加的EOF结果字段已经保存在了上面的temp_add_field中。
        # 啊哈啊哈哈哈啊哈，你一定看不懂这段list解包操作。
        # 其实，我也看不懂。
        # 但是，它可以用。并且从这个可运行的解包操作中可以认识到，python的list解包操作是从后到前解析的。
        # 我的粗浅理解只能到这里了。
        temp_field_list = ['grid_log_total_emission'] + [cate_eof_pair for zip_cate_eof in zip(category_field_list, temp_add_field) for cate_eof_pair in zip_cate_eof]

        # 构建筛选中心的表达式
        temp_where_clause = '"center_type"=\'{}\''.format(center.center_name)

        with arcpy.da.UpdateCursor(in_table=inPoint, field_names=temp_field_list, where_clause=temp_where_clause) as cursor:
            for row in tqdm(cursor):
                # 计算每个每个分类的排放量，通过“总量*分类比例”得到需要值。
                # 注意:
                #       这里由于使用的是对数值，所以实际的进行的运算是加法运算。
                # 注意2:
                #       由于很多部门的排放是0，在计算log10的时候会出错。
                #       并且就现实中的情况来说，一个区域的某一部门排放是0那就意味着这个地区没有这个部门，
                #       也就没有必要对这个部门进行计算了
                # 注意3：
                #       这里字段的结构设计为：[总量，部门比例_1，EOF_部门结果_1,...,部门比例_n，EOF_部门结果_n].
                #       所以，每个EOF部门结果，EOF_部门结果_n是field_list[2*n] = field_list[0] + log10(field_list[2n-1])
                
                # 该栅格没有某一部门排放的情况
                if row[1] == 0:
                    continue
                # 该栅格存在某一部门排放
                else:
                    # 计算每个部门对应的EOF结果
                    for i in range(1,len(category_field_list)+1):
                        # 这里还要进行排放量比例是否为0的检查，因为涉及对数操作。
                        # 如果比例为0则直接为排放量赋0值
                        if row[2*i-1] == 0:
                            row[2*i] = 0
                        # 如果比例不为0则进行计算
                        else:
                            temp_cate_property = numpy.log10(row[2*i-1])
                            row[2*i] = row[0] + temp_cate_property

                # 更新行信息
                cursor.updateRow(row)

        # 逐部门保存结果
        for cate in temp_add_field:
            # 保存列数据为栅格
            save_raster = '{}_{}_{}'.format(center.center_name, cate, inPoint[-4:])

            try:
                print('EOF rasterize start:{}'.format(save_raster))
                arcpy.PointToRaster_conversion(inPoint,cate, save_raster,'MOST_FREQUENT', '#', '0.1')

                # 添加转换结果到待返回列表
                temp_return_rasters.append(save_raster)

                # logger output
                self.ES_logger.debug('EOF rasterize finished:{}'.format(save_raster))
                print('EOF rasterize finished:{}'.format(save_raster))
            except:
                print('Create raster field: {}'.format(save_raster))

                # logger output
                self.ES_logger.error('EOF rasterize failed:{}'.format(save_raster))

                print(arcpy.GetMessages())

        # 从点数据中删除添加的临时列
        print('Deleting temporary fields...')
        arcpy.DeleteField_management(inPoint, temp_add_field)
        # logger output
        self.ES_logger.info('Deleted temporary fields.')
        print('Deleted temporary fields.')
        
        # 决定返回模式
        if return_results:
            # 返回一个包含生成栅格名称的列表
            return temp_return_rasters
        else:
            return
            
    # 通过给定中心和分类的列表，生成一个中心里所有分类的空间分布范围
    def EOF_generate_center_categories_geographical_extend(self, center, total_emission_list):
        if not center or not total_emission_list :
            print('ERROR: The inputs does not exist. Please check the inputs.'  )

            # logger output
            self.ES_logger.error('input does not exist.')
            exit(1)

        # 以下这部分生成中心分类列表的操作已经不再需要了
        # # 生成筛选排放中心的全部分类栅格的正则表达式列表
        # temp_wildcard_center_re = '{}_EOF_'.format(center.center_name) + r'{}_\d+'
        # # 以下使用了解包操作，如果调试有困难就该写成for...loop
        # temp_wildcard_list = [temp_wildcard_center_re.format(category) for category in category_field_list]

        # 检查总量栅格是否存在
        temp_working_rasters = self.do_arcpy_list_raster_list(wildcard_list=total_emission_list, wildcard_mode=True)

        # 生成输出栅格的文件名
        # 生成文件名示例：center_678_1970_EOF_geographical_extend_null_mask
        temp_output_name_fmt = '{}_'.format(center.center_name) + r'{}' +'_EOF_geographical_extend_' + r'{}'
        
        # 调用实际执行的do_generate_extend函数
        self.do_generate_center_geographical_extend(
            raster_list=temp_working_rasters,
            center=center,
            output_name_fmt=temp_output_name_fmt)

    # 将栅格数据转换为numpy压缩格式并导出
    def EOF_raster_to_numpy(self, raster_list, nodata_to_value=None, export_to_npz=True, export_path=None):
        if not raster_list:
            print('ERROR: input rasters do not exist. Please check the inputs.')

            # logger output
            self.ES_logger.error('input rasters do not exist.')
            exit(1)

        print('Exporting rasters to npz...')
        for raster in tqdm(raster_list):
            self.do_EOF_raster_to_numpy(inRaster=raster,
                                        export_path=export_path,
                                        export_to_npz=export_to_npz,
                                        nodata_to_value=nodata_to_value)

            # logger output
            self.ES_logger.info('Raster saved to npz: {}'.format(raster))

    # 函数将输入的栅格转换为numpy数组, 同时返回转换成功的numpy 数组。
    # 可以通过export_to_npz参数控制是否将numpy数组保存为文件，若设定此参数请提供保存路径
    def do_EOF_raster_to_numpy(self, inRaster, export_path, export_to_npz=True, lower_left_corner=None, ncols=None, nrows=None, nodata_to_value=None):
        if not inRaster:
            print('ERROR: convert to numpy array failed! input raster does not exist: {}'.format(inRaster))

            # logger output
            self.ES_logger.error('input raster does not exist: {}'.format(inRaster))
            exit(1)

        # 检查待转换的栅格是否存在
        if not arcpy.Exists(inRaster):
            print('ERROR: convert to numpy array failed! input raster does not exist: {}'.format(inRaster))

            # logger output
            self.ES_logger.error('input raster does not exist: {}'.format(inRaster))
            exit(1)

        # 执行转换为numpy array
        temp_numpy_arr = arcpy.RasterToNumPyArray(in_raster=inRaster,
                                                lower_left_corner=lower_left_corner,
                                                ncols=ncols,
                                                nrows=nrows,
                                                nodata_to_value=nodata_to_value)
        
        # 重新将nan填充到numpy array
        temp_numpy_arr[temp_numpy_arr == nodata_to_value] = numpy.nan

        # 沿Y轴翻转整个数组
        # 翻转的原因是arcgis显示栅格的位置是左上角，与普通的笛卡尔坐标系在Y轴上相反
        temp_numpy_arr = numpy.flipud(temp_numpy_arr)
        
        # logger output
        self.ES_logger.debug('The numpy array size in memory are: {}'.format(temp_numpy_arr.nbytes))
        self.ES_logger.debug('Raster converted to numpy array: {}'.format(inRaster))

        # 保存结果到NPZ文件
        if export_to_npz:
            if not export_path:
                print('ERROR: export NPZ file path does not exist: {}'.format(export_path))

                # logger output
                self.ES_logger.error('Path does not exist: {}'.format(export_path))
                exit(1)

            # 设置保存路径
            temp_save_name = '{}.npz'.format(inRaster)
            temp_save_name = os.path.join(export_path, temp_save_name)

            # 执行保存
            numpy.savez_compressed(temp_save_name, temp_numpy_arr)

            # logger output 
            self.ES_logger.debug('Numpy array saved to NPZ: {}'.format(temp_save_name))
    
        # 返回结果
        return temp_numpy_arr

    # 将结果的mode数据转换为raster
    def EOF_hdf_mode_to_raster(self, hdf_path, category_field_list, mode_name, num_eofs, output_fmt='{}_mode_{}', remove_value=None):
        if not category_field_list:
            print('ERROR: emission categories does not exist. Please check exist.')

            # logger output
            self.ES_logger.error('emission categories does not exist.')
            exit(1)
        
        if not os.path.exists(hdf_path):
            print('ERROR: hdf file does not exist. Please check the input.')

            # logger output
            self.ES_logger.error('hdf file does not exist.')
            exit(1)
        
        if not num_eofs:
            print('ERROR: eof mode numbers does not specified. Please check the input.')

            # logger output
            self.ES_logger.error('eof mode numbers does not specified.')
            exit(1)

        
        for cate in category_field_list:
            # 按照场数量逐个输出
            for eof in range(0, num_eofs):
                # 检查输出文件名是否能够成功构建
                try:
                    output_raster = output_fmt.format(cate, eof)
                except:
                    print('ERROR: can not format raster output name.')

                    # logger output
                    self.ES_logger.error('raster output name format failed.')
                    exit(1)

                # 生成hdf数据路径
                temp_data_path = os.path.join('/EOF_mode/', cate)

                # 执行数据提取
                self.do_EOF_hdf_mode_to_raster(hdf_file_path=hdf_path, hierarchical_data_path=temp_data_path, data_name=mode_name, output_path=output_raster, mode=eof, remove_value=remove_value)


    # 从HDF5中的eof mode数据转换为Arcgis raster
    # 使用这个函数要注意output_path参数，如果在构造函数初始化的过程中定义了工作空间，则可以直接传入不带后缀的保存文件名，
    # 输出的栅格将直接保存到工作空间中；如果，需要输出到非初始化过程中定义的工作空间位置，则应该传入完整的保存路径，
    # 这里的完整保存路径是指包括文件后缀的文件绝对路径。
    def do_EOF_hdf_mode_to_raster(self, hdf_file_path, hierarchical_data_path, data_name, output_path, mode=0, remove_value=None):
        '''
        如果指定了remove_value参数的值，那么在导出数据的过程中将会把该指定值设定为NoData。
        使用这个函数要注意output_path参数，如果在构造函数初始化的过程中定义了工作空间，则可以直接传入不带后缀的保存文件名，
        输出的栅格将直接保存到工作空间中；如果需要输出到非初始化过程中定义的工作空间位置，则应该传入完整的保存路径，
        这里的完整保存路径是指包括文件后缀的文件绝对路径。
        '''
        if not os.path.exists(hdf_file_path):
            print('ERROR: hdf file does not exist. Please check the input.')

            # logger output
            self.ES_logger.error('hdf file does not exist.')
            exit(1)
        
        # 打开hdf文件
        hdf = h5py.File(hdf_file_path, 'r')
        
        # 组合得到完整数据路径
        # 由于奇怪的msys bug，在这里使用os.path.join会误判分隔符为“\\”
        # 所以只能改用格式化字符串
        # full_data_path = os.path.join(hierarchical_data_path, data_name)
        full_data_path = '{}/{}'.format(hierarchical_data_path, data_name)

        # 检查数据是否存在
        if full_data_path not in hdf:
            print('ERROR: data not exist. Please check the input.')

            # logger output
            self.ES_logger.error('data not exist.')
            exit(1)
        
        # 取得hdf中数据
        temp_numpy_data = hdf[full_data_path][mode,...]

        # numpy 转 raster
        temp_raster = arcpy.NumPyArrayToRaster(temp_numpy_data, x_cell_size=0.1, lower_left_corner=arcpy.Point(-180, -90))
        # 为raster定义坐标系
        arcpy.DefineProjection_management(temp_raster, arcpy.SpatialReference(4326))

        # 执行设定remove_value为NoData
        if remove_value is None:
            # 保存raster
            temp_raster.save(output_path)
            self.ES_logger.info('raster saved: {}'.format(output_path))
        else:
            temp_where_clause = 'VALUE = {}'.format(remove_value)
            temp_raster_setnull = SetNull(in_conditional_raster=temp_raster, in_false_raster_or_constant=temp_raster, where_clause=temp_where_clause)
            temp_raster_setnull.save(output_path)
            self.ES_logger.info('raster saved at {} and removed value \'{}\''.format(output_path, remove_value))

    # # 将arcgis栅格数据转换成Numpy multivariates-EOF计算所用的格式
    # def EOF_raster_to_numpy_multivariates(self, raster_list, nodata_to_value, export_to_npz=True, export_path=None):
    #     pass

    # 函数将输入的栅格转换为numpy数组, 同时返回转换成功的numpy 数组。
    # 注意：
    #       使用这个函数要求传入一个字典，位于字典一个键下的所有栅格将被按列合并到同一个numpy 数组中。
    #       同时，这个字典应该是有序的，键的顺序就是每个numpy 数组的排列顺序。
    # 如果不确定应该输入一个怎样的字典，可以使用EOF_multivariates_input_rasters_example()查看示例
    # 可以通过export_to_npz参数控制是否将numpy数组保存为文件，若设定此参数请提供保存路径
    def deprecated_EOF_raster_to_numpy_multivariates(self, inRasterDict, export_path, export_to_npz=True, lower_left_corner=None, ncols=None, nrows=None, nodata_to_value=None):
        # if not inRasterDict:
        #     # 按照字典中的键值生成
        #     print 'ERROR: incorrect inputs'

        #     # logger output
        #     self.ES_logger.error('incorrect inputs.')
        #     return
    
        # # 保存生成的结果
        # temp_result_arr = []

        # # 按照字典中的键值生成numpy 数组
        # # 注意：这里要特别小心处理每个字典键的值列表中元素的顺序，必须保证顺序——也就是分类排列顺序一致。
        # for key in inRasterDict.keys():
        #     for raster in inRasterDict[key]:
        #         # 执行转换为numpy array
        #         temp_numpy_arr = arcpy.RasterToNumPyArray(inRaster=raster,
        #                                                 lower_left_corner=lower_left_corner,
        #                                                 ncols=ncols,
        #                                                 nrows=nrows,
        #                                                 nodata_to_value=nodata_to_value)

        #         # 如果是存在结果数组则执行追加模式
        #         if isinstance(temp_result_arr, numpy.ndarray):
        #             numpy.append(temp_result_arr, temp_result_arr, 1)
        #         # 如果是第一次循环则
        #         else:
        #             del temp_result_arr
        #             temp_result_arr = temp_numpy_arr

        #     # logger output
        #     self.ES_logger.debug('Raster converted to numpy array: %s' % inRaster)

        #     # 保存结果到NPZ文件
        #     if export_to_npz:
        #         if not export_path:
        #             print 'ERROR: export NPZ file path does not exist: %s' % export_path

        #             # logger output
        #             self.ES_logger.error('Path does not exist: %s' % export_path)
        #             break

        #         # 设置保存路径
        #         temp_save_name = '%s.npz' % inRaster
        #         temp_save_name = os.path.join(export_path, temp_save_name)

        #         # 执行保存
        #         numpy.savez_compressed(temp_save_name, temp_numpy_arr)

        #         # logger output 
        #         self.ES_logger.debug('Numpy array saved to NPZ: %s' % temp_save_name)
        pass
    
    # 生成一个可供EOF_raster_to_numpy_multivariates()使用的字典
    # 使用`分类`和`名称`归类同一年份栅格
    # 通过`category_list`参数给出的顺序，确定最终生成的字典的。
    # 注意：
    #       使用这个函数的时候一定要确定category_list 列表的顺序是期望的顺序。之后的所有函数执行
    #       过程都将保持这个列表中元素的顺序。
    def deprecated_category_year_organize(self, raster_list, category_list, year_range):
        # if not raster_list or not category_list or not year_range:
        #     print 'ERROR: incorrect inputs'

        #     # logger output
        #     self.ES_logger.error('incorrect inputs.')
        #     return

        # # 检查输入年份变量参数是否合规
        # if (type(year_range[0]) != int) or (type(year_range[1]) != int):
        #     print 'Error! Processing starting year and ending year must be int value'
        #     self.ES_logger.info('Year setting type error.')
        #     self.ES_logger.error('Year setting error!')
        #     return
        # elif min(year_range) < self.__default_start_year or max(year_range) > self.__default_end_year:
        #     print 'Error! Processing year range out of data support! The year must contain in 1970 to 2018'
        #     self.ES_logger.info('Year settings are out of range.')
        #     self.ES_logger.error('Year setting error!')
        #     return
        # else:
        #     temp_start_year, temp_end_year = min(year_range), max(year_range)
        #     self.ES_logger.info('Year has set.')

        # # 保存输入category_list的顺序
        # category_ordered_list = enumerate(category_list)
        # # 结果字典的键名格式
        # temp_prefix = 'EOF_multivariates_%s'

        # # 返回结果字典
        # temp_result_dict = {}

        # # 注意：！！！
        # #   为了保持顺序，需要先使用enumerate()函数确认列表元素顺序。
        # for year in range(temp_start_year, temp_end_year + 1):
        #     # 通过python解包获得同年份的分类栅格
        #     temp_search = [r for r in raster_list if int(r[-4:]) == year]

        #     # 保存临时结果
        #     temp_rasters = temp_search

        #     # 按照category_list 的顺序对栅格在列表中的顺序进行排序
        #     for cate in category_ordered_list:
        #         # 以下这个操作有点匪夷所思，但是复杂执行的原因是为了严格保持列表的顺序
        #         # 首先找到对应位置的栅格名称
        #         # 这里用到了列表的解包操作。主要是不想写搜索……
        #         temp_r = [r for r in temp_search if cate[1] in r]
        #         # 手动将栅格名摆放到category_list规定的位置
        #         temp_rasters[cate[0]] = temp_r[0]

        #     # 生成键名
        #     temp_key = temp_prefix % year

        #     # 将结果添加到返回字典中
        #     temp_result_dict[temp_key] = temp_rasters
        
        # return temp_result_dict
        pass

    # 打印一个EOF multivariates推荐的输入数组样式
    def deprecated_EOF_multivariates_input_rasters_example(self):
        pass

    # 快速导出叠加了背景的栅格
    def EOF_export_raster(self, raster_wildcard, output_path):
        # 检查输出路径是否存在
        if not output_path:
            print('ERROR: output path does not exist, please check the input.')

            # logger output
            self.ES_logger.error('output path does not exist.')
            exit(1)

        # 检查输入参数是否存在
        if not raster_wildcard:
            print('WARNING: raster_wildcard is empty, which will export all rasters in database!')

            # logger output
            self.ES_logger.warning('raster wildcard is empty! All rasters will be export.')
            raster_wildcard = '*'
        
        # 列出待导出栅格
        temp_working_rasters = self.do_arcpy_list_raster_str(wildcard_str=raster_wildcard)

        # 执行导出
        self.raster_quick_export(raster_list=temp_working_rasters,
                                nodata_value=-999,
                                output_path=output_path,
                                output_formate='TIFF')



# ======================================================================
# ======================================================================
# TEST SCRIPT
# ======================================================================
# ======================================================================
if __name__ == '__main__':
    # merge_sectors test
    # aaa = EDGAR_spatial.merge_sectors('D:\\workplace\\geodatabase\\EDGAR_test.gdb',
    #    st_year=2018, en_year=2018, sectors=test_es, colormap=test_esc)
    # cProfile.run('aaa = EDGAR_spatial.merge_sectors(\'D:\\workplace\\geodatabase\\EDGAR_test.gdb\',st_year=2018, en_year=2018, sectors=test_es, colormap=test_esc)', 'merge_sector_init_profile.prof')
    # aaa.proccess_year(start_year=2018, end_year=2018)
    # cProfile.run('aaa.proccess_year(start_year=2018, end_year=2018)','merge_sector_init_profile.prof')

    print('main process')
