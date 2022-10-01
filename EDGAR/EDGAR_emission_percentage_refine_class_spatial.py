# -*- coding: utf-8 -*-

# 路径处理模块
# Systerm path proccessing module
import os
from typing import Tuple

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
import logging
import csv
import pandas

__metaclass__ = type

# ======================================================================
# ======================================================================
# Memorandum:
# 备忘录：
#   1. 考虑是否需要在构造函数中包含 arcpy 的几个环境变量的引入；
#   2. 所有涉及数据的操作都需要采用绝对路径，防止arcpy出现识别数据错误。
#   3. 计算字段的构造方法
# ======================================================================
# ======================================================================

# ======================================================================
# ======================================================================
# SPATIAL OPERATIONS CLASS
# ======================================================================
# ======================================================================


class EDGAR_spatial:
    ############################################################################
    ############################################################################
    # 构造函数部分
    # 注意：这里需要两类构造函数：
    # 1.默认构造函数：不需要传入任何参数。所有计算用到的参数均
    # 为默认值。
    # 2.带有数据位置的构造函数：需要传入一个
    ############################################################################
    ############################################################################
    def __init__(self, workspace, background_flag=True, background_flag_label='BA', background_raster='background', sector={}, colormap={}, st_year=1970, en_year=2018, log_path='EDGAR.log'):
        # 初始化logger记录类的全体工作
        # ES_logger为可使用的logging实例
        self.ES_logger = logging.getLogger()
        self.ES_logger.setLevel(level=logging.DEBUG)
        ES_logger_file = logging.FileHandler(log_path)
        ES_logger_formatter = logging.Formatter(
            '%(asctime)s-[%(levelname)s]-[%(name)s]-[%(funcName)s]-%(message)s')
        ES_logger_file.setFormatter(ES_logger_formatter)
        self.ES_logger.addHandler(ES_logger_file)

        self.ES_logger.info('==========EDGAR_Spatial start==========')

        # arcgis 工作空间初始化
        # 必须明确一个arcgis工作空间！
        # 初始化构造需要明确arcgis工作空间或者一个确定的数据为
        # 检查输入是否为空值
        if workspace == '':
            print 'Spatial direction or database path error! Please check your input!'
            self.ES_logger.error('arcpy environment workspace set failed!')
            return

        # 为工作空间进行赋值
        # 这里需要为两个参数赋值：第一个参数是系统中arcpy environment workspace 参数，
        # 该参数保证了进行arcgis空间运算的“空间分析扩展”检查通过；第二个参数是为了
        # 缩短代码中“arcpy.env.workspace”属性的书写长度而设置的代用变量。
        self.__workspace = workspace
        arcpy.env.workspace = workspace
        self.ES_logger.info('workpace has set.')
        # 利用栅格计算器进行栅格代数计算时需要先检查是否开启了空间扩展
        arcpy.CheckOutExtension('Spatial')
        self.ES_logger.info('arcpy Spatial extension checked.')
        # 将多线程处理设置为100%
        #   吐槽：虽然没什么用，cpu利用率最多也只能达到5%
        arcpy.env.parallelProcessingFactor = "100%"
        self.ES_logger.info('arcpy parallelProcessingFactor set to 100%.')

        # EDGAR_sector 参数初始化部分
        # 检查输入参数类型
        # 默认情况下使用默认参数初始化
        # 为EDGAR_sector参数赋值
        if type(sector) != dict:
            print 'Error! EDGAR_sector only accept a dictionary type input.'
            self.ES_logger.info(
                'EDGAR_sector only accept a dictionary type input.')
            self.ES_logger.error('EDGAR_sector type error.')
            return
        elif sector == {}:
            self.EDGAR_sector = copy.deepcopy(self.__default_EDGAR_sector)
            self.ES_logger.info('This run use default EDGAR sector setting.')
            self.ES_logger.info('EDGAR_sector has set.')
        else:
            self.EDGAR_sector = copy.deepcopy(sector)
            self.ES_logger.info('EDGAR_sector has set.')

        # EDGAR_sector_colormap 参数初始化部分
        # 检查参数输入类型
        # 默认情况下使用默认参数初始化
        # 为EDGAR_sector_colormap 参数赋值
        if type(colormap) != dict:
            print 'Error! EDGAR_sector_colormap only accept a dictionary type input.'
            self.ES_logger.info(
                'EDGAR_sector_colormap only accept a dictionary type input.')
            self.ES_logger.error('EDGAR_sector_colormap type error.')
            return
        elif sector == {}:
            self.EDGAR_sector_colormap = copy.deepcopy(
                self.__default_EDGAR_sector_colormap)
            self.ES_logger.info(
                'This run use default EDGAR sector colormap setting.')
            self.ES_logger.info('EDGAR_sector_colormap has set.')
        else:
            self.EDGAR_sector_colormap = copy.deepcopy(colormap)
            self.ES_logger.info('EDGAR_sector_colormap has set.')

        # year_range 参数初始化部分
        # 这里需要初始化计算的起始和结束
        if (type(st_year) != int) or (type(en_year) != int):
            print 'Error! Proccessing starting year and ending year must be int value'
            self.ES_logger.info('Year setting type error.')
            self.ES_logger.error('Year setting error!')
            return
        elif st_year < 1970 or en_year > 2018:
            print 'Error! Proccessing year range out of data support! The year must containt in 1970 to 2018'
            self.ES_logger.info('Year settings are out of range.')
            self.ES_logger.error('Year setting error!')
            return
        else:
            self.start_year, self.end_year = st_year, en_year
            self.ES_logger.info('Year has set.')

        # background 参数初始化部分
        # 这里要明确处理的数据是否包含背景0值
        # 检查并赋值label
        if bool(background_flag) == True:
            if type(background_flag_label) == str:
                if arcpy.Exists(background_raster):
                    self.background_flag = bool(background_flag)
                    self.background_label = background_flag_label
                    self.background_raster = background_raster
                    self.ES_logger.debug('Background has set.')
            else:
                print 'Error: Please check background flag or label or raster.'
                self.ES_logger.error('Background setting error!')
                return
        elif bool(background_flag) == False:
            self.background_flag = bool(background_flag)
            self.background_label = ''
            self.background_raster = ''
            self.ES_logger.debug('Background has set.')
        else:
            self.ES_logger.error('Background setting error!')
            return

        # raster_filter 参数初始化部分
        # 这里要将初始化传入的部门参数字典“sector”进行列表化并赋值
        # 和起始、终止时间传入
        temp_init_filter_label = {'default_set': True, 'background_label_set': self.background_label,
                                  'sector_set': self.EDGAR_sector,
                                  'start_year_set': self.start_year,
                                  'end_year_set': self.end_year}
        self.filter_label = temp_init_filter_label
        self.ES_logger.info('filter_label has set.')

        print 'EDGAR_Spatial initialized! More debug information please check the log file.'
        self.ES_logger.info('Initialization finished.')
        self.ES_logger.debug('==========DEGUG INFORMATIONS==========')
        self.ES_logger.debug('acrpy.env.workspace:%s' % arcpy.env.workspace)
        self.ES_logger.debug('arcpy parallelProcessingFactor:%s' %
                             arcpy.env.parallelProcessingFactor)
        self.ES_logger.debug('EDGAR_sector was set to:%s' % self.EDGAR_sector)
        self.ES_logger.debug(
            'EDGAR_sector_colormap was set to:%s' % self.EDGAR_sector_colormap)
        self.ES_logger.debug('Processing begains in year:%s' % self.start_year)
        self.ES_logger.debug('Processing ends in year:%s' % self.end_year)
        self.ES_logger.debug('Raster has background:%s' % self.background_flag)
        self.ES_logger.debug(
            'Raster name\'s background label is:%s' % self.background_label)
        self.ES_logger.debug('Background raster is:%s' %
                             self.background_raster)
        self.ES_logger.debug(
            'Raster filter parameters was set to:%s' % self.filter_label)
        self.ES_logger.debug('==========DEGUG INFORMATIONS==========')

    ############################################################################
    ############################################################################
    # 默认参数
    # Default values
    ############################################################################
    ############################################################################

    # Class logger
    ES_logger = logging.getLogger()

    # Arcgis workspace
    __workspace = ''

    # EDGAR sector dicts & colormap dicts
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
    EDGAR_sector = {}
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
    EDGAR_sector_colormap = {}

    # 默认时间范围
    __default_start_year = 1970
    __default_end_year = 2018

    # 时间范围
    start_year = 0
    end_year = 0

    # 默认栅格数据背景零值标识和区分标签
    __background_flag = True
    __background_label = 'BA'
    __background_raster = 'background'

    # 栅格数据背景零值标识和区分标签
    background_flag = __background_flag
    background_label = __background_label
    background_raster = __background_raster

    # 默认过滤标签
    __default_filter_label_dict = {'default': True, 'label': {'background_label': __background_label,
                                                              'sector': __default_EDGAR_sector,
                                                              'start_year': __default_start_year,
                                                              'end_year': __default_end_year}}
    # 过滤标签
    filter_label_dict = __default_filter_label_dict

    # 数据库栅格数据筛选过滤标签
    # 默认数据库过滤标签
    __default_raster_filter_wildcard = []
    # 数据库过滤标签
    raster_filter_wildcard = __default_raster_filter_wildcard

    # 准备时间范围内的所有部门的栅格
    all_prepare_working_rasters = []

    # 需要操作的栅格
    working_rasters = []

    ############################################################################
    ############################################################################
    # 参数设定部分
    ############################################################################
    ############################################################################
    # 想要自定义或者修改处理的部门排放需要使用特殊的set函数
    def set_EDGAR_sector(self, sector):
        if type(sector) != dict:
            print 'Error type! EDGAR sectors should be dictionary!'
            self.ES_logger.error(
                'Error type! EDGAR sectors should be dictionary.')
            return

        self.EDGAR_sector = sector

        # logger output
        self.ES_logger.debug('EDGAR_sector changed to:%s' % sector)

    def get_EDGAR_sector(self):
        print self.EDGAR_sector

    sector = property(get_EDGAR_sector, set_EDGAR_sector)

    def set_EDGAR_sector_colormap(self, sector_colormap):
        if type(sector_colormap) != dict:
            print 'Error type! EDGAR sectors colormap should be diectionary!'
            self.ES_logger.error(
                'Error type! EDGAR sectors colormap should be diectionary.')
            return

        self.EDGAR_sector_colormap = sector_colormap

        # logger output
        self.ES_logger.debug(
            'EDGAR_sector_colormap changed to:%s' % sector_colormap)

    def get_EDGAR_sector_colormap(self):
        print self.EDGAR_sector_colormap

    sector_colormap = property(
        get_EDGAR_sector_colormap, set_EDGAR_sector_colormap)

    def set_year_range(self, start_end=(1970, 2018)):
        self.start_year, self.end_year = start_end

        # logger output
        self.ES_logger.debug('year range changed to:%s' % start_end)

    def get_year_range(self):
        print 'Start year: %s\nEnd year: %s' % (self.start_year, self.end_year)

    year_range = property(get_year_range, set_year_range)

    # 栅格图像背景值设置和查看属性/函数
    def set_background(self, flag_label_raster_dict):
        # 检查flag参数并赋值
        # 关闭background，即栅格不包含背景0值
        if bool(flag_label_raster_dict['flag']) == False:
            try:
                self.background_flag = bool(flag_label_raster_dict['flag'])
                self.background_label = ''
                print 'Background value flag closed!'

                # logger output
                self.ES_logger.debug('Backgroud closed.')
            except:
                print 'Background flag set failed! Please check the flag argument input.'
                self.ES_logger.error(
                    'Background flag set failed! Please check the flag argument input.')
        # 开启background，即栅格包含背景0值
        elif bool(flag_label_raster_dict['flag']) == True:
            # 检查flag参数并赋值
            try:
                self.background_flag = bool(flag_label_raster_dict['flag'])

                # logger output
                self.ES_logger.debug('Backgroud opened.')
            except:
                print 'Background flag set failed! Please check the flag argument input.'
                self.ES_logger.error(
                    'Background flag set failed! Please check the flag argument input.')

            # 检查flag_label参数并
            if type(flag_label_raster_dict['label']) == str:
                self.background_label = flag_label_raster_dict['label']

                # logger output
                self.ES_logger.debug(
                    'Backgroud label changed to:%s' % flag_label_raster_dict['label'])
            else:
                print 'Background flag label set failed! Please check the flag argument input.'
                self.ES_logger.error(
                    'Background flag set failed! Please check the flag argument input.')

            # 检查raster参数并
            if type(flag_label_raster_dict['raster']) == str:
                if arcpy.Exists(flag_label_raster_dict['raster']):
                    self.background_raster = flag_label_raster_dict['raster']

                    # logger output
                    self.ES_logger.debug(
                        'Backgroud label changed to:%s' % flag_label_raster_dict['raster'])
                else:
                    print 'Background raster set failed! The background raster dose not exits.'
                    self.ES_logger.error(
                        'Background raster set failed! The background raster dose not exits.')
            else:
                print 'Background flag label set failed! Please check the flag argument input.'
                self.ES_logger.error(
                    'Background flag set failed! Please check the flag argument input.')

    def get_background(self):
        # 这里直接返回一个元组，包括背景栅格的三个信息，开启，标签，空白栅格名称
        return (self.background_flag, self.background_label, self.background_raster)

    background = property(get_background, set_background)

    # 类中提供了两个过滤标签的构造方法
    # 1. 本人生成的数据保存的格式，例如：‘BA_EDGAR_TNR_Aviation_CDS_2010’，其中‘BA’代表包含背景值，数据名结尾
    #    字符串为‘部门_年份’。
    # 2. 自定义标签格式。可以根据用户已有的数据的名称进行筛选。请注意：筛选字符串需要符合 Arcpy 中 wild_card定义的标准进行设定。
    def build_raster_filter_default(self, background_label, sector, start_year, end_year):
        # 检查年份设定是否为整数。（其他参数可以暂时忽略，因为默认格式下基本不会改变）
        if (type(start_year) != int) or (type(end_year) != int):
            print 'Error: Year setting error!'
            self.ES_logger.error(
                'Year setting error. Year settings must be integer and between 1970 to 2018.')
            return

        temp_time_range = range(start_year, end_year+1)

        # 这里使用了python列表解析的方法来生成部门和年份逐一配的元组。
        # 生成的元组个数应该为‘部门数量’*‘年份数量’
        # 注意！！！
        # 这里生成的列表中的元素是元组，该元组中包含[0]号元素为部门，[1]号元素为年份
        temp_sector_year_tupe_list = [(se, yr)
                                      for se in sector for yr in temp_time_range]

        # 逐年逐部门生成筛选条件语句，并保存到raster_filter_wildcard中
        for i in temp_sector_year_tupe_list:
            temp_raster_filter_wildcard = '%s*%s_%s' % (
                background_label, i[0], i[1])
            self.raster_filter_wildcard.append(temp_raster_filter_wildcard),

        # logger output
        self.ES_logger.debug('raster_filter set by default.')

    def build_raster_filter_costum(self, custom_label):
        # 对于自定义筛选条件，只需要检查是否为字符串
        if type(custom_label) != str:
            print "arcpy.ListRasters() need a string for 'wild_card'."
            self.ES_logger.error(
                'Wild_card set faild. The wild_card string must follow the standard of arcgis wild_card rules.')
            return
        self.raster_filter_wildcard = custom_label

        # logger output
        self.ES_logger.debug('raster_filter set by costum.')

    # filter_label 构造方法：
    # filter_label字典组的构造如下：
    # 'default':接受一个符合布尔型数据的值，其中True表示使用
    # 默认方式构造筛选条件；
    # 'label':该参数中应该保存需要的筛选条件语句。
    # 注意！！！：
    # 如果使用默认方式构造筛选条件，则label参数
    # 应该包含由以下标签构成的字典：
    # 'background_label'：数据是否为包括空值数据；
    # 'sector'：部门标签列表list或者str
    # 'start_year'：起始年份
    # 'end_year'：结束年份
    def set_filter_label(self, filter_label):
        # 检查default set，并赋值
        if bool(filter_label['default_set']) == True:
            self.filter_label_dict['default'] = True

            # logger output
            self.ES_logger.debug('filter label will set by default.')
        elif bool(filter_label['default_set']) == False:
            self.filter_label_dict['default'] = False

            # logger output
            self.ES_logger.debug('filter label will set by costum.')
        else:
            print 'default set error. Please check default_set argument.'
            self.ES_logger.error(
                'default set error. default_set argument need a bool type input.')

        # 检查background label 并赋值
        if bool(filter_label['background_label_set']) == True:
            self.filter_label_dict['label']['background_label'] = self.background[1]

            # logger output
            self.ES_logger.debug(
                'filter label will containt background value.')
        elif bool(filter_label['background_label_set']) == False:
            self.filter_label_dict['label']['background_label'] = ''

            # logger output
            self.ES_logger.debug(
                'filter label will NOT containt background value.')
        else:
            print 'background label set error. Please check backgroud_label_set argument.'
            self.ES_logger.error(
                'background label set error. The background_label_set need a dict type input. More information please refere the project readme.md files.')

        # 检查sector是否为str或者dict
        if (type(filter_label['sector_set']) == str) or (type(filter_label['sector_set']) == dict):
            self.filter_label_dict['label']['sector'] = filter_label['sector_set']

            # logger output
            self.ES_logger.debug('filter_label changed to:%s' %
                                 filter_label['sector_set'])
        else:
            print 'filter_label: sector setting error! sector only accept string or dictionary type.'
            self.ES_logger.error(
                'filter label set error. The filter_label need a dict or a list type input. More information please refere the project readme.md files.')

        # 检查start_year 和 end_year
        if (type(filter_label['start_year_set']) != int) or (type(filter_label['end_year_set']) != int):
            print 'filter_label: year setting error! please check year arguments'
            self.ES_logger.error(
                'year error. The star year and end year must be integer. More information please refere the project readme.md files.')
            return
        else:
            self.filter_label_dict['label']['start_year'] = filter_label['start_year_set']
            self.filter_label_dict['label']['end_year'] = filter_label['end_year_set']

            # logger output
            self.ES_logger.debug('filter_label year range changed to:%s to %s' % (
                filter_label['start_year_set'], filter_label['end_year_set']))

    def get_filter_label(self):
        return self.filter_label_dict

    filter_label = property(get_filter_label, set_filter_label)

    # 注意：这里需要为set函数传入一个filter_label字典
    def set_raster_filter(self, filter_label):
        # 判断是否为默认标签，是则调用默认的构造
        if filter_label['default'] == True:
            # 这里使用python的**kwags特性，**操作符解包字典并提取字典的值。
            self.build_raster_filter_default(**filter_label['label'])

            # logger output
            self.ES_logger.debug('filter_label changed by default function.')
        # 判断是否为默认标签，否则直接赋值为标签数据
        elif filter_label['default'] == False:
            self.build_raster_filter_costum(filter_label['label'])

            # logger output
            self.ES_logger.debug('filter_label changed by costum function.')
        else:
            print 'Error: raster filter arguments error.'

    def get_raster_filter(self):
        return self.raster_filter_wildcard

    raster_filter = property(get_raster_filter, set_raster_filter)

    # 生成需要处理数据列表
    def prepare_working_rasters(self, raster_filter_wildcard):
        # 涉及arcpy操作，且所有数据都基于这步筛选，
        # 所以需要进行大量的数据检查。
        temp_type_check = type(raster_filter_wildcard)

        # 传入列表情况
        if temp_type_check == list:
            # 列表为空
            if raster_filter_wildcard == []:
                # 显示警告：这个操作会列出数据库中的所有栅格
                print 'WARNING: No fliter! All rasters will be list!'

                # 使用str方式列出所有栅格
                self.do_prepare_arcpy_list_raster_str(wildcard_str='')

                # logger output
                self.ES_logger.debug('rasters listed without filter.')
            # 列表不为空的情况
            else:
                # 直接将参数传入list方式的方法列出需要栅格
                self.do_prepare_arcpy_list_raster_list(
                    wildcard_list=raster_filter_wildcard)

                # logger output
                self.ES_logger.debug('rasters listed.')
        # 传入单一字符串情况
        elif temp_type_check == str:
            # 显示筛选列表警告:
            # 如果为空值则警告可能会对所有栅格进行操作：
            if raster_filter_wildcard == '':
                print 'WARNING: No fliter! All rasters will be list!'

            self.do_prepare_arcpy_list_raster_str(
                wildcard_str=raster_filter_wildcard)

            # logger output
            self.ES_logger.debug('rasters listed without filter.')
        else:
            # 其他情况直接退出
            print 'Error: No fliter! Please check input raster filter!'
            self.ES_logger.error(
                'No fliter! Please check input raster filter!')
            return

    # 准备栅格时实际执行列出栅格的方法，这个为str方式
    def do_prepare_arcpy_list_raster_str(self, wildcard_str):
        self.all_prepare_working_rasters.extend(
            arcpy.ListRasters(wild_card=wildcard_str))

        # logger output
        self.ES_logger.debug('working rasters chenged to:%s' %
                             self.all_prepare_working_rasters)

    # 准备栅格时实际执行列出栅格的方法，这个为list方式
    def do_prepare_arcpy_list_raster_list(self, wildcard_list):
        # 逐年份生成需要处理的数据列表
        for i in wildcard_list:
            self.all_prepare_working_rasters.extend(
                arcpy.ListRasters(wild_card=i))

        # logger output
        self.ES_logger.debug('working rasters chenged to:%s' %
                             self.all_prepare_working_rasters)

    # 实际执行列出栅格的方法，这个为str方式
    def do_arcpy_list_raster_str(self, wildcard_str):
        self.working_rasters.extend(arcpy.ListRasters(wild_card=wildcard_str))

        # logger output
        self.ES_logger.debug('working rasters chenged to:%s' %
                             self.working_rasters)

    # 实际执行列出栅格的方法，这个为list方式
    def do_arcpy_list_raster_list(self, wildcard_list):
        # 逐年份生成需要处理的数据列表
        for i in wildcard_list:
            self.working_rasters.extend(arcpy.ListRasters(wild_card=i))

        # logger output
        self.ES_logger.debug('working rasters chenged to:%s' %
                             self.working_rasters)

    # 将部门key和对应的栅格文件组合为一个字典
    # 注意这个函数只能使用在确定了年份的列表中。
    # 如果暴力使用这个函数返回的字典将没有任何意义。
    def zip_sector_raster_to_dict(self, sector_list, raster_list):
        # 函数返回的字典
        sector_raster_dict = {}

        for s in sector_list:
            temp_regex = re.compile('%s' % s)
            # 注意这里filter函数返回的是一个list，
            # 需要取出其中的值赋值到字典中
            temp_value = filter(temp_regex.search, raster_list)
            sector_raster_dict[s] = temp_value.pop()

        return sector_raster_dict

    ############################################################################
    ############################################################################
    # 实际数据计算相关函数/方法
    ############################################################################
    ############################################################################

    # 生成arcgis需要的工作环境
    def generate_working_environment(self):
        arcpy.CheckOutExtension('Spatial')

    # 检查arcgis工作环境是否完整
    def check_working_environment(self):
        # 利用栅格计算器进行栅格代数计算时需要先检查是否开启了空间扩展
        arcpy.CheckOutExtension('Spatial')

    # 生成需要计算的栅格列表
    def prepare_raster(self):
        # 首先构造用于筛选raster用的wildcard
        self.raster_filter = self.filter_label
        # 通过arcpy列出需要的栅格
        self.prepare_working_rasters(self.raster_filter)

    # 栅格叠加的实际执行函数
    # 这个函数用到了tqdm显示累加进度
    def do_raster_add(self, raster_list, result_raster):
        if type(result_raster) != str:
            print 'Raster add: The output result raster path error.'

            # logger output
            self.ES_logger.error('The output result raster path error.')
            return

        # 将列表中的第一个栅格作为累加的起始栅格
        temp_raster = arcpy.Raster(raster_list[0])
        raster_list.pop(0)

        # 累加剩余栅格
        for r in tqdm(raster_list):
            temp_raster = temp_raster + arcpy.Raster(r)

            # logger output
            self.ES_logger.debug('Processing raster:%s' % r)

        # logger output
        self.ES_logger.info('Rasters added: %s' % raster_list)
        return temp_raster.save(result_raster)

    # 函数需要传入一个包含需要叠加的部门列表list，
    # 以及执行操作的年份
    def year_sectors_merge(self, raster_list, merge_sector, year):
        # 筛选需要计算的部门
        # 这里是通过构造正则表达式的方式来筛选列表中符合的元素
        temp_sector = ''
        for i in merge_sector:
            temp_sector = temp_sector + '|%s' % i

        temp_sector = temp_sector[1:len(temp_sector)]
        temp_sector_year = '(%s).*%s' % (temp_sector, year)
        filter_regex = re.compile(temp_sector_year)

        # 吐槽：神奇的python语法~~~
        temp_merge_raster = [s for s in raster_list if filter_regex.search(s)]

        # logger output
        self.ES_logger.debug('mergeing rasters: %s' % temp_merge_raster)

        # 此处输出的总量数据文件名不可更改！！！
        # 未来加入自定义文件名功能
        result_year = 'total_emission_%s' % year

        # 执行栅格数据累加
        self.do_raster_add(temp_merge_raster, result_year)

        # logger output
        self.ES_logger.info('year_sectors_merge finished!')

    # 实用（暴力）计算全年部门排放总和的函数
    def year_total_sectors_merge(self, year):
        # 列出全部门名称
        temp_sector = list(self.EDGAR_sector.values())

        # 执行部门累加
        self.year_sectors_merge(
            self.all_prepare_working_rasters, temp_sector, year)

        print 'Total emission of %s saved!\n' % year

        # logger output
        self.ES_logger.info('All sector merged!')

    # 删除临时生成的图层文件
    def delete_temporary_feature_classes(self, feature_list):
        print 'Deleting temporary files'

        prepare_feature = [s for s in feature_list if arcpy.ListFeatureClasses(
            wild_card=s, feature_type=Point)]

        for f in tqdm(prepare_feature):
            # 这里可能涉及一个arcpy的BUG。在独立脚本中使用删除图层工具时
            # 需要提供完整路径，即使你已经设置了env.workspace。
            # 而且在删除的时候不能使用deletefeature！
            # 需要使用delete_management.
            feature_fullpath = os.path.join(self.__workspace, f)
            arcpy.Delete_management(feature_fullpath)

            # logger output
            self.ES_logger.debug('Deleted feature:%s' % f)

        print 'Deleting temporary files finished!'

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
        # 这里要注意，总量栅格的名称在year_sector_merge()中写死了
        temp_year_total = arcpy.Raster('total_emission_%s' % year)
        temp_sector_wildcard = '%s*%s*%s' % (self.background[1], sector, year)
        temp_sector_emission = arcpy.Raster(
            arcpy.ListRasters(wild_card=temp_sector_wildcard)[0])

        # 检查输入的部门栅格和总量栅格是否存在，如果不存在则报错并返回
        if not (arcpy.Exists(temp_sector_emission)) or not (arcpy.Exists(temp_year_total)):
            print 'Sector_emission_percentage: Error! sector emission or year total emission raster does not exist.'

            # logger output
            self.ES_logger.error(
                'sector emission or year total emission raster does not exist.')
            return

        # 计算部门排放相对于全体部门总排放的比例
        # 注意！！！
        # 这里涉及除法！0值的背景会被抹去为nodata。所以要再mosaic一个背景上去才能转化为点。
        temp_output_weight_raster = temp_sector_emission / temp_year_total

        # logger output
        self.ES_logger.debug('Sectal raster weight calculated:%s' % sector)

        # Mosaic 比例计算结果和0值背景
        # Mosaic 的结果仍然保存在temp_output_weight_raster中
        arcpy.Mosaic_management(inputs=[temp_output_weight_raster, self.background[2]],
                                target=temp_output_weight_raster,
                                mosaic_type="FIRST",
                                colormap="FIRST",
                                mosaicking_tolerance=0.5)

        # logger output
        self.ES_logger.debug('Sectal raster weight mosaic to 1800*3600.')

        # 保存栅格格式权重计算结果
        temp_output_weight_raster_path = '%s_weight_raster_%s' % (sector, year)
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
        # The error will throll an exception of NOT found table in raster,
        # because of the RasterToPoint_conversion miss include a deleted
        # anonymous temporary variable.
        #######################################################################
        #######################################################################
        del temp_output_weight_raster
        #######################################################################
        #######################################################################

        print 'Sector emission weight raster saved: %s\n' % sector

        # logger output
        self.ES_logger.info('Sector emission weight raster saved')

        # 栅格数据转点对象。转为点对象后可以实现计算比例并同时记录对应排放比例的部门名称
        # 这里用到了arcpy.AlterField_management()这个函数可能在10.2版本中没有
        try:
            # transform to point features
            arcpy.RasterToPoint_conversion(
                temp_output_weight_raster_path, output_sector_point, 'VALUE')

            # logger output
            self.ES_logger.debug(
                'Sector emission weight raster convert to point:%s' % sector)

            # rename value field
            arcpy.AlterField_management(
                output_sector_point, 'grid_code', new_field_name=sector)
            # 删除表链接结果结果中生成的统计字段'pointid'和'grid_code'
            arcpy.DeleteField_management(output_sector_point, 'pointid')

            # logger output
            self.ES_logger.debug(
                'Sector emission weight point fields cleaned:%s' % sector)

            print 'Sector raster convert to pointfinished: %s of %s' % (sector, year)
        except:
            print 'Failed sector to point : %s' % sector

            # logger output
            self.ES_logger.error(
                'Raster weight converting to point failed:%s' % sector)

            print arcpy.GetMessages()

    # 计算一年中所有部门的比例
    def year_sector_emission_percentage(self, year):
        for s in tqdm(self.EDGAR_sector):
            # 设定输出点数据的格式
            output = '%s_weight_%s' % (s, year)
            self.sector_emission_percentage(s, year, output)

    # 将同一年份的部门整合到同一个点数据图层中
    def year_weight_joint(self, year, sector_list):
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
        output_sectoral_weights = 'sectoral_weights_%s' % year
        # 设定需要删除的临时生成文件列表
        delete_temporary = []

        # 筛选需要计算的部门
        # 列出提取值的栅格
        # do_arcpy_list_raster_list的结果会保存到self.working_rasters
        temp_wildcard_pair = zip(sector_list, [str(year)]*len(sector_list))
        temp_wildcard = ['%s_weight_raster_%s' % i for i in temp_wildcard_pair]
        self.do_arcpy_list_raster_list(wildcard_list=temp_wildcard)
        temp_extract_raster = self.zip_sector_raster_to_dict(
            sector_list, self.working_rasters)

        # logger output
        self.ES_logger.debug('Calculate weight in: %s' %
                             str(temp_extract_raster))

        # 首先弹出一个部门作为合并的起始指针
        temp_point_start_wildcard = '%s*%s' % (
            temp_extract_raster.popitem()[0], year)
        # 这里的逻辑看似有点奇怪，其实并不奇怪。
        # 因为在sector_emission_percentage()中已经保存了完整的‘部门-年份’点数据
        # 所以可以用其中一个点数据作为提取的起点。
        temp_point_start = arcpy.ListFeatureClasses(wild_card=temp_point_start_wildcard,
                                                    feature_type=Point).pop()

        # logger output
        self.ES_logger.debug('First weight extract point:%s' %
                             temp_point_start)

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
        try:
            # 构建启动循环的第一次提取
            # 从字典中pop出一部门，保存部门名称和对应的待提取值栅格
            temp_ETP_1_sector, temp_ETP_1_raster = temp_extract_raster.popitem()
            arcpy.sa.ExtractValuesToPoints(in_point_features=temp_point_start,
                                           in_raster=temp_ETP_1_raster,
                                           out_point_features=temp_point_trigger,
                                           interpolate_values='NONE',
                                           add_attributes='VALUE_ONLY')

            # 提取成功以后需要将RASTERVALU字段改为部门名称
            arcpy.AlterField_management(in_table=temp_point_trigger,
                                        field='RASTERVALU',
                                        new_field_name=temp_ETP_1_sector)
            # logger output
            self.ES_logger.debug('Extract trigger built:%s' %
                                 temp_ETP_1_raster)
        except:
            print 'Error: Extract value to point failed! The trigger building failed.'

            # logger output
            self.ES_logger.error('The trigger building failed.')

            print arcpy.GetMessages()

        for sect in tqdm(temp_extract_raster):
            try:
                # 从字典中获得部门和对应的待提取值栅格
                temp_ETP_raster = temp_extract_raster[sect]
                temp_point_output = temp_point_iter_root + sect

                arcpy.sa.ExtractValuesToPoints(in_point_features=temp_point_trigger,
                                               in_raster=temp_ETP_raster,
                                               out_point_features=temp_point_output,
                                               interpolate_values='NONE',
                                               add_attributes='VALUE_ONLY')

                # 提取成功以后需要将RASTERVALU字段改为部门名称
                arcpy.AlterField_management(in_table=temp_point_output,
                                            field='RASTERVALU',
                                            new_field_name=sect)
                # logger output
                self.ES_logger.debug('Extract raster:%s' %
                                     temp_extract_raster[sect])

                # 交换temp_point_iter和temp_point_output指针
                temp_point_trigger = temp_point_output

                # 添加到删除名单
                delete_temporary.append(temp_point_output)

            except:
                print 'Error: Extract value to point failed!'

                # logger output
                self.ES_logger.error('Extract raster failed:%s' %
                                     temp_extract_raster[sect])

                print arcpy.GetMessages()

        # 保存最后的输出结果
        print 'Saving sectoral weights...'
        arcpy.CopyFeatures_management(
            temp_point_output, output_sectoral_weights)
        print 'Sectoral weights finished:%s' % year

        # logger output
        self.ES_logger.debug('Sectoral weights saved:%s' %
                             output_sectoral_weights)

        # 删除临时生成的迭代变量
        self.delete_temporary_feature_classes(delete_temporary)

        # 清空全局working_rasters变量，防止突发bug
        self.working_rasters = []

        # logger output
        self.ES_logger.debug('working_rasters cleaned!')

    # 导出不同年份最大权重栅格

    def max_weight_rasterize(self, year):
        temp_point = 'sectoral_weights_%s' % year
        save_raster_categories = 'main_emi_%s' % year
        save_raster_weight = 'main_emi_weight_%s' % year

        # 向point feature中添加列
        # 1.权重最大值 wmax
        # 2.权重最大值名称 wmaxid
        # 3.将权重最大值名称映射为一个整数，方便输出为栅格 wraster
        # 4.统计一个栅格中共计有多少个部门排放
        # 并计算添加字段的值
        temp_new_fields = ['wmax', 'wmaxid', 'wraster', 'sector_counts']
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
            # logger output
            self.ES_logger.debug(
                'Max-Classes fields added:%s' % temp_new_fields)
        except:
            print 'Add field to point faild in: %s' % temp_point

            # logger output
            self.ES_logger.error(
                'Add field to point faild in: %s' % temp_point)

            print arcpy.GetMessages()
            return

        # 这里的year参数可能需要删除aa
        self.do_sector_max_extract(temp_point, temp_new_fields)

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

            # logger output
            self.ES_logger.debug('Max-Classes rasterize finished:%s' % year)
        except:
            print 'Create main emission raster field: %s' % temp_point

            # logger output
            self.ES_logger.error('Max-Classes rasterize failed:%s' % year)

            print arcpy.GetMessages()

    # 用arcpy.da.cursor类进行操作
    # 在一行中同时实现找到最大值，最大值对应的id，最大值对应的colormap
    def do_sector_max_extract(self, sector_points, calculate_fields):
        temp_sector = copy.deepcopy(self.EDGAR_sector)
        temp_sector_colormap = copy.deepcopy(self.EDGAR_sector_colormap)
        temp_working_sector = sector_points

        # 构造需要操作的字段
        # 神奇的python赋值解包
        temp_cursor_fileds = [i for i in temp_sector]

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
                max_weight = max(row[0:-calculate_fields_counts])
                max_id = temp_cursor_fileds[row.index(max_weight)]
                max_colormap = temp_sector_colormap[max_id]
                emitted_sectors = len(
                    [i for i in row[0:-calculate_fields_counts] if i != 0])

                row[-1] = emitted_sectors
                row[-2] = max_colormap
                row[-3] = max_id
                row[-4] = max_weight

                cursor.updateRow(row)

    # 处理给定年份范围内的工作
    # 批量处理可以使用这个函数
    def proccess_year(self, start_year, end_year):
        # 首先需要列出所有需要使用到的栅格
        self.prepare_raster()

        # 逐年处理
        for yr in range(start_year, end_year+1):
            self.print_start_year(yr)
            self.year_total_sectors_merge(yr)
            self.year_sector_emission_percentage(yr)
            self.year_weight_joint(yr, self.EDGAR_sector)
            self.max_weight_rasterize(yr)
            self.print_finish_year(yr)

    # 暴力处理所有年份
    def proccess_all_year(self):
        self.proccess_year(start_year=1970, end_year=2018)

    def print_start_year(self, year):

        # logger output
        self.ES_logger.debug('Processing start of year %s' % year)

        print '=============================='
        print '=============================='
        print 'Processing start of year %s' % year
        print '=============================='
        print '=============================='

    def print_finish_year(self, year):

        # logger output
        self.ES_logger.debug('Finished processing data of year %s' % year)

        print '=============================='
        print '=============================='
        print 'Congratulations!'
        print 'Finished processing data of year %s' % year
        print '=============================='
        print '=============================='

    # 这个函数实际执行从一个年份中提取中心操作
    # 这里要求可以center_range是一个元组
    def do_extract_center_area(self,center_range, total_emission_raster, year):
        # 临时变量
        temp_center_upper_bound = 0
        temp_center_lower_bound = 0
        temp_center = 0

        # 变量检查
        if type(center_range) == tuple:
            temp_center_upper_bound = max(center_range)
            temp_center_lower_bound = min(center_range)
            temp_center = str((temp_center_lower_bound+temp_center_upper_bound)/2).replace('.', '')
        else:
            print "Error: center range require a tuple. Please check the input."

            # logger output
            self.ES_logger.error('Center range type error.')
            return
        
        # 检查两个输入的栅格是否存在
        # 检查total_emission
        if not(arcpy.Exists(total_emission_raster)):
            print 'Error: input total emission raster does not exist'

            # logger output
            self.ES_logger.error('input tatal emission not found.')
            return

        # 检查对应年份的主要排放部门栅格
        temp_main_sector = 'main_emi_%s' % year

        if not(arcpy.Exists(temp_main_sector)):
            print 'Error: input total emission raster does not exist'

            # logger output
            self.ES_logger.error('input tatal emission not found.')
            return

        # 检查对应年份的主要排放部门权重栅格
        temp_main_sector_weight = 'main_emi_weight_%s' % year

        if not(arcpy.Exists(temp_main_sector_weight)):
            print 'Error: input total emission weight raster does not exist'

            # logger output
            self.ES_logger.error('input tatal emission not found.')
            return
        
        # 将大于上界和小于下界范围的栅格设为nodata
        # Set local variables
        whereClause = "VALUE < %s OR VALUE > %s" % (temp_center_lower_bound, temp_center_upper_bound)

        # Execute SetNull
        outSetNull = SetNull(total_emission_raster, total_emission_raster, whereClause)

        # Save the output 
        temp_center_path = 'center_%s_%s' % (year, temp_center)
        outSetNull.save(temp_center_path)

        # 防止BUG删除outSetNull
        del outSetNull

        # 生成中心的mask
        # Execute Con
        outCon = Con(temp_center_path, 1, '')

        # Save the outputs 
        temp_center_mask_path = 'center_mask_%s_%s' % (year, temp_center)
        outCon.save(temp_center_mask_path)

        # 防止BUG删除outCon
        del outCon

        # 生成中心主要排放部门栅格
        outMain = arcpy.Raster(temp_center_mask_path) * arcpy.Raster(temp_main_sector)

        # Save the output
        temp_center_main = 'center_main_sector_%s_%s' % (year, temp_center)
        outMain.save(temp_center_main)

        # 防止BUG删除outCon
        del outMain

        # 生成中心主要排放部门比重栅格
        outMainWeight = arcpy.Raster(temp_center_mask_path) * arcpy.Raster(temp_main_sector_weight)
        
        # Save the output
        temp_center_main_weight = 'center_main_sector_weight_%s_%s' % (year, temp_center)
        outMainWeight.save(temp_center_main_weight)

        del outMainWeight

    # 这里要求可以center_range和year_range是一个元组
    def extract_center_area(self, center_range, year_range, isLog):
        # 临时变量
        temp_start_year = self.start_year
        temp_end_year = self.end_year

        # 检查年份变量
        if (type(year_range[0]) != int) or (type(year_range[1]) != int):
            print 'Error! Proccessing starting year and ending year must be int value'
            self.ES_logger.info('Year setting type error.')
            self.ES_logger.error('Year setting error!')
            return
        elif min(year_range) < 1970 or max(year_range) > 2018:
            print 'Error! Proccessing year range out of data support! The year must containt in 1970 to 2018'
            self.ES_logger.info('Year settings are out of range.')
            self.ES_logger.error('Year setting error!')
            return
        else:
            temp_start_year, temp_end_year = min(year_range),max(year_range)
            self.ES_logger.info('Year has set.')
        
        # 列出总排放量栅格
        if bool(isLog) == True:
            temp_wild_card = ['total_emission_%s_log' % s for s in range(temp_start_year,temp_end_year+1)]
        elif bool(isLog) == False:
            temp_wild_card = ['total_emission_%s' % s for s in range(temp_start_year,temp_end_year+1)]
        else:
            print 'Error: Please set the isLog flag.'

            # logger output
            self.ES_logger.error('isLog flag check failed.')
            return

        # 列出需要的total emission 栅格
        self.do_arcpy_list_raster_list(temp_wild_card)


        # 逐年处理
        for yr in tqdm(range(temp_start_year,temp_end_year+1)):
            temp_total_emission = [s for s in self.working_rasters if str(yr) in s].pop()

            self.do_extract_center_area(center_range=center_range,
                                        total_emission_raster=temp_total_emission,
                                        year=yr)

        self.working_rasters = []

    def do_zonal_statistic_to_table(self, year, inZoneData, zoneField, inValueRaster, outTable):
        # Execute ZonalStatisticsAsTable
        outZSaT = ZonalStatisticsAsTable(inZoneData, zoneField, inValueRaster, 
                                        outTable, "DATA", "ALL")
        
        # logger output
        self.ES_logger.debug('Sataistics finished.')
    
    def do_zonal_table_to_csv(self, table, year, outPath):
        temp_table = table

        #--first lets make a list of all of the fields in the table
        fields = arcpy.ListFields(table)
        field_names = [field.name for field in fields]
        # 追加年份在最后一列
        field_names.append('year')

        # 获得输出文件的绝对路径
        temp_outPath = os.path.abspath(outPath)

        with open(temp_outPath,'wt') as f:
            w = csv.writer(f)
            #--write all field names to the output file
            w.writerow(field_names)

            #--now we make the search cursor that will iterate through the rows of the table
            for row in arcpy.SearchCursor(temp_table):
                field_vals = [row.getValue(field.name) for field in fields]
                field_vals.append(str(year))
                w.writerow(field_vals)
            del row
        
        # logger output
        self.ES_logger.debug('Convert %s\'s statistics table to csv file:%s' % (year,temp_outPath))

    def zonal_year_statistics(self, year_range, inZone, center_range, outPath):
        # 获得保存路径
        temp_out_csv_path = os.path.abspath(outPath) 

        # 检查输入的分区是否存在
        if not(arcpy.Exists(inZone)):
            print 'Error: inZone not found.'

            # logger output
            self.ES_logger.error('inZone does not exist.')

            return

        # 生成中心点
        temp_center = str((min(center_range)+max(center_range))/2).replace('.', '')

        for yr in tqdm(range(min(year_range),max(year_range)+1)):
            # 生成中心的栅格名称
            temp_main_inRaster = 'center_main_sector_%s_%s' % (yr, temp_center)
            # 检查输入的待统计值
            if not(arcpy.Exists(temp_main_inRaster)):
                print 'Error: inRaster not found.'

                # logger output
                self.ES_logger.error('inRaster does not exist.')

                return

            temp_outTable = 'table_' + temp_main_inRaster 

            self.do_zonal_statistic_to_table(year=yr,
                                            inZoneData=inZone,
                                            zoneField='ISO_A3',
                                            inValueRaster=temp_main_inRaster,
                                            outTable= temp_outTable)
            # logger output
            self.ES_logger.debug('Zonal statistics finished:%s' % temp_main_inRaster)

            temp_outCsv = os.path.join(temp_out_csv_path,temp_main_inRaster+'.csv')
            self.do_zonal_table_to_csv(table=temp_outTable,
                                        year=yr,
                                        outPath=temp_outCsv)
            
            # logger output
            self.ES_logger.debug('Zonal statitics convert to csv.')
            
            # 生成中心权重的栅格名称
            temp_main_weight_inRaster = 'center_main_sector_weight_%s_%s' % (yr, temp_center)
            # 检查输入的待统计值
            if not(arcpy.Exists(temp_main_weight_inRaster)):
                print 'Error: inRaster not found.'

                # logger output
                self.ES_logger.error('inRaster does not exist.')

                return

            temp_outTable = 'table_' + temp_main_weight_inRaster 

            self.do_zonal_statistic_to_table(year=yr,
                                            inZoneData=inZone,
                                            zoneField='ISO_A3',
                                            inValueRaster=temp_main_weight_inRaster,
                                            outTable= temp_outTable)
            # logger output
            self.ES_logger.debug('Zonal statistics finished:%s' % temp_main_weight_inRaster)

            temp_outCsv = os.path.join(temp_out_csv_path,temp_main_weight_inRaster+'.csv')
            self.do_zonal_table_to_csv(table=temp_outTable,
                                        year=yr,
                                        outPath=temp_outCsv)
            
            # logger output
            self.ES_logger.debug('Zonal statitics convert to csv.')
                                            



# ======================================================================
# ======================================================================
# TEST SCRIPT
# ======================================================================
# ======================================================================
if __name__ == '__main__':
    # test contents
    # test_es = {'E2A':'E2A','E1A1A':'E1A1A','E1A4':'E1A4'}
    # test_esc = {'E2A':1,'E1A1A':2,'E1A4':3}

    # aaa = EDGAR_spatial('D:\\workplace\\geodatabase\\EDGAR_test_42.gdb',st_year=2012,en_year=2012,sector=test_es,colormap=test_esc,background_flag=True, background_flag_label='')
    # aaa.prepare_raster()
    # print aaa.working_rasters
    # aaa.year_total_sectors_merge(2012)
    # aaa.sector_emission_percentage('E2A',2012,'test_e2a_weight')

    # test_es = {'AGS': 'AGS', 'ENE': 'ENE', 'RCO': 'RCO', 'IND': 'IND',
    #            'REF_TRF': 'REF_TRF', 'SWD_INC': 'SWD_INC', 'TNR_Ship': 'TNR_Ship'}
    # test_esc = {'AGS': 1, 'ENE': 2, 'RCO': 3, 'IND': 4,
    #             'REF_TRF': 5, 'SWD_INC': 6, 'TNR_Ship': 7}

    #calculate_fields = ['wmax','wmaxid','wraster','sector_counts']
    # aaa = EDGAR_spatial('D:\\workplace\\geodatabase\\EDGAR_test_60.gdb',
    #                    st_year=2018, en_year=2018, sector=test_es, colormap=test_esc)
    aaa = EDGAR_spatial('D:\\workplace\\geodatabase\\result_no_ship.gdb',
                        st_year=2010, en_year=2018)

    # aaa.prepare_raster()
    # print aaa.working_rasters
    # aaa.year_total_sectors_merge(2018)
    # aaa.year_sector_emission_percentage(2018)
    # aaa.year_weight_joint(2018, list(test_es.values()))
    # aaa.max_weight_rasterize(2018)
    # aaa.proccess_year(start_year=1980, end_year=1989)
    aaa.extract_center_area(center_range=(3.5,4.5),year_range=(2010,2018),isLog=True)
