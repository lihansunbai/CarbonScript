# -*- coding: utf-8 -*-

# 路径处理模块
# Systerm path proccessing module
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
import logging

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
#   3. 所有涉及数据的操作都需要采用绝对路径，防止arcpy出现识别数据错误。
#   4. 多年份处理函数
#   5. 计算字段的构造方法
#   6. 正式开始写数据运算部分内容
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
    ## 构造函数部分
    ## 注意：这里需要两类构造函数：
    ##      1.默认构造函数：不需要传入任何参数。所有计算用到的参数均
    ##        为默认值。
    ##      2.带有数据位置的构造函数：需要传入一个
    ############################################################################
    ############################################################################
    def __init__(self, workspace, background_flag = True, background_flag_label = 'BA',background_raster = 'background', sector={}, colormap={}, st_year=1970, en_year=2018, log_path='EDGAR.log'):
        # 初始化logger记录类的全体工作
        # ES_logger为可使用的logging实例
        self.ES_logger = logging.getLogger()
        self.ES_logger.setLevel(level=logging.DEBUG)
        ES_logger_file = logging.FileHandler(log_path)
        ES_logger_formatter = logging.Formatter('%(asctime)s-[%(levelname)s]-[%(name)s]-[%(funcName)s]-%(message)s')
        ES_logger_file.setFormatter(ES_logger_formatter)
        self.ES_logger.addHandler(ES_logger_file)

        self.ES_logger.info('==========EDGAR_Spatial start==========')

        # arcgis 工作空间初始化
        ## 必须明确一个arcgis工作空间！
        ## 初始化构造需要明确arcgis工作空间或者一个确定的数据为
        ## 检查输入是否为空值
        if workspace == '':
            print 'Spatial direction or database path error! Please check your input!'
            self.ES_logger.error('arcpy environment workspace set failed!')
            return

        ## 为工作空间进行赋值
        ### 这里需要为两个参数赋值：第一个参数是系统中arcpy environment workspace 参数，
        ###  该参数保证了进行arcgis空间运算的“空间分析扩展”检查通过；第二个参数是为了
        ###  缩短代码中“arcpy.env.workspace”属性的书写长度而设置的代用变量。
        self.__workspace = workspace
        arcpy.env.workspace = workspace
        self.ES_logger.info('workpace has set.')
        # 利用栅格计算器进行栅格代数计算时需要先检查是否开启了空间扩展
        arcpy.CheckOutExtension('Spatial')
        self.ES_logger.info('arcpy Spatial extension checked.')
        arcpy.env.parallelProcessingFactor = "100%"
        self.ES_logger.info('arcpy parallelProcessingFactor set to 100%.')

        # EDGAR_sector 参数初始化部分
        ## 检查输入参数类型
        ## 默认情况下使用默认参数初始化
        ## 为EDGAR_sector参数赋值
        if type(sector) != dict:
            print 'Error! EDGAR_sector only accept a dictionary type input.'
            self.ES_logger.info('EDGAR_sector only accept a dictionary type input.')
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
        ## 检查参数输入类型
        ## 默认情况下使用默认参数初始化
        ## 为EDGAR_sector_colormap 参数赋值
        if type(colormap) != dict:
            print 'Error! EDGAR_sector_colormap only accept a dictionary type input.'
            self.ES_logger.info('EDGAR_sector_colormap only accept a dictionary type input.')
            self.ES_logger.error('EDGAR_sector_colormap type error.')
            return
        elif sector == {}:
            self.EDGAR_sector_colormap = copy.deepcopy(self.__default_EDGAR_sector_colormap)
            self.ES_logger.info('This run use default EDGAR sector colormap setting.')
            self.ES_logger.info('EDGAR_sector_colormap has set.')
        else:
            self.EDGAR_sector_colormap = copy.deepcopy(colormap)
            self.ES_logger.info('EDGAR_sector_colormap has set.')

        # year_range 参数初始化部分
        ## 这里需要初始化计算的起始和结束
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
        ## 这里要明确处理的数据是否包含背景0值
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
        ## 这里要将初始化传入的部门参数字典“sector”进行列表化并赋值
        ## 和起始、终止时间传入
        temp_init_filter_label = {'default_set': True, 'background_label_set': self.background[1],
                                                         'sector_set': sector, 
                                                         'start_year_set': st_year, 
                                                         'end_year_set':en_year}
        self.filter_label = temp_init_filter_label
        self.ES_logger.info('filter_label has set.')

        print 'EDGAR_Spatial initialized! More debug information please check the log file.'
        self.ES_logger.info('Initialization finished.')
        self.ES_logger.debug('==========DEGUG INFORMATIONS==========')
        self.ES_logger.debug('acrpy.env.workspace:%s' % arcpy.env.workspace)
        self.ES_logger.debug('arcpy parallelProcessingFactor:%s' % arcpy.env.parallelProcessingFactor)
        self.ES_logger.debug('EDGAR_sector was set to:%s' % self.EDGAR_sector)
        self.ES_logger.debug('EDGAR_sector_colormap was set to:%s' % self.EDGAR_sector_colormap)
        self.ES_logger.debug('Processing begains in year:%s' % self.start_year)
        self.ES_logger.debug('Processing ends in year:%s' % self.end_year)
        self.ES_logger.debug('Raster has background:%s' % self.background_flag)
        self.ES_logger.debug('Raster name\'s background label is:%s' % self.background_label)
        self.ES_logger.debug('Background raster is:%s' % self.background_raster)
        self.ES_logger.debug('Raster filter parameters was set to:%s' % self.filter_label)
        self.ES_logger.debug('==========DEGUG INFORMATIONS==========')
    
    ############################################################################
    ############################################################################
    ## 默认参数
    ## Default values
    ############################################################################
    ############################################################################

    ## Class logger
    ES_logger = logging.getLogger()

    ## Arcgis workspace
    __workspace = ''

    ## EDGAR sector dicts & colormap dicts
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
                                                         'sector': [], 
                                                         'start_year': __default_start_year, 
                                                         'end_year': __default_end_year}}
    # 过滤标签
    filter_label_dict = __default_filter_label_dict

    # 数据库栅格数据筛选过滤标签
    # 默认数据库过滤标签
    __default_raster_filter_wildcard = []
    # 数据库过滤标签
    raster_filter_wildcard = __default_raster_filter_wildcard

    # 需要操作的栅格
    working_rasters = []

    # 特殊变量，用于保存所有部门排放的总和
    __raster_sum = ''

    # 保存部门排放的累加结果
    __raster_overlay = ''

    ############################################################################
    ############################################################################
    ## 参数设定部分
    ############################################################################
    ############################################################################
    # 想要自定义或者修改处理的部门排放需要使用特殊的set函数
    def set_EDGAR_sector(self, sector):
        if type(sector) != dict:
            print 'Error type! EDGAR sectors should be dictionary!'
            self.ES_logger.error('Error type! EDGAR sectors should be dictionary.')
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
            self.ES_logger.error('Error type! EDGAR sectors colormap should be diectionary.')
            return

        self.EDGAR_sector_colormap = sector_colormap

        # logger output
        self.ES_logger.debug('EDGAR_sector_colormap changed to:%s' % sector_colormap)

    def get_EDGAR_sector_colormap(self):
        print self.EDGAR_sector_colormap

    sector_colormap = property(get_EDGAR_sector_colormap, set_EDGAR_sector_colormap)

    def set_year_range(self, start_end=(1970, 2018)):
        self.start_year, self.end_year = start_end

        # logger output
        self.ES_logger.debug('year range changed to:%s' % start_end)

    def get_year_range(self):
        print 'Start year: %s\nEnd year: %s' % (self.start_year, self.end_year)

    year_range = property(get_year_range, set_year_range)

    # 栅格图像背景值设置和查看属性/函数
    def set_background(self, flag_and_label_dict):
        # 关闭background，即栅格不包含背景0值
        if bool(flag_and_label_dict['flag']) == False:
            # 检查flag参数并赋值
            try:
                self.background_flag = bool(flag_and_label_dict['flag'])
                self.background_label = ''
                print 'Background value flag closed!'

                # logger output
                self.ES_logger.debug('Backgroud closed.')
            except:
                print 'Background value flag set failed! Please check the flag argument input.'
                self.ES_logger.error('Background value flag set failed! Please check the flag argument input.')
        # 开启background，即栅格包含背景0值
        elif bool(flag_and_label_dict['flag']) == True:
            # 检查flag参数并赋值
            try:
                self.background_flag = bool(flag_and_label_dict['flag'])

                # logger output
                self.ES_logger.debug('Backgroud opened.')
            except:
                print 'Background value flag set failed! Please check the flag argument input.'
                self.ES_logger.error('Background value flag set failed! Please check the flag argument input.')

            # 检查flag_label参数并
            if type(flag_and_label_dict['label']) == str:
                self.background_label = flag_and_label_dict['label']

                # logger output
                self.ES_logger.debug('Backgroud label changed to:%s' % flag_and_label_dict['label'])
            else:
                print 'Background value flag label set failed! Please check the flag argument input.'
                self.ES_logger.error('Background value flag set failed! Please check the flag argument input.')

    def get_background(self):
        return (self.background_flag,self.background_label)

    background = property(get_background, set_background)

    # 类中提供了两个过滤标签的构造方法
    # 1. 本人生成的数据保存的格式，例如：‘BA_EDGAR_TNR_Aviation_CDS_2010’，其中‘BA’代表包含背景值，数据名结尾
    #    字符串为‘部门_年份’。
    # 2. 自定义标签格式。可以根据用户已有的数据的名称进行筛选。请注意：筛选字符串需要符合 Arcpy 中 wild_card定义的标准进行设定。
    def build_raster_filter_default(self, background_label, sector, start_year, end_year):
        # 检查年份设定是否为整数。（其他参数可以暂时忽略，因为默认格式下基本不会改变）
        if (type(start_year) != int) or (type(end_year) != int):
            print 'Error: Year setting error!'
            self.ES_logger.error('Year setting error. Year settings must be integer and between 1970 to 2018.')
            return
        
        temp_time_range = range(start_year,end_year+1)
        ## 这里使用了zip()函数将部分和年份一一配对。
        ## zip()方法的思路是将两个列表统一到等长度，然后意义对应生成元组
        ## 注意！！！
        ##  这里生成的是元组，该元组中包含[0]号元素为部门，[1]号元素为年份
        temp_sector_year_tupe_list = zip(list(sector.values()) * len(temp_time_range),temp_time_range * len(sector))

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
            self.ES_logger.error('Wild_card set faild. The wild_card string must follow the standard of arcgis wild_card rules.')
            return
        self.raster_filter_wildcard = custom_label
        
        # logger output
        self.ES_logger.debug('raster_filter set by costum.')
    
    ## filter_label 构造方法：
    ##      filter_label字典组的构造如下：
    ##      'default':接受一个符合布尔型数据的值，其中True表示使用
    ##              默认方式构造筛选条件；
    ##      'label':该参数中应该保存需要的筛选条件语句。
    ##              注意！！！：
    ##              如果使用默认方式构造筛选条件，则label参数
    ##              应该包含由以下标签构成的字典：
    ##              'background_label'：数据是否为包括空值数据；
    ##              'sector'：部门标签列表list或者str
    ##              'start_year'：起始年份
    ##              'end_year'：结束年份
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
            self.ES_logger.error('default set error. default_set argument need a bool type input.')
        
        # 检查background label 并赋值
        if bool(filter_label['background_label_set']) == True:
            self.filter_label_dict['label']['background_label'] = self.background[1]

            # logger output
            self.ES_logger.debug('filter label will containt background value.')
        elif bool(filter_label['background_label_set']) == False:
            self.filter_label_dict['label']['background_label'] = ''

            # logger output
            self.ES_logger.debug('filter label will NOT containt background value.')
        else:
            print 'background label set error. Please check backgroud_label_set argument.'
            self.ES_logger.error('background label set error. The background_label_set need a dict type input. More information please refere the project readme.md files.')
        
        # 检查sector是否为str或者list
        if (type(filter_label['sector_set']) == str) or (type(filter_label['sector_set']) == dict):
            self.filter_label_dict['label']['sector'] = filter_label['sector_set']

            # logger output
            self.ES_logger.debug('filter_label changed to:%s' % filter_label['sector_set'])
        else:
            print 'filter_label: sector setting error! sector only accept string or list type.'
            self.ES_logger.error('filter label set error. The filter_label need a dict or a list type input. More information please refere the project readme.md files.')
        
        # 检查start_year 和 end_year
        if (type(filter_label['start_year_set']) != int) or (type(filter_label['end_year_set']) != int):
            print 'filter_label: year setting error! please check year arguments'
            self.ES_logger.error('year error. The star year and end year must be integer. More information please refere the project readme.md files.')
            return
        else:
            self.filter_label_dict['label']['start_year'] = filter_label['start_year_set']
            self.filter_label_dict['label']['end_year'] = filter_label['end_year_set']

            #logger output
            self.ES_logger.debug('filter_label year range changed to:%s to %s' % (filter_label['start_year_set'], filter_label['end_year_set']))


    def get_filter_label(self):
        return self.filter_label_dict

    filter_label = property(get_filter_label,set_filter_label)

    ## 注意：这里需要为set函数传入一个filter_label字典
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
    def list_working_rasters(self, raster_filter_wildcard):
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
                self.do_arcpy_list_raster_str(wildcard_str='')

                # logger output
                self.ES_logger.debug('rasters listed without filter.')
            # 列表不为空的情况
            else:
                # 直接将参数传入list方式的方法列出需要栅格
                self.do_arcpy_list_raster_list(wildcard_list=raster_filter_wildcard)

                # logger output
                self.ES_logger.debug('rasters listed.')
        # 传入单一字符串情况
        elif temp_type_check == str:
            # 显示筛选列表警告:
            # 如果为空值则警告可能会对所有栅格进行操作：
            if raster_filter_wildcard == '':
                print 'WARNING: No fliter! All rasters will be list!'
            
            self.do_arcpy_list_raster_str(wildcard_str=raster_filter_wildcard)

            # logger output
            self.ES_logger.debug('rasters listed without filter.')
        else:
            # 其他情况直接退出
            # 或者，如果raster_filter_wildcard为空则直接显示筛选列表错误
            print 'Error: No fliter! Please check input raster filter!'
            self.ES_logger.error('No fliter! Please check input raster filter!')
            return
        
    # 实际执行列出栅格的方法，这个为str方式
    def do_arcpy_list_raster_str(self, wildcard_str):
        self.working_rasters.extend(arcpy.ListRasters(wild_card=wildcard_str))

        # logger output
        self.ES_logger.debug('working rasters chenged to:%s' % self.working_rasters)

    # 实际执行列出栅格的方法，这个为list方式
    def do_arcpy_list_raster_list(self, wildcard_list):
        # 逐年份生成需要处理的数据列表
        for i in wildcard_list:
            self.working_rasters.extend(arcpy.ListRasters(wild_card=i))


        # logger output
        self.ES_logger.debug('working rasters chenged to:%s' % self.working_rasters)

    ############################################################################
    ############################################################################
    ## 实际数据计算相关函数/方法
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
        self.list_working_rasters(self.raster_filter_wildcard)

    # 栅格叠加
    # 这个函数用到了tqdm显示累加进度
    def do_raster_add(self, raster_list, result_raster):
        if result_raster != str:
            print 'Raster add: The output result raster path error.'
        # 将列表中的第一个栅格作为累加的起始栅格
        temp_raster = arcpy.Raster(raster_list[0])
        raster_list.pop(0)

        # 累加剩余栅格
        for r in tqdm(raster_list):
            temp_raster = temp_raster + arcpy.Raster(r)
        
        return temp_raster.save(result_raster)
    
    # 函数需要传入一个包含需要叠加的部门列表list，
    # 以及执行操作的年份
    def year_sectors_merge(self, raster_list, merge_sector, year):
        # 筛选需要计算的部门
        temp_sector = ''
        for i in merge_sector:
            temp_sector = temp_sector + '|%s' % i
        
        temp_sector = temp_sector[1:len-1]
        temp_sector_year = '%s.*%s' % (temp_sector,year)
        filter_regex = re.compile(temp_sector_year)

        temp_merge_raster = list(filter(filter_regex,raster_list))

        # 此处输出的总量数据文件名不可更改！！！
        # TODO
        # 加入自定义文件名功能
        result_year = 'total_emission_%s' % year
        self.do_raster_add(temp_merge_raster, result_year)

    # 实用（暴力）计算全年部门排放总和的函数
    def year_total_sectors_merge(self, year):
        temp_sector = list(self.EDGAR_sector.values())
        self.year_sectors_merge(self.working_rasters,temp_sector,year)
        print 'Total emission of %s saved!\n' % year


    # 计算单个部门占年排放总量中的比例
    # 注意！！！
    # 这里的比例定义为：
    #       对每一个栅格：部门排放/该栅格的总量
    ######################################
    def sector_emission_percentage(self, sector, year, output_sector_point):
        # 尝试列出当年总量的栅格
        # 这里要注意，总量栅格的名称在year_sector_merge()中写死了
        temp_year_total = arcpy.Raster('total_emission_'.join(yaer))
        temp_sector_wildcard = '%s*%s*%s' % (self.background[1],sector,year)
        temp_sector_emission = arcpy.ListRasters(wild_card=temp_sector_wildcard)

        # 检查输入的部门栅格和总量栅格是否存在，如果不存在则报错并返回
        if not (arcpy.Exists(sector)) or not (arcpy.Exists(temp_year_total)):
            print 'Sector_emission_percentage: Error! sector or year total emission raster does not exist.'
            return
        
        # 计算部门排放相对于全体部门总排放的比例
        # 注意！！！
        # 这里涉及除法！0值的背景会被抹去为nodata。所以要再mosaic一个背景上去才能转化为点。
        temp_output_weight_raster = sector / temp_year_total

        ## 保存栅格格式权重计算结果
        temp_output_weight_raster_path =  '%s_weight_raster_%s' % (sector, year)
        temp_output_weight_raster.save(temp_output_weight_raster_path)
        print 'Sector emission weight saved: %s\n' % sector

        # 栅格数据转点对象。转为点对象后可以实现计算比例并同时记录对应排放比例的部门名称
        # 这里用到了arcpy.AlterField_management()这个函数可能在10.2版本中没有
        try:
            # transform to point features
            arcpy.RasterToPoint_conversion(temp_output_weight_raster, output_sector_point, 'VALUE')
            # rename value field
            arcpy.AlterField_management(output_sector_point,'grid_code',new_field_name='test_sector')

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

        # 这里的year参数可能需要删除aa
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
        temp_sector = copy.deepcopy(self.EDGAR_sector)
        temp_sector_colormap = copy.deepcopy(self.EDGAR_sector_colormap)
        ## temp_working_sector = self.__workspace + '\\%s' % sector_points
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
            for row in tqdm(cursor):
                max_weight = max(row[0:-calculate_fields_counts])
                max_id = temp_cursor_fileds[row.index(max_weight)]
                max_colormap = temp_sector_colormap[max_id]
                emitted_sectors = len([i for i in row[0:-calculate_fields_counts] if i != 0])

                row[-1] = emitted_sectors
                row[-2] = max_colormap
                row[-3] = max_id
                row[-4] = max_weight

                cursor.updateRow(row)

    def proccess_year(self,start_year,end_year):
        # for year in [start_year:end_year]:
        #     self.sector_max()
        pass

    def proccess_all(self):
        pass

    

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



# ======================================================================
# ======================================================================
# TEST SCRIPT
# ======================================================================
# ======================================================================
if __name__ == '__main__':
    ## test contents
    test_es = {'E2A':'E2A','E3':'E3'}
    test_esc = {'E2A':1,'E3':2}

    aaa = EDGAR_spatial('D:\\workplace\\geodatabase\\EDGAR_test_42.gdb',st_year=2012,en_year=2012,sector=test_es,colormap=test_esc,background_flag=False,background_flag_label='')
    calculate_fields = ['wmax','wmaxid','wraster','sector_counts']
    aaa.prepare_raster()
    # aaa.sector_max('categories_2015',calculate_fields)
    print aaa.working_rasters
    #aaa.year_total_sectors_merge(2012)
    
    # test_es = {'ENE':'ENE','IND':'IND','REF_TRF':'REF_TRF','TNR_Aviation_CDS':'TNR_Aviation_CDS'}

    # test_esc={'ENE':1,'IND':2,'REF_TRF':3,'TNR_Aviation_CDS':4}
    # aaa = EDGAR_spatial('D:\\workplace\\DATA\\geodatabase\\test\\EDGAR_test.gdb',sector=test_es,colormap=test_esc,st_year=2010,en_year=2014)
    # aaa.prepare_raster()
    # print aaa.working_rasters
