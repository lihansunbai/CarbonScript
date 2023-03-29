# -*- coding: utf-8 -*-

################################################################################
################################################################################
## 备忘录：
## TODO
##  1. 这个类中需要重写EDGAR_spatial中的emission_center类
##  2. 类构造函数中还是需要定义一个log日志记录工具
################################################################################
################################################################################

import chunk
import os
import re
import itertools
import collections
import logging
import string

import eofs
import h5py
import numpy
import tqdm
import xarray
from tqdm import tqdm

class EDGAR_eof():
    '''
    EOF analysis using EDGAR data
    '''
    def __init__(self,
                 st_year=1970,
                 en_year=2018,
                 log_path='eof.log'):

        # 初始化logger记录类的全体工作
        # EE_logger为可使用的logging实例
        # 类使用的logger
        self.EE_logger = logging.getLogger()
        self.EE_logger.setLevel(level=logging.DEBUG)
        EE_logger_file = logging.FileHandler(log_path)
        EE_logger_formatter = logging.Formatter(
            '%(asctime)s-[%(levelname)s]-[%(name)s]-[%(funcName)s]-%(message)s')
        EE_logger_file.setFormatter(EE_logger_formatter)
        self.EE_logger.addHandler(EE_logger_file)

        self.EE_logger.info('==========EDGAR_EOF start==========')

        # year_range 参数初始化部分
        # 这里需要初始化计算的起始和结束
        if (type(st_year) != int) or (type(en_year) != int):
            print('Error! Processing starting year and ending year must be int value')
            self.EE_logger.info('Year setting type error.')
            self.EE_logger.error('Year setting error!')
            return
        elif st_year < self.__default_start_year or en_year > self.__default_end_year:
            print('Error! Processing year range out of data support! The year must contain in 1970 to 2018')
            self.EE_logger.info('Year settings are out of range.')
            self.EE_logger.error('Year setting error!')
            return
        else:
            self.year_range = (st_year, en_year)
            self.EE_logger.info('Year has set.')

        # 初始化numpy文件的过滤标签
        self.numpy_filter = []

        # 初始化排放中心列表
        self.emission_center_list = []

        # 初始化分解文件名的metadata
        self._metadata = {}

    # 默认时间范围
    __default_start_year = 1970
    __default_end_year = 2018

    # 默认部门编码
    __default_gen_encode_list = ['G_ENE', 'G_IND', 'G_TRA', 'G_RCO', 'G_AGS', 'G_WST']

    # 默认HDF5元数据
    __default_eof_hdf_meta_data = {'attrs_title':'Categored emission for EOF'}

    ############################################################################
    ############################################################################
    # 通用参数、属性和方法
    ############################################################################
    ############################################################################

    # 想要自定义或者修改数据处理的年份时间范围的特殊property函数
    @property
    def year_range(self):
        return (self.start_year, self.end_year)

    @year_range.setter
    def year_range(self, start_end=(1970, 2018)):
        self.start_year, self.end_year = start_end

        # logger output
        self.EE_logger.debug('year range changed to:%s to %s', start_end[0], start_end[1])

    # 自定义metadata的属性
    @property
    def metadata_handler(self):
        return self._metadata

    @metadata_handler.setter
    def metadata_handler(self, metadata):
        if not metadata:
            print('ERROR: the input is empty.')

            # logger output
            self.EE_logger.error('input metadata dict is empty.')
            return

        self._metadata = metadata

    # 以下函数已被弃用
    # 以下函数功能整合到hdf5_hierarchical_path属性中
    # 用以拆解文件名的函数
    # 该函数会按照metadata字典中给出的键值，分析输入文件名的结构，并返回一个符合HDF层次结构的路径
    def deprecated_file_name_decomposer(self, filename, metadata):
        '''
        按照metadata字典中给出的键值，
        分析输入文件名的结构，
        并返回一个符合HDF层次结构的路径
        '''
        # 保存返回用结果字典
        return_decomposed_parts = {}

        for meta in metadata.items():
            return_decomposed_parts[meta[0]] = [value for value in meta[1] if(value in filename)]

        return return_decomposed_parts


    def print_start_year(self, year):
        # logger output
        self.EE_logger.debug('Processing start of year %s', year)

        print('==============================')
        print('==============================')
        print('Processing start of year %s', year)
        print('==============================')
        print('==============================')

    def print_finish_year(self, year):

        # logger output
        self.EE_logger.debug('Finished processing data of year %s', year)

        print('==============================')
        print('==============================')
        print('Congratulations!')
        print('Finished processing data of year %s', year)
        print('==============================')
        print('==============================')

    ############################################################################
    ############################################################################
    # 将 numpy 数据保存为 hdf
    ############################################################################
    ############################################################################

    # 这个函数需要完全重写，因为保存为一个numpy数组对于0.1度的数据来说会占据很大的空间，极其有可能导致程序假死或者崩溃。
    # 所以，这里不再采取保存numpy数组的形式，只通过固定参数将数据保存到一个HDF5 格式的文件中。
    def numpy_to_hdf5(self, numpy_list, file_name_metadata, output_name=None, output_path=None, nodata_to_value=None):
        if not numpy_list or not file_name_metadata:
            print('ERROR: input rasters or file name metadata do not exist. Please check the inputs.')

            # logger output
            self.EE_logger.error('input rasters or file name metadata do not exist.')
            return

        # 检查输入路径是否存在
        # 如果路径存在则组合为HDF文件的绝对路径
        if os.path.exists(output_path):
            temp_full_path_name = os.path.join(output_path, output_name)
        else:
            print('ERROR: HDF file location does not exists, please check the input.')

            # logger output
            self.EE_logger.error('HDF file location does not exist.')
            return

        # 使用a参数打开文件，如果文件存在则追加啊，如果文件不存在则创建新文件
        with h5py.File(temp_full_path_name, 'a') as hdf:
            # 对输入的栅格列表中的栅格执行转换为numpy array再写入hdf
            for numpy_file in numpy_list:
                # 构建hdf5_hierarchical_path生成需要的传入参数                
                temp_save_name = {'file_name':numpy_file,
                                    'metadata':file_name_metadata}
                
                # 通过属性的生成方法生成hdf5_hierarchical_path
                self.hdf5_hierarchical_path = temp_save_name

                # 从numpy npz文件读取数据
                temp_numpy_array = numpy.load(numpy_file)
                temp_numpy_array_dtype = temp_numpy_array['arr_0'].dtype

                # 设置保存的hdf 组的路径
                temp_group = hdf.create_group(name=self.hdf5_data_hierarchical_path)
                # 保存数据
                temp_dataset = temp_group.create_dataset(name='grid_co2',
                                          data=temp_numpy_array['arr_0'],
                                          dtype=temp_numpy_array_dtype,
                                          chunks=True,
                                          compression="gzip")
                
                # 为dataset添加属性信息
                temp_dataset.attrs['DimensionNames'] = 'nlat,nlon'
                temp_dataset.attrs['units'] = 't/grid'
                temp_attrs = self.hdf5_data_hierarchical_path.split('/')
                temp_dataset.attrs['year'] = temp_attrs[0]
                temp_dataset.attrs['components'] = temp_attrs[1]
                temp_dataset.attrs['region'] = temp_attrs[2]

    @property
    def hdf5_hierarchical_path(self):
        if not self.hdf5_data_hierarchical_path:
            return ''
        else:
            return self.hdf5_data_hierarchical_path

    @hdf5_hierarchical_path.setter
    def hdf5_hierarchical_path(self, data):
        '''
        在传入的data中，按照metadata字典中给出的键值，在file_name中，
        通过正则表达式匹配输入文件名的结构，
        并返回一个符合HDF层次结构的路径。

        metadata字典中必须包含年份'year'键值，其他可选的键值为'emission_components', 'region', 'centers', 'emission_categories', or 'EDGAR_sectors'。
        示例：
        传入的data字典结构示例：
        {'file_name': 'a string of npz file name',
         'metadata':{'year':['1970',...,'2018'],
                     'emission_categories':['G_IND',...,'G_WST'],
                     ... : ...}
        }
        '''
        if not data:
            print('ERROR: input metadata dose not exist. Please check the input.')

            # logger output
            self.EE_logger.error('input metadata does not exist.')
            self.hdf5_data_hierarchical_path = ''
            return
        
        if not data['metadata']['year']:
            print('ERROR: year must contained in metadata. Please check the input.')

            # logger output
            self.EE_logger.error('year is empty in metadata')
            self.hdf5_data_hierarchical_path = ''
            return

        # 保存返回的结果路径
        temp_hierarchical_path = ''

        # 保存返回用结果字典
        return_decomposed_parts = {}

        # 从文件名中，按照meta字典给出的键值拆解路径信息
        for meta in data['metadata'].items():
            for value in meta[1]:
                meta_match = re.search(re.compile(value), data['file_name'])
                if meta_match:
                    return_decomposed_parts[meta[0]] = meta_match[0]

        # 检查年份匹配结果，如果没有匹配到年份信息则直接退出
        if not return_decomposed_parts['year']:
            print('ERROR: No input year was found in file name. Please check the input')

            # logger output
            self.EE_logger.error('no year information was found.')
            self.hdf5_data_hierarchical_path = ''
            return
            
        # 以下构建路径的逻辑是从子路径向父路径逐层构建。
        # 采用这个方式构建的优点是最多只需要进行三层构建。
        # 不采用迭代的方式进行构建的原因是：尚未发现更简单或者更由效率的方法。
        # 判断是否需要构建中心
        if 'centers' in data['metadata']:
            temp_hierarchical_path = temp_hierarchical_path.join('{}'.format(return_decomposed_parts['centers']))
        else:
            temp_hierarchical_path = 'global'

        # 判断emission_components
        # 判断是否需要构建总量、部门或分类排放
        if 'emission_categories' in data['metadata']:
            temp_hierarchical_path = '{}/{}'.format(return_decomposed_parts['emission_categories'], temp_hierarchical_path)
        elif 'EDGAR_sectors' in data['metadata']:  
            temp_hierarchical_path = '{}/{}'.format(return_decomposed_parts['EDGAR_sectors'], temp_hierarchical_path)
        else:
            temp_hierarchical_path = '{}/{}'.format('total', temp_hierarchical_path)
        
        # 构建年份路径
        temp_hierarchical_path = '{}/{}'.format(return_decomposed_parts['year'], temp_hierarchical_path)

        # 保存最终结果
        self.hdf5_data_hierarchical_path = temp_hierarchical_path

    # numpy_filter_label 构造方法
    # numpy_filter_label 字典由以下键结构组成：
    #   必要键值：
    #       'category'：一个包含指定排放分类名称的列表。
    #       'time'：一个确定的时间范围元组，注意，这个值只能是元组。
    #       'delimiter'：用于连接各个标签的自定义分隔符
    #       'append'：numpy文件的扩展名
    #   可选键值
    #       'prefix'：用于填充到filter_fmt中的可选参数
    #       'suffix'：用于填充到filter_fmt中的可选参数
    #   注意在构造numpy_filter_label的时候会强制检查必要键值，若不符合要求则无法构造并返回一个空列表。
    #   如果符合构造要求，函数会按照[prefix][delimiter][category][delimiter][time][delimiter][suffix][append]的顺序，
    #   迭代展开键值中的每一个包含列表元素的键值并连接，返回包含所有numpy名称的列表。
    @property
    def numpy_file_filter(self):
        return self.numpy_filter

    @numpy_file_filter.setter
    def numpy_file_filter(self, filter_label):
        # 检查必要键值是否存在，若不存在则直接返回空列表
        if not filter_label['category'] or not filter_label['time'] or not filter_label['delimiter'] or not filter_label['append']:
            print('ERROR: category and time and filter_fmt must be offered.')
            
            # logger output
            self.EE_logger.error('can not found category or time or filter_fmt in input dict.')
            return []

        # 将时间转换为列表
        if type(filter_label['time']) != tuple:
            print('ERROR: time must be a tuple with a star year and a end year.')

            # logger output
            self.EE_logger.error('time is not a tuple.')
            return []
        else:
            filter_label['time'] = ['{}'.format(i) for i in range(min(filter_label['time']), max(filter_label['time']) + 1)]

        # 检查是否存在可选字段
        if 'prefix' in filter_label and bool(filter_label['prefix']):
            temp_has_prefix = '1'
        else:
            temp_has_prefix = '0'

        if 'suffix' in filter_label and bool(filter_label['suffix']):
            temp_has_suffix = '1'
        else:
            temp_has_suffix = '0'

        # 保存可选字段检查结果
        temp_has_costume = int(temp_has_prefix+temp_has_suffix,2)

        # 针对包含可选字段的情况展开所有标签
        if temp_has_costume == 0:
            temp_iter = itertools.product(filter_label['category'],filter_label['delimiter'],filter_label['time'])
        elif temp_has_costume == 1:
            temp_iter = itertools.product(filter_label['category'],filter_label['delimiter'],filter_label['time'],filter_label['delimiter'],filter_label['suffix'])
        elif temp_has_costume == 2:
            temp_iter = itertools.product(filter_label['prefix'],filter_label['delimiter'],filter_label['category'],filter_label['delimiter'],filter_label['time'])
        elif temp_has_costume == 3:
            temp_iter = itertools.product(filter_label['prefix'],filter_label['delimiter'],filter_label['category'],filter_label['delimiter'],filter_label['time'],filter_label['delimiter'],filter_label['suffix'])

        # list comprehensions生成返回结果列表
        return_numpy_filter = [''.join(it) for it in list(temp_iter)]
        return_numpy_filter = [it + '.' + filter_label['append'] for it in return_numpy_filter]

        self.numpy_filter = return_numpy_filter

    # 筛选需要的numpy数据
    # 并通过一个列表返回numpy数据的完整路径。
    def select_numpy(self, numpy_file_filter, search_path=None):
        if not numpy_file_filter:
            print('ERROR: none numpy filter!')

            # logger output
            self.EE_logger.error('numpy filter in empty.')
            return
    
        if not search_path:
            print('ERROR: search path is empty, please check the input')

            # logger output
            self.EE_logger.error('search path does not exists')
            
            return
        
        # 列出search_path路径下的所有文件
        temp_search_files = [f for f in os.listdir(search_path) if os.path.isfile(os.path.join(search_path, f))]

        # 从当前路径的文件中找到标签指定的文件
        numpy_files = list(set(temp_search_files).intersection(numpy_file_filter))
        
        # 构建完整numpy文件路径
        numpy_files_path = [os.path.join(search_path, f) for f in numpy_files]

        return numpy_files_path
 
    ############################################################################
    ############################################################################
    # emission_center 类和类相关的操作函数
    ############################################################################
    ############################################################################

    ############################################################################
    # 类定义
    ############################################################################
    class emission_center():
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

                return

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
                self.outer_class.EE_logger.error('Emission peak is empty.')
                return

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
                self.outer_class.EE_logger.error('center peaks is empty.')
                return
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
                return
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
            self.EE_logger.error('input emission center does not exist.')
            return

        return emission_center.return_center()

    # 删除center中的某个peak
    # 只需要提供年份即可
    def remove_peak(self, emission_center, year):
        # 检查输入的emission_center是否存在，不存在则直接返回
        if not emission_center:
            print('ERROR: emission center does not exist.')

            # logger output
            self.EE_logger.error('input emission center does not exist.')
            return

        if not year or year > self.end_year or year < self.start_year:
            print('ERROR: removing peak failed, please assign a correct year to index the peak.')

            # logger output
            self.EE_logger.error('Input year is empty.')
            return

        emission_center.center_peaks.pop(year)

    # 修改某个peak的内容
    def edit_peak(self, emission_center, emission_peak):
        # 检查输入的emission_center是否存在，不存在则直接返回
        if not emission_center:
            print('ERROR: emission center does not exist.')

            # logger output
            self.EE_logger.error('input emission center does not exist.')
            return

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
        elif isinstance(year, collections.abc.Iterable):
            temp_peaks_list = []

            for yr in year:
                temp_peaks_list.append(
                    self.do_emission_peak(emission_peak_range=emission_peak_range, year=yr))

            return temp_peaks_list
        else:
            print('ERROR: input year error.')

            # logger output
            self.EE_logger.error('input year error.')
            return

    # 实际执行构建排放峰值
    def do_emission_peak(self, emission_peak_range, year):
        # 排放中心变量检查
        if type(emission_peak_range) == tuple:
            if len(emission_peak_range) == 2:
                temp_peak_upper_bound = max(emission_peak_range)
                temp_peak_lower_bound = min(emission_peak_range)
                temp_peak = str(
                    (temp_peak_lower_bound + temp_peak_upper_bound) / 2).replace('.', '')
            else:
                print("Error: emission peak require maximum and minimum range.")

                # logger output
                self.EE_logger.error('Emission peak range error.')
                return
        else:
            print("Error: emission peak range require a tuple. Please check the input.")

        # 年份变量检查
        if year < self.start_year or year > self.end_year:
            print("Error: emission peak require a correct year.")

            # logger output
            self.EE_logger.error('Emission peak year error.')
            return

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
            self.EE_logger.error('center name type error.')
            return

        # 创建一个仅包含名称的emission_center实例
        return self.emission_center(outer_class=outer_class, center_name=emission_center_name)

    # 向中心中添加排放峰值数据
    def add_emission_peaks(self, emission_center, peaks_list):
        # 检查输入的emission_center是否存在，不存在则直接返回
        if not emission_center:
            print('ERROR: emission center does not exist.')

            # logger output
            self.EE_logger.error('input emission center does not exist.')
            return

        # 检查输入的peak_list是否存在，不存在则直接返回
        if not peaks_list:
            print('ERROR: peak list center does not exist.')

            # logger output
            self.EE_logger.error('input peak list does not exist.')
            return

        # 支持将emission_peak或者由它组成的列表传入类中
        if type(peaks_list) == list:
            for pl in peaks_list:
                emission_center.emission_peak_assembler(pl)
        elif type(peaks_list) == dict:
            emission_center.emission_peak_assembler(peaks_list)
        else:
            print('ERROR: emission peak type error, please run emission_peak function to generate a emission peak or a list of emission peaks.')

            # logger output
            self.EE_logger.error('emission peak type or structure error.')
            return

        # 添加emission_peak后重新整理emission_center的内容
        emission_center.generate_center()

    # 生成所有排放中心的名字列表
    def return_emission_center_list(self):
        return self.emission_center_list


if __name__ == '__main__':
    print('main process')