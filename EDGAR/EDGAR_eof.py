# -*- coding: utf-8 -*-

################################################################################
################################################################################
## 备忘录：
## TODO
##  1. 这个类中需要重写EDGAR_spatial中的emission_center类
##  2. 类构造函数中还是需要定义一个log日志记录工具
################################################################################
################################################################################

import collections
import logging

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


    # 默认时间范围
    __default_start_year = 1970
    __default_end_year = 2018

    # 默认部门编码
    __default_gen_encode_list = ['G_ENE', 'G_IND', 'G_TRA', 'G_RCO', 'G_AGS', 'G_WST']

    # 默认HDF5元数据
    __default_eof_hdf_meta_data = {'attrs_title':'Categored emission for EOF'}

    # TODO
    # 这个函数需要完全重写，因为保存为一个numpy数组对于0.1度的数据来说会占据很大的空间，极其有可能导致程序假死或者崩溃。
    # 所以，这里不再采取保存numpy数组的形式，只通过固定参数将数据保存到一个HDF5 格式的文件中。
    def do_EOF_numpy_to_hdf5(self, raster, hdf_full_path_name=None):
        if hdf_full_path_name:
            print('ERROR: save HDF5 file does not exist. Please check the input.')

            # logger output
            self.EE_logger.error('save HDF5 file does not exist.')
            return
        pass

    def EOF_raster_to_hdf5(self, raster_list, output_name=None, nodata_to_value=None):
        if not raster_list:
            print('ERROR: input rasters do not exist. Please check the inputs.')

            # logger output
            self.EE_logger.error('input rasters do not exist.')
            return

        # 检查输出目标的hdf文件是否存在，如果存在则打开，同时修改追加文件标识为；如果不存在则进行创建
        if is_open(output_name):
            append_flag = True
        else:
            pass

        # 对输入的栅格列表中的栅格执行转换为numpy array再写入hdf
        for raster in raster_list:
            temp_numpy = self.do_EOF_raster_to_numpy(inRaster=raster,nodata_to_value=nodata_to_value)

    ############################################################################
    # emission_center 类和类相关的操作函数
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
        elif isinstance(year, collections.Iterable):
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