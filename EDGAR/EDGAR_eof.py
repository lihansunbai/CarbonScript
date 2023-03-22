# -*- coding: utf-8 -*-

import numpy
import h5py
import eofs

class EDGAR_eof():
    def __init__(self):
        pass

    # TODO
    # 这个函数需要完全重写，因为保存为一个numpy数组对于0.1度的数据来说会占据很大的空间，极其有可能导致程序假死或者崩溃。
    # 所以，这里不再采取保存numpy数组的形式，只通过固定参数将数据保存到一个HDF5 格式的文件中。
    def do_EOF_numpy_to_hdf5(self, raster, hdf_full_path_name=None):
        if hdf_full_path_name:
            print 'ERROR: save HDF5 file does not exist. Please check the input.'

            # logger output
            self.ES_logger.error('save HDF5 file does not exist.')
            return
        pass

    def EOF_raster_to_hdf5(self, raster_list, output_name=None, nodata_to_value=None):
        if not raster_list:
            print 'ERROR: input rasters do not exist. Please check the inputs.'

            # logger output
            self.ES_logger.error('input rasters do not exist.')
            return

        # 检查输出目标的hdf文件是否存在，如果存在则打开，同时修改追加文件标识为；如果不存在则进行创建
        if is_open(output_name):
            append_flag = True
        else:
            pass

        # 对输入的栅格列表中的栅格执行转换为numpy array再写入hdf
        for raster in raster_list:
            temp_numpy = self.do_EOF_raster_to_numpy(inRaster=raster,nodata_to_value=nodata_to_value)
