# EDGAR_spatial 类
# EDGAR_spatial class
## 简介
在EDGAR 5.0版本以后，利用其发布的TEXT数据生成全球格网数据或者兴趣区域/部门格网数据的运算变得非常缓慢。原有的利用简单脚本实现的生成栅格数据的方式使用起来面临着诸多问题，例如单次生成的数据量过于庞大，缓慢的处理速度难以明确的了解处理进度。为了解决上述的问题和在未来可以将这一方法应用到未来的新版本数据中，亟需设计一个集合各种处理工具、具有友善的处理进度监控和可自定义处理兴趣范围的Python工具模块。所以，本人重新设计并实现了EDGAR_spatial类来实现上述需求。

## Introduction
After the release of EDGAR v50 dataset, generating global emission raster data or specific regions and time raster data based on the TEXT raw release are unacceptablly slow. The perivous raster generating-method with a single file script was abandoned because of many problems like mass disk occupation in a proccess, slowly generation and dosen't has a proccess monitor. To overcome above problems and to implement my method in the future EDGAR versions, I designed a new module/class that contain the core method of TEXT-to-raster and the core method of major-emission-sector indicator, many useful utilities, proccessing visualization with proccessing bar and customized options of specific regions and time.

## 使用说明
待完成：说明不同文件的用途

## Manual:
TODO: Usage of files...

## 类函数
## class functions
### EDGAR_sector 属性函数
- set_EDGAR_sector()
- get_EDGAR_sector()
获取EDGAR_sector的属性或修改EDGAR_sector的内容。EDGAR_sector 参数接受一个字典，字典的 key 是部门排放的缩写，对应的值同样是部门排放缩写的字符串。

### EDGAR_sector_colormap 属性函数
- set_EDGAR_sector_colormap()
- get_EDGAR_sector_colormap()
获取EDGAR_sector_colormap的属性和修改EDGAR_sector_colormap的内容。EDGAR_sector_colormap 参数接受一个字典，字典的 key 是部门排放的缩写，对应的值是整数。整数用于标志栅格数据中的不同排放部门。

### year_range 属性函数
- set_year_range()
- get_year_range()
获取year_range的属性和修改year_range()的内容。修改year_range的内容会同时修改start_year和end_year两个参数的值。

## 类属性
## Class attributes
### __default_EDGAR_sector
默认EDGAR部门排放，保存了所有部门排放的种类。
### __default_EDGAR_sector_colormap
默认的部门排放的栅格分类值。
### EDGAR_sector
存储部门排放，为字典结构。
### EDGAR_sector_colormap
存储部门排放的栅格分类值，为字典结构。
### __default_start_year
默认计算起始时间，值为1970。
### __default_end_year
默认计算结束时间，值为2018。
### start_year
计算的起始年份，为整数，最早时间为1970，最晚时间为2018。
### end_year
计算的结束年份，为整数，最早时间为1970，最晚时间为2018。


### 参数简介
1. EDGAR_sector 参数接受一个字典，字典的 key 是部门排放的缩写，对应的值同样是部门排放缩写的字符串。
2. EDGAR_sector_colormap 参数接受一个字典，字典的 key 是部门排放的缩写，对应的值是整数。整数用于标志栅格数据中的不同排放部门。
3. 
### Variables short introduction:
1. EDGAR_sector: accept a dictionary that key is theabbreviation of EDGAR specific-sector and key valuealso the abbreviation of EDGAR specific-sector.
2. EDGAR_sector_colormap: accept a dictionary that keyis the abbreviation of EDGAR specific-sector and key value is a integer that will be used for indicateddifferent sector in raster results.
3.  