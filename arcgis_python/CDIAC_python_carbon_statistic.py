#-*- coding:UTF-8 -*-
import numpy


def carbon_statistic(carbon, outpath, startyear, endyear):
    # Process argument
    if carbon == "":
        return "ERROR: Incorrect argument!"
    elif isinstance(carbon, str) is False:
        return "ERROR: Please input a csv file path!"

    if outpath == "":
        return "ERROR: Incorrect argument!"
    elif isinstance(outpath, str) is False:
        return "ERROR: Please select a path for export data!"

    # generate time series
    if startyear > endyear:
        return "ERROR: Please select a correct set of start to end years!"

    time_series = numpy.arange(startyear - 1750, endyear - 1749)

    # import data form csv file
    co2_raw_data = numpy.genfromtxt(carbon,
                                    delimiter=',',
                                    usecols=time_series,
                                    skip_header=1,
                                    filling_values=0)

    ###########################################################################
    ###########################################################################
    ## 这里开始处理每年的数据并输出格式化输出文本。
    ## 备忘录：
    ##   1.原始的导入数据被保存，保存的主要原因是因为里面还包含了国家信息，
    ##     可以在未来被再次利用。
    ##   2.一下的数据处理和统计输出都是提取里面的年份维度下的数据进行使用。
    ##   3.关于数据统计中的平均数，选择计算两个平均数：
    ##      a).纯统计概念上的平均值
    ##         即统计某一年份全球的排放平均值。这个平均将当年无法计入统计的国家
    ##         排放认定为"0"，并进行平均值计算。故，我认为，这个平均值在时间较
    ##         早的年份会小的可怜。
    ##      b).有效数据平均值
    ##         即统计某一年份全球包含有效数据记录的平均值，通常数据中的无效数据
    ##         被"0"替代了。这个平均值只计算那些有数据记录的国家的平均值。所以，
    ##         在某些较早年份，由于包含有效记录的国家数量太少，这个平均值可能
    ##         比近几十年的平均值还要大。
    ##   4.计算两个平均值的原因
    ##     有效数据的样本太少，尤其是1800年以前，有效记录可能只有几个。这样的
    ##     样本中，并不是表明全球的化石燃料碳排放在很早的年份很小，而是由于
    ##     不同的原因，很多排放无法被有效记录。由此带来的全球平均计算可能无法
    ##     准确的反应当时的实际碳排放。如果以统计全球所有国家的角度考虑，将
    ##     无法统计的国家计入平均值计算，即默认全球所有现在行政区划中存在的国家
    ##     都包含有化石燃料的碳排放。但是，这种默认包含一个问题，许多现在存在的
    ##     行政区在过去的时间中实际没有可观的，可以被纳入记录的化石燃料碳排放。
    ##     由此产生的平均实际上是强迫增加了这些国家和地区的“历史债务”。
    ##     样本数据的跨度很大。在近期年份，数据的跨度大概是10^6数量级。这样的
    ##     情况下，很多小排放量产生的效应在某种程度上都被抹去了。（大排放量的
    ##     国家的方差解释变量很大）
    ###########################################################################
    ###########################################################################

    # set export file
    export_file = outpath + 'quartile_statistic.csv'
    export_array = numpy.array([0, 0., 0., 0., 0., 0., 0.])

    # 由于使用了numpy的原因，所以实际需要计算的只有有效数据平均值这个数据了
    # 关于这个复杂的迭代器。对于二维矩阵，第一个参数描述行，第二个参数描述列
    # 关于这里用到的数据，我们要取从第1列以后的数据
    
    time_series = numpy.arange(startyear - 1751, endyear - 1750)
    for i in time_series:
        statistic_tmp = co2_raw_data[:, i]
        # calculating mean simply statitical
        co2_mean = statistic_tmp.mean()

        # expel zero data
        statistic = statistic_tmp[statistic_tmp.nonzero()]
        co2_mean_valid = statistic.mean()
        co2_max = statistic.max()
        co2_min = statistic.min()
        co2_q1 = numpy.percentile(statistic, 25, interpolation='midpoint')
        co2_q3 = numpy.percentile(statistic, 75, interpolation='midpoint')

        # save to result numpy array
        export_tmp = numpy.array([i + 1751, co2_max, co2_min, co2_q1, co2_q3, co2_mean_valid, co2_mean])
        export_array = numpy.vstack((export_array, export_tmp))

    # write to export file
    numpy.savetxt(export_file,
                  export_array,
                  fmt='%s',
                  delimiter=',',
                  header='year,co2_max,co2_min,co2_q1,co2_q3,co2_mean_valid,co2_mean')
