SELECT AddGeometryColumn (
-- 如果表建立在非 public schem 下，第一个参数为 schem
--    'schem_name',
    'grid_co2',
    'edgar_co2_grid_year',
    'geom',
    4326,
    'POINT',
    2
);

-- 如果表建立在非 public schem 下，需要通过 schem 出现在表前
-- UPDATE schem.table
UPDATE grid_co2.edgar_co2_grid_year
    SET geom = ST_GeomFromText('POINT('||longitude||' '||latitude||')',4326);

--特殊用途，当导入数据过大过多时
--采用导入一个分类，生成一次geom列，导出一次数据的方法。
--或者有其他更简便的方法
--比如，在VIEW中生成geom列，在原始数据表中不进行geom列生成操作。
--因为EDGAR 数据实在是太大太多了，一次操作完成的空间和时间成本都很大，有时候难以接受。
