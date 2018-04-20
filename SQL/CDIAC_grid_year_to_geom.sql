SELECT AddGeometryColumn (
-- 如果表建立在非 public schem 下，第一个参数为 schem
    'grid_co2',
    'cdiac_co2_grid_year',
    'geom',
    4326,
    'POINT',
    2
);

-- 为地理数据列和时间列分别建立索引
CREATE INDEX yr_time
   ON grid_co2.cdiac_co2_grid_year USING btree (year);
CREATE INDEX co2_geom_year
   ON grid_co2.cdiac_co2_grid_year USING gist(geom);

-- 如果表建立在非 public schem 下，需要通过 schem 出现在表前
-- UPDATE schem.table
UPDATE grid_co2.cdiac_co2_grid_year
    SET geom = ST_GeomFromText('POINT('||longitude||' '||latitude||')',4326);
