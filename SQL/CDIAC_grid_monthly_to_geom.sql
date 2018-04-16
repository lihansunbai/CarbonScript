SELECT AddGeometryColumn (
-- 如果表建立在非 public schem 下，第一个参数为 schem
    'grid_co2',
    'cdiac_co2_grid_monthly',
    'geom',
    4326,
    'POINT',
    2
);

-- 如果表建立在非 public schem 下，需要通过 schem 出现在表前
-- UPDATE schem.table
UPDATE  cgrid_co2.diac_co2_grid_monthly
    SET geom = ST_GeomFromText('POINT('||longitude||' '||latitude||')',4326);

