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
