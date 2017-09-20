SELECT AddGeometryColumn (
    'cdica_co2_grid_monthly'
    'geom',
    4326,
    'POINT',
    2
);

UPDATE  cdica_co2_grid_monthly
    SET geom = ST_GeomFromText('POINT('||longitude||' '||latitude||')',4326);
