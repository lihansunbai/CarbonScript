SELECT AddGeometryColumn (
    'co2_grid_year',
    'geom',
    4326,
    'POINT',
    2
);

UPDATE co2_grid_year 
    SET geom = ST_GeomFromText('POINT('||longitude||' '||latitude||')',4326);
