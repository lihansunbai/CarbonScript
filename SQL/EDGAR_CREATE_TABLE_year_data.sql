CREATE TABLE grid_co2.EDGAR_CO2_grid_year(
    year INT,
    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION,
    CO2_Con DOUBLE PRECISION
);

CREATE INDEX index_edgar_grid_year ON grid_co2.EDGAR_CO2_grid_year USING btree (year);
CREATE INDEX index_edgar_grid_geom ON grid_co2.EDGAR_CO2_grid_year USING GIST (geom);
