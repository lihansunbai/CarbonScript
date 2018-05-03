CREATE TABLE grid_co2.EDGAR_CO2_grid_year(
    edgar_ver TEXT,
    substance TEXT,
    substance_info TEXT,
    yr INT,
    categories_abbr TEXT,
    categories_edgar TEXT,
    categories_ipcc TEXT,
    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION,
    emi DOUBLE PRECISION
);

CREATE INDEX index_edgar_grid_year ON grid_co2.EDGAR_CO2_grid_year USING btree (yr);
CREATE INDEX index_edgar_grid_cate ON grid_co2.EDGAR_CO2_grid_year USING btree (categories_abbr);
