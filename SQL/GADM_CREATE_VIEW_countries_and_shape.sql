CREATE VIEW countries.countries(
    iso,
    name_iso,
    name_engli,
    name_ediga_dataset,
    name_cdica_country_codes, 
    name_fao, 
    name_local, 
    name_chine, 
    sovereign, 
    iso2, 
    fips, 
    ison, 
    unregion1, 
    unregion2,
    gid,
    geom
)
AS
SELECT 
    countries_index.iso as iso,
    countries_index.name_iso as name_iso,
    countries_index.name_engli as name_engli,
    countries_index.name_ediga_dataset as name_ediga_dataset,
    countries_index.name_cdica_country_codes as name_cdica_country_codes, 
    countries_index.name_fao as name_fao, 
    countries_index.name_local as name_local, 
    countries_index.name_chine as name_chine, 
    countries_index.sovereign as sovereign, 
    countries_index.iso2 as iso2, 
    countries_index.fips as fips, 
    countries_index.ison as ison, 
    countries_index.unregion1 as unregion1, 
    countries_index.unregion2 as unregion2,
    gadm.gid as gid,
    gadm.geom as geom
FROM countries.countries_index, countries.gadm 
WHERE countries.countries_index.iso = countries.gadm.iso ;

