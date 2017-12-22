/*************************************************************
****
**** edgar_nation_convet function summrized edgar nation data
**** which was categoried in sectors.
****
**** Parameter:
****    startYear: the year of time series begin
****     endYear: the year of time series end
****
*************************************************************/

CREATE OR REPLACE FUNCTION edgar_nation_convet(startYear int, endYear int) RETURNS void AS $$
DECLARE
    startYear integer := $1;
    endYear integer := $2;
    result_sql text := '';

BEGIN

    --Constructing view colum 
    FOR i IN startYear .. endYear LOOP
        result_sql := result_sql || 'sum(emission_' || i || ') AS '||'yr' || i || ',';
    END LOOP;
    SELECT rtrim(result_sql,',') INTO result_sql;

    --create view.
    EXECUTE format('CREATE VIEW historical_emission.edgar_nations'
        '_%s_%s '
        'AS SELECT iso_code, iso_country, %s '
        'FROM historical_emission.edgar_global_ffco2 '
        'GROUP BY iso_code,iso_country;',
        startYear,endYear,result_sql);

END;
$$ LANGUAGE plpgsql
