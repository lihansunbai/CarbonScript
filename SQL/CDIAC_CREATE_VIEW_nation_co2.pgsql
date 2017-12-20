/*************************************************************
****   Function convert cdiac emission data to country and 
**** emission of the year type. Pramater source is the colum
**** of emission types, such as total.
****
**** parameter:
****     source: emission types. total, solid, liquid et al.
****     startYear: convert start year. First year is 1751.
****     endYear: convert end year. Last year is 2014.
****
*************************************************************/

CREATE OR REPLACE FUNCTION cdiac_nation_convet(source text,startYear int, endYear int) RETURNS void AS $$
DECLARE
    startYear integer := $2;
    endYear integer := $3;
    sourceColum text := source;
    result_sql text := '';
    source_sql text;
    category_sql text;
    temptext text;

BEGIN

    --Constructing retrn set of 
    FOR i IN startYear .. endYear LOOP
        result_sql := result_sql || 'yr' || i || ' DOUBLE PRECISION,';
    END LOOP;
    SELECT rtrim(result_sql,',') INTO result_sql;
    SELECT concat('nation TEXT,',result_sql) INTO temptext;
    SELECT temptext INTO result_sql;

    --Constructing emission source sql
    EXECUTE format('SELECT ''SELECT nation, year,%s FROM '
        'historical_emission.cdiac_nations_co2 '
        'where year >= %s and year <= %s '
        'ORDER BY nation;'';',sourceColum,startYear,endYear)
    INTO source_sql;

    --Constructing emission category sql
    category_sql := 'SELECT DISTINCT year '
    || 'FROM historical_emission.cdiac_nations_co2 '
    || 'where year >= ' || startYear || ' and '
    || 'year <= ' || endYear || 'ORDER BY year;';

    --Use crosstab function to create view.
    EXECUTE format('CREATE VIEW historical_emission.cdiac_nations_%s'
        '_%s_%s '
        'AS SELECT * FROM crosstab(''%s'',''%s'') '
        'AS ct(%s);',
        sourceColum,startYear,endYear,
        source_sql,category_sql,result_sql);

END;
$$ LANGUAGE plpgsql
