
  
    
    

    create  table
      "footprint"."main"."country_trends__dbt_tmp"
  
    as (
      SELECT
    country,
    year,
    carbon_footprint,
    AVG(carbon_footprint)
      OVER (
        PARTITION BY country
        ORDER BY year
        ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
      ) AS rolling_3y_avg
FROM "footprint"."main"."stg_footprint"
    );
  
  