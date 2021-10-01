with medals as (
	select
           athlete_name as name,
           sum(case when medal_type = 'Gold Medal' then 1 else 0 end) as gold_medal_count,
           sum(case when medal_type = 'Silver Medal' then 1 else 0 end) as silver_medal_count,
           sum(case when medal_type = 'Bronze Medal' then 1 else 0 end) as bronze_medal_count
	from {{ source('imported_data', 'medals') }}
	 
	group by athlete_name


)



select
  initcap(name) as name,
  short_name as short_name,
  gender as gender,
  birth_date as birth_date,
  birth_place as birth_place,
  birth_country as birth_country,
  country as country,
  country_code as country_code,
  discipline as discipline,
  discipline_code as discipline_code,
  residence_place as residence_place,
  substring("height_m/ft", 1, 4) as height_m,
  url as url,
  DATE_PART('year', '2012-01-01'::date) - DATE_PART('year', "birth_date"::date) as age,
  medals.gold_medal_count as gold_medal_count,
  medals.silver_medal_count as silver_medal_count,
  medals.bronze_medal_count as bronze_medal_count

from {{ source('imported_data', 'athletes') }}

left join medals using (name)

