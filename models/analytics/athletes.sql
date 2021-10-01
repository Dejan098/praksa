with medals as (
	select
           athlete_name as name,
           sum(case when medal_type = 'Gold Medal' then 1 else 0 end) as gold_medal_count,
           sum(case when medal_type = 'Silver Medal' then 1 else 0 end) as silver_medal_count,
           sum(case when medal_type = 'Bronze Medal' then 1 else 0 end) as bronze_medal_count
	from {{ source('imported_data', 'medals') }}
	 
	group by athlete_name,country,athlete_sex,discipline


)



select
  initcap(name) as name,
  initcap(short_name) as short_name,
  gender as gender,
  birth_date as birth_date,
  initcap(birth_place) as birth_place,
  birth_country as birth_country,
  country as country,
  country_code as country_code,
  discipline as discipline,
  discipline_code as discipline_code,
  initcap(residence_place) as residence_place,
  left("height_m/ft", strpos("height_m/ft", '/') - 1)::float as height_m,
  url as url,
  date_part('year', '2012-01-01'::date) - date_part('year', "birth_date"::date) as age,
  coalesce(medals.gold_medal_count,0) as gold_medal_count,
  coalesce(medals.silver_medal_count,0) as silver_medal_count,
  coalesce(medals.bronze_medal_count,0) as bronze_medal_count

from {{ source('imported_data', 'athletes') }}

left join medals using (name)

