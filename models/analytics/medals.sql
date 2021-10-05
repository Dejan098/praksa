select
  medal_type as type,
  medal_date as won_on_date,
  date_part('day', "medal_date"::date) - date_part('day', '2021-07-24'::date)+1 as won_on_day,
  initcap(athlete_name) as athlete_name,
  case athlete_sex
    when 'M' then 'Male'
    when 'W' then 'Female'	
  end as athlete_gender,
  country as athlete_country,
  discipline,
  event
  

from {{ source('imported_data', 'medals') }}
