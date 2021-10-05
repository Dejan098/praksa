select
  medal_type,
  medal_date as won_on_date,
  date_part('day', "medal_date"::date) - date_part('day', '2021-07-24'::date)+1 as won_on_day,
  initcap(athlete_name) as athlete_name,
  case
    when athlete_sex ='M' then 'Male'
    when  athlete_sex ='W' then N 'Female'	
  end as gender,
  country,
  discipline,
  event
  

from {{ source('imported_data', 'medals') }}
