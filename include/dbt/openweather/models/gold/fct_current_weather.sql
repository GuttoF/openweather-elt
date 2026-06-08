select
    md5(s.city || '|' || s.uf)                  as city_sk,
    to_char(date_trunc('hour', s.observed_at), 'YYYYMMDDHH24') as datetime_sk,
    s.observed_at,
    s.weather_main,
    s.weather_description,
    s.temp_c,
    s.feels_like_c,
    s.temp_min_c,
    s.temp_max_c,
    s.pressure,
    s.humidity,
    s.wind_speed,
    s.wind_deg,
    s.clouds_all
from {{ ref('slv_current_weather') }} as s
