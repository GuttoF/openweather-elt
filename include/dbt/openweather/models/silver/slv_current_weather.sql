with ranked as (
    select
        *,
        row_number() over (
            partition by city, observed_at
            order by _dlt_load_id desc
        ) as rn
    from {{ ref('stg_current_weather') }}
)

select
    city,
    uf,
    lat,
    lon,
    country,
    observed_at,
    weather_main,
    weather_description,
    round(temp_k - 273.15, 2)       as temp_c,
    round(feels_like_k - 273.15, 2) as feels_like_c,
    round(temp_min_k - 273.15, 2)   as temp_min_c,
    round(temp_max_k - 273.15, 2)   as temp_max_c,
    pressure,
    humidity,
    wind_speed,
    wind_deg,
    clouds_all
from ranked
where rn = 1
