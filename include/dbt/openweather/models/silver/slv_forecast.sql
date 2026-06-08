with ranked as (
    select
        *,
        row_number() over (
            partition by city, forecast_at
            order by _dlt_load_id desc
        ) as rn
    from {{ ref('stg_forecast') }}
)

select
    city,
    uf,
    lat,
    lon,
    country,
    forecast_at,
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
    clouds_all,
    pop
from ranked
where rn = 1
