with cities as (
    select distinct city, uf, lat, lon, country from {{ ref('slv_current_weather') }}
    union
    select distinct city, uf, lat, lon, country from {{ ref('slv_forecast') }}
)

select
    md5(city || '|' || uf) as city_sk,
    city,
    uf,
    lat,
    lon,
    country
from cities
