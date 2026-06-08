with instants as (
    select distinct date_trunc('hour', observed_at) as ts from {{ ref('slv_current_weather') }}
    union
    select distinct date_trunc('hour', forecast_at) as ts from {{ ref('slv_forecast') }}
)

select
    to_char(ts, 'YYYYMMDDHH24')    as datetime_sk,
    ts                             as datetime,
    ts::date                       as date,
    extract(year   from ts)::int   as year,
    extract(month  from ts)::int   as month,
    extract(day    from ts)::int   as day,
    extract(hour   from ts)::int   as hour,
    extract(isodow from ts)::int   as day_of_week,  -- 1=segunda ... 7=domingo
    to_char(ts, 'TMDay')           as day_name
from instants
