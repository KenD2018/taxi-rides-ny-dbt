{{ config(materialized='table') }}

select
    -- 使用函数提取日期 📅
    date(pickup_datetime) as pickup_date,
    -- 计算平均时长 📈
    avg(trip_duration) as avg_trip_duration,
    -- 统计行程单数 🚖
    count(*) as total_trips
from {{ ref('stg_yellow_tripdata') }}
group by 1
order by 1