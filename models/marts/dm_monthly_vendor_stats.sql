{{ config(materialized='table') }} -- 汇总表建议物化为 table 提高查询速度

with trips_data as (
    select * from {{ ref('stg_yellow_tripdata') }}
)

select 
    -- 1. 时间维度：按月汇总
    date_trunc(pickup_datetime, month) as revenue_month,
    
    -- 2. 供应商维度：直接使用你刚才用 Seed 翻译好的名称
    vendor_name,
    
    -- 3. 业务指标
    count(vendorid) as total_monthly_trips,
    -- 👈 关键点：在这里把 STRING 转回 INT64 才能求平均值
    avg(cast(passenger_count as int64)) as avg_passenger_count,
    
    -- 👇 新增的距离指标
    avg(trip_distance) as avg_trip_distance,

    -- 👇 新增的效率指标
    avg(cast(trip_duration as int64)) as avg_trip_duration,

    

    -- fare_amount 本身就是 NUMERIC，所以可以直接 SUM
    sum(fare_amount) as total_monthly_revenue

    
from trips_data
group by 1, 2
order by 1, 3 desc