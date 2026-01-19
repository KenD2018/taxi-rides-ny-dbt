{{ config(materialized='view') }}

with raw_data as (
    select *,
        row_number() over(partition by vendorid, tpep_pickup_datetime) as rn
    from {{ source('staging', 'external_yellow_tripdata') }}
),

-- 新增这一层，专门用来做计算和起名字 🏗️  -- 1. 这里先保持数字类型，方便后面做数学比较
-- ... 前面的代码保持不变 ...
renamed_and_calculated as (
    select
        vendorid,  -- 👈 先保持原始类型（数字）进行计算和筛选
        passenger_count,
        -- 👇 2. 在这里把它传递给下游
        trip_distance,
        -- ... 其他字段 ...
        timestamp_diff(cast(tpep_dropoff_datetime as timestamp), cast(tpep_pickup_datetime as timestamp), minute) as trip_duration,
        cast(tpep_pickup_datetime as timestamp) as pickup_datetime,
        cast(tpep_dropoff_datetime as timestamp) as dropoff_datetime,
        cast(fare_amount as numeric) as fare_amount
    from raw_data
    where rn = 1
),

final_conversion as (
    select
        -- 👈 在最后这一层，才把需要做测试的字段转成 STRING，  -- 2. 在最后输出层，才转换成 STRING 供测试使用
        cast(vendorid as string) as vendorid,
        cast(passenger_count as string) as passenger_count,
        -- 👇 1. 在这里把原始数据的距离字段选进来
        trip_distance,
        pickup_datetime,
        dropoff_datetime,
        trip_duration,
        fare_amount
    from renamed_and_calculated
    where passenger_count > 0   -- ✅ 此时还是数字，可以安全使用 > 0
      and trip_duration > 0     -- ✅ 此时还是数字
),

-- 1. 引用刚才生成的种子表
taxi_type_lookup as (
    select * from {{ ref('taxi_type_lookup') }}
)

-- 2. 通过 JOIN 把 vendor_name 加上去
select 
    f.*,
    l.vendor_name
from final_conversion f
left join taxi_type_lookup l
  -- 注意：seed里vendorid默认是数字，我们的final里是string，所以要转换一下才能对上
  on f.vendorid = cast(l.vendorid as string)