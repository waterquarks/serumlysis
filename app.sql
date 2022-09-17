-- Performance overview
with
    trades as (
        select
            owner,
            case when owner = '9BVcYqEQxyccuwznvxXqDkSJFavvTyheiTYk231T1A8S' then 'Mango Markets' else 'Others' end as entity,
            case when maker then 'maker' when not maker then 'taker' end as type,
            abs(cast(case when bid then "nativeQuantityPaid" when not bid then "nativeQuantityReleased" end as bigint) / 1e6) as volume,
            cast("nativeFeeOrRebate" as real) / 1e6 as fees,
            strftime('%Y-%W', datetime(replace(loadTimestamp, '+00', ''))) as week
        from event
            left join owner using ("openOrders")
        where "quoteCurrency" = 'USDC'
          and fill
          and "loadTimestamp" >= '2022-05-23'
          and "loadTimestamp" < '2022-09-12'
    ),
    trades_by_entity as (
        select
            week,
            round(sum(volume)) as volume,
            round(sum(case when entity = 'Mango Markets' and type = 'taker' then volume end)) as taker_volume_mango,
            round(sum(case when entity = 'Mango Markets' and type = 'maker' then volume end)) as maker_volume_mango,
            round(sum(case when entity = 'Mango Markets' then volume end)) as volume_mango,
            round(sum(case when entity = 'Mango Markets' then fees end)) as fees_mango,
            round(sum(case when entity = 'Others' and type = 'taker' then volume end)) as taker_volume_others,
            round(sum(case when entity = 'Others' and type = 'maker' then volume end)) as maker_volume_others,
            round(sum(case when entity = 'Others' then volume end)) as volume_others,
            round(sum(case when entity = 'Others' then fees end)) as fees_others
        from trades
        group by week
        order by week
    )
select
    week,
    taker_volume_mango,
    maker_volume_mango,
    volume_mango,
    fees_mango,
    taker_volume_others,
    maker_volume_others,
    volume_others,
    fees_others,
    volume_mango + volume_others as total_volume,
    volume_mango / (volume_mango + volume_others) as volume_mango_to_total_volume,
    fees_mango + fees_others as total_fees,
    fees_mango / (fees_mango + fees_others) as fees_mango_to_total_fees
from trades_by_entity;

-- Pivoted slippages - run main.py first
with
    aggregate as (
        select
            exchange,
            symbol,
            json_group_array((weighted_average_buy_price - weighted_average_sell_price) / weighted_average_buy_price) as spreads,
            timestamp
        from quotes
        where mid_price is not null
          and ((weighted_average_buy_price - weighted_average_sell_price) / weighted_average_buy_price) > 0
        group by exchange, symbol, timestamp
    ),
    partitions as (
        select
            exchange,
            symbol,
            strftime('%m/%d/%Y %H:%M:00', timestamp) as timestamp,
            spreads->>'$[0]' as "1000",
            spreads->>'$[1]' as "10000",
            spreads->>'$[2]' as "25000",
            spreads->>'$[3]' as "50000",
            spreads->>'$[4]' as "100000",
            dense_rank() over (order by date(timestamp)) as id
        from aggregate
    )
select
    symbol,
    timestamp,
    "1000",
    "10000",
    "25000",
    "50000",
    "100000"
from partitions;

