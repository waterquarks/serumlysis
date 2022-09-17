import json
import sqlite3
from pathlib import Path


def main():
    db = sqlite3.connect(Path(__file__).parent / 'app.db')

    db.execute('drop table if exists orders')

    db.execute("""
        create table orders (
            exchange text,
            symbol text,
            side text,
            price real,
            account text,
            size real,
            id text,
            primary key (exchange, symbol, side, id)
        ) without rowid, strict;
    """)

    db.execute('drop table if exists quotes')

    db.execute("""
        create table quotes (
            exchange text,
            symbol text,
            size real,
            mid_price real,
            weighted_average_buy_price real,
            weighted_average_sell_price real,
            timestamp text,
            primary key (exchange, symbol, timestamp, size)
        ) without rowid, strict
    """)

    for exchange, symbol, is_snapshot, orders, timestamp, is_target in db.execute("""
        with
            instructions as (
                select
                    exchange,
                    symbol,
                    content->>'type' = 'l3snapshot' as is_snapshot,
                    json_group_array(
                        json_object(
                            'side', case when value->>'side' = 'buy' then 'bids' when value->>'side' = 'sell' then 'asks' end,
                            'price', coalesce(value->>'price', 0),
                            'size', coalesce(value->>'size', 0),
                            'account', value->>'account',
                            'id', value->>'orderId'
                        )
                    ) as orders,
                    content->>'timestamp' as timestamp,
                    coalesce(strftime('%Y-%m-%dT%H:%M:00.000Z', content->>'timestamp') != lag(strftime('%Y-%m-%dT%H:%M:00.000Z', content->>'timestamp')) over (partition by exchange, symbol order by content->>'timestamp'), true) as is_target
                from messages, json_each(
                    case
                        when content->>'type' = 'l3snapshot' then
                            (
                                select
                                    json_group_array(json(value)) as value
                                from (
                                    select value from json_each(content->'asks')
                                    union all
                                    select value from json_each(content->'bids')
                                )
                            )
                        else json_array(json(content))
                    end
                )
                where exchange = 'Mango Markets'
                  and symbol = 'SOL/USDC'
                  and content->>'type' in ('l3snapshot', 'open', 'done')
                group by exchange, symbol, content->>'timestamp'
                order by exchange, symbol, content->>'timestamp'
            )
        select
            exchange,
            symbol,
            is_snapshot,
            (
                with
                    scratch as (
                        select value from json_each(orders) order by value->>'price' desc
                    ),
                    split as (
                        select
                            value->>'side' as side,
                            json_group_array(json_remove(value, '$.side')) as orders
                        from scratch group by side
                    )
                select json_group_object(side, json(orders)) from split
            ) as orders,
            timestamp,
            is_target
        from instructions;
    """):
        if is_snapshot:
            db.execute('delete from orders where exchange = ? and symbol = ?', [exchange, symbol])

        for side in ['bids', 'asks']:
            for order in json.loads(orders).get(side) or []:
                if order['price'] == 0:
                    db.execute('delete from orders where exchange = ? and symbol = ? and id = ?', [exchange, symbol, order['id']])
                else:
                    db.execute(
                        'insert or replace into orders values (?, ?, ?, ?, ?, ?, ?)',
                        [exchange, symbol, side, order['price'], order['account'], order['size'], order['id']]
                    )
        else:
            if not is_target:
                continue

            for size in [1000, 10000, 25000, 50000, 100000]:
                db.execute("""
                    insert into quotes
                    with
                        orders as (
                            select
                                exchange,
                                symbol,
                                side,
                                price,
                                size,
                                price * size as volume,
                                sum(price * size) over (partition by exchange, symbol, side order by case when side = 'bids' then - price when side = 'asks' then price end) as cumulative_volume
                            from (
                                select
                                    exchange,
                                    symbol,
                                    side,
                                    price,
                                    sum(size) as size
                                from main.orders
                                group by exchange, symbol, side, price
                            )
                            order by exchange, symbol, side, case when side = 'bids' then - price when side = 'asks' then price end
                        ),
                        fills as (
                            select
                                exchange,
                                symbol,
                                side,
                                price,
                                fill,
                                sum(fill) over (
                                    partition by side order by case when side = 'bids' then - price when side = 'asks' then price end
                                ) as cumulative_fill
                            from (
                                select
                                    exchange,
                                    symbol,
                                    side,
                                    price,
                                    case
                                        when cumulative_volume < :size then volume
                                        else coalesce(lag(remainder) over (partition by exchange, symbol, side), case when volume < :size then volume else :size end)
                                    end as fill
                                from (select *, :size - cumulative_volume as remainder from orders)
                            )
                            where fill > 0
                        ),
                        weighted_average_fill_prices as (
                            select
                                exchange,
                                symbol,
                                :size as size,
                                case when sum(case when side = 'asks' then fill end) = :size then sum(case when side = 'asks' then price * fill end) / :size end as weighted_average_buy_price,
                                case when sum(case when side = 'bids' then fill end) = :size then sum(case when side = 'bids' then price * fill end) / :size end as weighted_average_sell_price,
                                :timestamp as timestamp
                            from fills
                            group by exchange, symbol, timestamp, size
                        ),
                        misc as (
                            select
                                exchange,
                                symbol,
                                (top_bid + top_ask) / 2 as mid_price
                            from (
                                select
                                    exchange,
                                    symbol,
                                    max(price) filter ( where side = 'bids') as top_bid,
                                    min(price) filter ( where side = 'asks') as top_ask
                                from orders
                                group by exchange, symbol
                            )
                        )
                    select
                        exchange,
                        symbol,
                        size,
                        mid_price,
                        weighted_average_buy_price,
                        weighted_average_sell_price,
                        timestamp
                    from weighted_average_fill_prices
                    inner join misc using (exchange, symbol);
                """, {'size': size, 'timestamp': timestamp})

            print(timestamp)

        db.commit()


if __name__ == '__main__':
    main()