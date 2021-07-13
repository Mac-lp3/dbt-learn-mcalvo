with payments as (

    select * from {{ ref('stg_payments') }}

),

stgorders as (

    select * from {{ ref('stg_orders') }}

),

order_payments as (
    select
        order_id,
        sum(case when status = 'success' then amount end) as amount
    from payments
    group by 1
),

final as (
    select
        stgorders.order_id,
        stgorders.customer_id,
        order_payments.amount,
        order_date
    
    from stgorders
    left join order_payments using (order_id)
)

select * from final