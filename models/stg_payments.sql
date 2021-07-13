select
    id as payment_id,
    orderid as order_id,
    created as payment_date,
    status,
    amount

from raw.stripe.payment