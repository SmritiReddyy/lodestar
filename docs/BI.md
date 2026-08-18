# BI layer

The marts are the contract. A dashboard reads `marts` and nothing else — the BI
service account has no IAM binding for `raw`, `staging` or `intermediate`, so
that boundary is enforced rather than merely documented.

## Connecting

### Metabase (local, free)

```bash
docker run -d -p 3000:3000 --name metabase \
  -v "$(pwd)/data:/data" \
  metabase/metabase
```

Metabase has no DuckDB driver out of the box. For a local demo the simplest
route is to point it at BigQuery, or install the community DuckDB driver into
`plugins/`. For exploring locally without any of that:

```bash
duckdb data/lodestar.duckdb
```

### Looker Studio (BigQuery)

Connects natively. Add `marts.agg_daily_performance` as the primary source and
join `dim_date` for period comparisons. Authenticate as the BI service account
from `terraform output bi_service_account` so the dashboard inherits
marts-only access.

## The three dashboards the model is built for

### 1. Revenue trend

**Source:** `agg_daily_performance` joined to `dim_date`.

```sql
select
    d.month_start_date,
    a.customer_region,
    sum(a.total_revenue)      as revenue,
    sum(a.gross_item_revenue) as product_revenue,
    sum(a.freight_revenue)    as shipping_revenue,
    sum(a.orders_placed)      as orders
from marts.agg_daily_performance a
join marts.dim_date d on a.purchased_date = d.date_key
group by 1, 2
order by 1, 2;
```

Splitting product from shipping revenue is deliberate — they behave differently
and blending them hides margin changes.

### 2. Delivery SLA breaches

**Source:** `agg_daily_performance` for the trend, `dim_sellers` for the
offender list.

```sql
-- rolling late rate
select
    d.week_start_date,
    a.customer_state,
    sum(a.orders_late)                                as late_orders,
    sum(a.orders_delivered)                           as delivered_orders,
    safe_divide(sum(a.orders_late), sum(a.orders_delivered)) as late_rate
from marts.agg_daily_performance a
join marts.dim_date d on a.purchased_date = d.date_key
group by 1, 2
having delivered_orders >= 20     -- suppress noise from tiny denominators
order by late_rate desc;
```

```sql
-- worst sellers by late rate, with enough volume to be meaningful
select
    seller_key, seller_city, seller_state,
    delivered_order_count, late_order_count,
    late_delivery_rate, avg_days_to_delivery
from marts.dim_sellers
where delivered_order_count >= 50
order by late_delivery_rate desc
limit 25;
```

The `having` / `where` volume floors matter: without them the top of a "worst
late rate" list is entirely sellers with three orders and one hiccup.

`late_delivery_rate` is null when nothing has been delivered, rather than zero.
Charting a null as 0% would claim perfect performance where there is simply no
data.

### 3. Top categories by region

```sql
select
    a.customer_region,
    a.category_name,
    sum(a.total_revenue)          as revenue,
    sum(a.item_count)             as units,
    avg(a.avg_review_score)       as review_score,
    avg(a.avg_freight_ratio)      as freight_ratio
from marts.agg_daily_performance a
where a.purchased_date >= date_sub(current_date(), interval 365 day)
group by 1, 2
qualify row_number() over (partition by a.customer_region order by revenue desc) <= 10
order by a.customer_region, revenue desc;
```

## Notes for whoever builds the dashboards

**Use the aggregate, not the fact.** `agg_daily_performance` exists so panels do
not rescan `fct_order_items` on every refresh — 2.65× faster locally, and on
BigQuery the difference is in bytes billed. Drop to the fact only when you need
a grain the aggregate does not carry (per product, per seller, per order line).

**Order-level measures cannot be summed from the item grain.** `orders_placed`,
`orders_late` and `avg_review_score` are aggregated separately in the model and
joined back at day × state precisely to avoid that double count. Do not try to
recompute them from item-level columns.

**Exclude the flagged rows explicitly.** `is_missing_payment`,
`is_missing_items` and `is_missing_delivery_timestamp` are carried onto
`fct_orders` so a dashboard can filter them deliberately rather than having them
silently dropped upstream. If a panel's numbers need to reconcile to finance,
filter them; if it is an operational count, do not.

**Point-in-time seller attribution.** `dim_sellers` holds current attributes. To
attribute historical revenue to where a seller *was* at the time, join
`dim_sellers_history`:

```sql
from marts.fct_order_items i
join marts.dim_sellers_history h
  on i.seller_key = h.seller_key
 and i.purchased_at >= h.valid_from
 and (i.purchased_at < h.valid_to or h.valid_to is null)
```

Otherwise a seller relocating from São Paulo to Curitiba silently rewrites last
year's regional revenue split.
