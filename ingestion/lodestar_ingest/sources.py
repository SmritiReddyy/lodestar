"""Registry of the source tables Lodestar ingests.

The column names mirror the public Olist Brazilian e-commerce dataset
exactly (including the two misspelled `_lenght` columns), so the pipeline
runs unchanged against either the real Kaggle export or the synthetic
generator in `generate.py`.
"""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(frozen=True)
class SourceTable:
    """One raw table: where it comes from and how it is loaded."""

    name: str
    filename: str
    primary_key: tuple[str, ...]
    columns: dict[str, str]
    # "full" reloads the whole table each run; "incremental" appends the
    # current partition only. Olist is a static export, so most tables are
    # full — orders/order_items carry event timestamps and load incrementally.
    load_strategy: str = "full"
    watermark_column: str | None = None
    description: str = ""
    tags: tuple[str, ...] = field(default_factory=tuple)

    @property
    def column_names(self) -> list[str]:
        return list(self.columns)


# Types are the *logical* types used to build the warehouse DDL. They are
# mapped to BigQuery / DuckDB types in `warehouse.py`.
SOURCE_TABLES: tuple[SourceTable, ...] = (
    SourceTable(
        name="customers",
        filename="olist_customers_dataset.csv",
        primary_key=("customer_id",),
        columns={
            "customer_id": "string",
            "customer_unique_id": "string",
            "customer_zip_code_prefix": "string",
            "customer_city": "string",
            "customer_state": "string",
        },
        description="One row per order-scoped customer key.",
        tags=("dimension",),
    ),
    SourceTable(
        name="geolocation",
        filename="olist_geolocation_dataset.csv",
        primary_key=(),  # genuinely has duplicates upstream; deduped in staging
        columns={
            "geolocation_zip_code_prefix": "string",
            "geolocation_lat": "float",
            "geolocation_lng": "float",
            "geolocation_city": "string",
            "geolocation_state": "string",
        },
        description="Lat/lng samples per zip prefix. Duplicated by design upstream.",
        tags=("dimension",),
    ),
    SourceTable(
        name="order_items",
        filename="olist_order_items_dataset.csv",
        primary_key=("order_id", "order_item_id"),
        columns={
            "order_id": "string",
            "order_item_id": "integer",
            "product_id": "string",
            "seller_id": "string",
            "shipping_limit_date": "timestamp",
            "price": "float",
            "freight_value": "float",
        },
        load_strategy="incremental",
        watermark_column="shipping_limit_date",
        description="Line items. Grain: order_id + order_item_id.",
        tags=("fact",),
    ),
    SourceTable(
        name="order_payments",
        filename="olist_order_payments_dataset.csv",
        primary_key=("order_id", "payment_sequential"),
        columns={
            "order_id": "string",
            "payment_sequential": "integer",
            "payment_type": "string",
            "payment_installments": "integer",
            "payment_value": "float",
        },
        description="Payment splits. An order can settle across several rows.",
        tags=("fact",),
    ),
    SourceTable(
        name="order_reviews",
        filename="olist_order_reviews_dataset.csv",
        primary_key=("review_id", "order_id"),
        columns={
            "review_id": "string",
            "order_id": "string",
            "review_score": "integer",
            "review_comment_title": "string",
            "review_comment_message": "string",
            "review_creation_date": "timestamp",
            "review_answer_timestamp": "timestamp",
        },
        description="Post-delivery survey responses.",
        tags=("fact",),
    ),
    SourceTable(
        name="orders",
        filename="olist_orders_dataset.csv",
        primary_key=("order_id",),
        columns={
            "order_id": "string",
            "customer_id": "string",
            "order_status": "string",
            "order_purchase_timestamp": "timestamp",
            "order_approved_at": "timestamp",
            "order_delivered_carrier_date": "timestamp",
            "order_delivered_customer_date": "timestamp",
            "order_estimated_delivery_date": "timestamp",
        },
        load_strategy="incremental",
        watermark_column="order_purchase_timestamp",
        description="Order header. Grain: order_id.",
        tags=("fact",),
    ),
    SourceTable(
        name="products",
        filename="olist_products_dataset.csv",
        primary_key=("product_id",),
        columns={
            "product_id": "string",
            "product_category_name": "string",
            # Upstream typo preserved deliberately; renamed in staging.
            "product_name_lenght": "integer",
            "product_description_lenght": "integer",
            "product_photos_qty": "integer",
            "product_weight_g": "float",
            "product_length_cm": "float",
            "product_height_cm": "float",
            "product_width_cm": "float",
        },
        description="Product catalogue with Portuguese category names.",
        tags=("dimension",),
    ),
    SourceTable(
        name="sellers",
        filename="olist_sellers_dataset.csv",
        primary_key=("seller_id",),
        columns={
            "seller_id": "string",
            "seller_zip_code_prefix": "string",
            "seller_city": "string",
            "seller_state": "string",
        },
        description="Marketplace sellers.",
        tags=("dimension",),
    ),
    SourceTable(
        name="product_category_translation",
        filename="product_category_name_translation.csv",
        primary_key=("product_category_name",),
        columns={
            "product_category_name": "string",
            "product_category_name_english": "string",
        },
        description="Portuguese to English category lookup.",
        tags=("dimension",),
    ),
)

TABLES_BY_NAME: dict[str, SourceTable] = {t.name: t for t in SOURCE_TABLES}


def get_table(name: str) -> SourceTable:
    try:
        return TABLES_BY_NAME[name]
    except KeyError:
        known = ", ".join(sorted(TABLES_BY_NAME))
        raise KeyError(f"Unknown source table {name!r}. Known tables: {known}") from None
