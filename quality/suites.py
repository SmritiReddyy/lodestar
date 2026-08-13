"""Great Expectations suites for the Lodestar mart layer.

These deliberately do *not* duplicate the dbt tests. dbt already owns the
row-level structural checks — uniqueness, not-null, referential integrity,
accepted values — and it owns them well, close to the model definitions.

What dbt schema tests cannot express is the shape of a column: "revenue is
still distributed the way it was last month", "the 99th percentile freight
ratio has not doubled", "the table has roughly the row count we expect". Those
are the failures that pass every row-level test while quietly making a
dashboard wrong, and they are what lives here.
"""

from __future__ import annotations

from dataclasses import dataclass, field

import great_expectations as gx
from great_expectations import expectations as gxe


@dataclass
class SuiteSpec:
    """One table's worth of checks, plus how to fetch it."""

    name: str
    schema: str
    table: str
    expectations: list = field(default_factory=list)
    # Only these columns are pulled into memory. Keeping the projection narrow
    # is what makes a pandas-backed validation affordable on a wide fact table.
    columns: list[str] = field(default_factory=list)
    where: str | None = None


def fct_orders_suite(expected_rows: tuple[int, int]) -> SuiteSpec:
    return SuiteSpec(
        name="fct_orders",
        schema="marts",
        table="fct_orders",
        columns=[
            "order_key",
            "order_status",
            "delivery_status",
            "order_total",
            "gross_item_revenue",
            "freight_revenue",
            "payment_total",
            "payment_variance",
            "item_count",
            "days_to_delivery",
            "avg_review_score",
        ],
        expectations=[
            # --- volume ---------------------------------------------------
            # A run that loses half the table passes every row-level test.
            gxe.ExpectTableRowCountToBeBetween(
                min_value=expected_rows[0], max_value=expected_rows[1]
            ),
            # --- structure ------------------------------------------------
            gxe.ExpectColumnValuesToNotBeNull(column="order_key"),
            gxe.ExpectColumnValuesToBeUnique(column="order_key"),
            gxe.ExpectColumnValuesToBeInSet(
                column="delivery_status",
                value_set=[
                    "delivered_on_time",
                    "delivered_late",
                    "in_transit",
                    "approved",
                    "pending",
                    "canceled",
                ],
            ),
            # --- distribution ---------------------------------------------
            # Bounds are wide on purpose: this is a tripwire for a broken
            # currency conversion or a units change, not a business target.
            gxe.ExpectColumnValuesToBeBetween(column="order_total", min_value=0, max_value=50_000),
            gxe.ExpectColumnMeanToBeBetween(column="order_total", min_value=40, max_value=1_000),
            gxe.ExpectColumnQuantileValuesToBeBetween(
                column="order_total",
                quantile_ranges={
                    "quantiles": [0.5, 0.95, 0.99],
                    "value_ranges": [[30, 500], [200, 3_000], [400, 8_000]],
                },
            ),
            gxe.ExpectColumnValuesToBeBetween(column="item_count", min_value=0, max_value=100),
            gxe.ExpectColumnValuesToBeBetween(
                column="avg_review_score",
                min_value=1,
                max_value=5,
                mostly=1.0,
            ),
            # --- business rule --------------------------------------------
            # Payments must settle the order. Allows for the known slice of
            # orders with no payment rows at all, which is measured separately.
            gxe.ExpectColumnValuesToBeBetween(
                column="payment_variance",
                min_value=-0.01,
                max_value=0.01,
                mostly=0.99,
            ),
            # Delivery cannot take a negative number of days, and a delivery
            # taking over a year means a date-parsing bug.
            gxe.ExpectColumnValuesToBeBetween(
                column="days_to_delivery",
                min_value=0,
                max_value=400,
                mostly=1.0,
            ),
        ],
    )


def fct_order_items_suite(expected_rows: tuple[int, int]) -> SuiteSpec:
    return SuiteSpec(
        name="fct_order_items",
        schema="marts",
        table="fct_order_items",
        columns=[
            "order_item_key",
            "order_key",
            "item_price",
            "freight_value",
            "item_total",
            "freight_ratio",
            "category_name",
            "product_weight_g",
        ],
        expectations=[
            gxe.ExpectTableRowCountToBeBetween(
                min_value=expected_rows[0], max_value=expected_rows[1]
            ),
            gxe.ExpectColumnValuesToBeUnique(column="order_item_key"),
            gxe.ExpectColumnValuesToNotBeNull(column="item_price"),
            gxe.ExpectColumnValuesToBeBetween(column="item_price", min_value=0, max_value=20_000),
            gxe.ExpectColumnMeanToBeBetween(column="item_price", min_value=20, max_value=500),
            # Freight is the unit-economics canary: if shipping cost starts
            # routinely exceeding the item price, something upstream changed.
            gxe.ExpectColumnQuantileValuesToBeBetween(
                column="freight_ratio",
                quantile_ranges={
                    "quantiles": [0.5, 0.99],
                    "value_ranges": [[0.0, 1.5], [0.0, 20.0]],
                },
            ),
            # item_total = item_price + freight_value, and freight is never
            # negative, so the total can never fall below the price. Catches a
            # column-swap or sign regression in staging.
            gxe.ExpectColumnPairValuesAToBeGreaterThanB(
                column_A="item_total",
                column_B="item_price",
                or_equal=True,
            ),
            gxe.ExpectColumnValuesToNotBeNull(column="category_name"),
        ],
    )


def agg_daily_performance_suite() -> SuiteSpec:
    return SuiteSpec(
        name="agg_daily_performance",
        schema="marts",
        table="agg_daily_performance",
        columns=[
            "purchased_date",
            "customer_state",
            "category_name",
            "total_revenue",
            "orders_placed",
            "orders_late",
            "late_delivery_rate",
            "avg_review_score",
        ],
        expectations=[
            gxe.ExpectColumnValuesToNotBeNull(column="purchased_date"),
            gxe.ExpectColumnValuesToBeBetween(
                column="late_delivery_rate", min_value=0, max_value=1
            ),
            # The headline SLA number. If the mean rate hits 0 or 1, the metric
            # has broken rather than the business.
            gxe.ExpectColumnMeanToBeBetween(
                column="late_delivery_rate", min_value=0.001, max_value=0.60
            ),
            gxe.ExpectColumnValuesToBeBetween(
                column="total_revenue", min_value=0, max_value=5_000_000
            ),
            gxe.ExpectColumnValueLengthsToEqual(column="customer_state", value=2),
            gxe.ExpectColumnValuesToBeBetween(column="avg_review_score", min_value=1, max_value=5),
        ],
    )


def build_suites(scale: int = 100_000) -> list[SuiteSpec]:
    """All suites, with volume bounds scaled to the expected order count.

    Bounds are +/-40% rather than exact so a legitimately larger day does not
    fail the build, while a truncated or doubled load still does.
    """
    lo, hi = int(scale * 0.6), int(scale * 1.4)
    return [
        fct_orders_suite((lo, hi)),
        # ~1.5 line items per order in this dataset.
        fct_order_items_suite((int(lo * 1.2), int(hi * 2.0))),
        agg_daily_performance_suite(),
    ]


def to_gx_suite(spec: SuiteSpec) -> gx.ExpectationSuite:
    suite = gx.ExpectationSuite(name=spec.name)
    for expectation in spec.expectations:
        suite.add_expectation(expectation)
    return suite
