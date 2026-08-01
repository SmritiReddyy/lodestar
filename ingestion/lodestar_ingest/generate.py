"""Deterministic generator for Olist-shaped source data.

The real Olist export lives behind a Kaggle login, which makes a clone-and-run
portfolio repo awkward. This module emits CSVs with byte-identical column
names and realistic value distributions, so every downstream stage — landing
zone, warehouse, dbt, Great Expectations — behaves exactly as it would on the
genuine export. Drop the real CSVs into `data/source/` and the generator is
skipped automatically.

The messiness is deliberate and mirrors quirks in the real dataset:
  * `order_approved_at` is null for a small share of orders
  * some orders are flagged `delivered` yet never got a delivery timestamp
  * `geolocation` contains many rows per zip prefix
  * a slice of products has no category name
  * estimated delivery dates are missed often enough to make an SLA metric move
"""

from __future__ import annotations

import csv
import random
from datetime import datetime, timedelta
from pathlib import Path

from .sources import SOURCE_TABLES, SourceTable

# --- Reference data -------------------------------------------------------

# (state, weight) — roughly the real geographic skew, SP dominates.
STATES: tuple[tuple[str, int], ...] = (
    ("SP", 42),
    ("RJ", 13),
    ("MG", 12),
    ("RS", 5),
    ("PR", 5),
    ("SC", 4),
    ("BA", 3),
    ("DF", 2),
    ("GO", 2),
    ("ES", 2),
    ("PE", 2),
    ("CE", 2),
    ("PA", 1),
    ("MT", 1),
    ("MA", 1),
    ("MS", 1),
    ("PB", 1),
    ("RN", 1),
    ("PI", 1),
    ("AL", 1),
)

CITIES_BY_STATE: dict[str, tuple[str, ...]] = {
    "SP": ("sao paulo", "campinas", "guarulhos", "santo andre", "osasco", "sorocaba"),
    "RJ": ("rio de janeiro", "niteroi", "nova iguacu", "campos dos goytacazes"),
    "MG": ("belo horizonte", "uberlandia", "contagem", "juiz de fora"),
    "RS": ("porto alegre", "caxias do sul", "pelotas"),
    "PR": ("curitiba", "londrina", "maringa"),
    "SC": ("florianopolis", "joinville", "blumenau"),
    "BA": ("salvador", "feira de santana"),
    "DF": ("brasilia",),
    "GO": ("goiania", "aparecida de goiania"),
    "ES": ("vitoria", "vila velha"),
    "PE": ("recife", "jaboatao dos guararapes"),
    "CE": ("fortaleza", "caucaia"),
    "PA": ("belem", "ananindeua"),
    "MT": ("cuiaba", "varzea grande"),
    "MA": ("sao luis", "imperatriz"),
    "MS": ("campo grande", "dourados"),
    "PB": ("joao pessoa", "campina grande"),
    "RN": ("natal", "mossoro"),
    "PI": ("teresina", "parnaiba"),
    "AL": ("maceio", "arapiraca"),
}

# Approximate state centroids, used to scatter plausible lat/lng points.
STATE_CENTROIDS: dict[str, tuple[float, float]] = {
    "SP": (-23.55, -46.63),
    "RJ": (-22.91, -43.17),
    "MG": (-19.92, -43.94),
    "RS": (-30.03, -51.23),
    "PR": (-25.43, -49.27),
    "SC": (-27.59, -48.55),
    "BA": (-12.97, -38.51),
    "DF": (-15.79, -47.88),
    "GO": (-16.68, -49.25),
    "ES": (-20.32, -40.34),
    "PE": (-8.05, -34.88),
    "CE": (-3.73, -38.53),
    "PA": (-1.46, -48.49),
    "MT": (-15.60, -56.10),
    "MA": (-2.53, -44.30),
    "MS": (-20.44, -54.65),
    "PB": (-7.12, -34.88),
    "RN": (-5.79, -35.21),
    "PI": (-5.09, -42.80),
    "AL": (-9.67, -35.74),
}

# (portuguese, english, weight, price_low, price_high)
CATEGORIES: tuple[tuple[str, str, int, float, float], ...] = (
    ("cama_mesa_banho", "bed_bath_table", 12, 20.0, 260.0),
    ("beleza_saude", "health_beauty", 11, 15.0, 320.0),
    ("esporte_lazer", "sports_leisure", 10, 25.0, 480.0),
    ("informatica_acessorios", "computers_accessories", 9, 30.0, 900.0),
    ("moveis_decoracao", "furniture_decor", 9, 35.0, 750.0),
    ("utilidades_domesticas", "housewares", 8, 18.0, 300.0),
    ("relogios_presentes", "watches_gifts", 7, 45.0, 1100.0),
    ("telefonia", "telephony", 6, 12.0, 420.0),
    ("automotivo", "auto", 5, 22.0, 640.0),
    ("brinquedos", "toys", 5, 15.0, 380.0),
    ("cool_stuff", "cool_stuff", 4, 30.0, 700.0),
    ("ferramentas_jardim", "garden_tools", 4, 20.0, 560.0),
    ("perfumaria", "perfumery", 3, 25.0, 400.0),
    ("bebes", "baby", 3, 20.0, 450.0),
    ("eletronicos", "electronics", 2, 40.0, 1500.0),
    ("papelaria", "stationery", 2, 8.0, 160.0),
)

PAYMENT_TYPES: tuple[tuple[str, int], ...] = (
    ("credit_card", 74),
    ("boleto", 19),
    ("voucher", 5),
    ("debit_card", 2),
)

# Terminal order statuses and their share of the population.
ORDER_STATUSES: tuple[tuple[str, int], ...] = (
    ("delivered", 92),
    ("shipped", 3),
    ("canceled", 2),
    ("unavailable", 1),
    ("invoiced", 1),
    ("processing", 1),
)

REVIEW_TITLES = (
    "",
    "",
    "",
    "recomendo",
    "muito bom",
    "produto otimo",
    "nao recomendo",
    "chegou antes do prazo",
    "atrasou",
)

_HEX = "0123456789abcdef"


class OlistGenerator:
    """Emits the nine Olist CSVs into a target directory."""

    def __init__(self, n_orders: int = 20_000, seed: int = 20240501) -> None:
        if n_orders < 100:
            raise ValueError("n_orders must be at least 100 to keep joins meaningful")
        self.n_orders = n_orders
        self.rng = random.Random(seed)
        # Scale the supporting entities off the order count, keeping the real
        # dataset's rough ratios (≈1 seller per 32 orders, 1 product per 3).
        self.n_sellers = max(20, n_orders // 32)
        self.n_products = max(200, n_orders // 3)
        self.n_unique_customers = max(100, int(n_orders * 0.94))
        self.start = datetime(2023, 1, 1)
        self.end = datetime(2024, 12, 31)

    # --- primitives -------------------------------------------------------

    def _hex_id(self) -> str:
        """32-char hex key, same shape as the real Olist surrogate keys."""
        return "".join(self.rng.choice(_HEX) for _ in range(32))

    def _weighted(self, pairs):
        population = [p[0] for p in pairs]
        weights = [p[1] for p in pairs]
        return self.rng.choices(population, weights=weights, k=1)[0]

    def _state(self) -> str:
        return self._weighted(STATES)

    def _zip_for_state(self, state: str) -> str:
        # Stable per-state prefix band so zip → state stays consistent.
        base = 1000 + (sum(ord(c) for c in state) % 9) * 8000
        return f"{base + self.rng.randrange(0, 900):05d}"

    def _ts(self, dt: datetime) -> str:
        return dt.strftime("%Y-%m-%d %H:%M:%S")

    # --- entity builders --------------------------------------------------

    def _build_geolocation(self, zips: dict[str, str]) -> list[dict]:
        rows: list[dict] = []
        for zip_prefix, state in zips.items():
            lat0, lng0 = STATE_CENTROIDS[state]
            # Several samples per prefix — the real table is intentionally
            # non-unique, which is what makes the staging dedup worth writing.
            for _ in range(self.rng.randint(1, 6)):
                rows.append(
                    {
                        "geolocation_zip_code_prefix": zip_prefix,
                        "geolocation_lat": round(lat0 + self.rng.gauss(0, 0.35), 6),
                        "geolocation_lng": round(lng0 + self.rng.gauss(0, 0.35), 6),
                        "geolocation_city": self.rng.choice(CITIES_BY_STATE[state]),
                        "geolocation_state": state,
                    }
                )
        self.rng.shuffle(rows)
        return rows

    def _build_products(self) -> list[dict]:
        rows = []
        for _ in range(self.n_products):
            cat = self._weighted([(c[0], c[2]) for c in CATEGORIES])
            # ~1.5% of products have no category, as in the real export.
            has_category = self.rng.random() > 0.015
            rows.append(
                {
                    "product_id": self._hex_id(),
                    "product_category_name": cat if has_category else "",
                    "product_name_lenght": self.rng.randint(20, 76) if has_category else "",
                    "product_description_lenght": (
                        self.rng.randint(100, 3800) if has_category else ""
                    ),
                    "product_photos_qty": self.rng.randint(1, 6) if has_category else "",
                    "product_weight_g": round(self.rng.lognormvariate(6.4, 1.1), 1),
                    "product_length_cm": round(self.rng.uniform(8, 90), 1),
                    "product_height_cm": round(self.rng.uniform(3, 60), 1),
                    "product_width_cm": round(self.rng.uniform(6, 70), 1),
                }
            )
        return rows

    def _price_for(self, category: str) -> float:
        for pt, _en, _w, lo, hi in CATEGORIES:
            if pt == category:
                # Log-uniform: most orders cheap, a long tail of expensive ones.
                span = hi / lo
                return round(lo * (span ** self.rng.random()), 2)
        return round(self.rng.uniform(20, 300), 2)

    def _order_timeline(self, purchase: datetime, status: str) -> dict:
        """Build the five-timestamp delivery chain for one order."""
        out = {
            "order_purchase_timestamp": self._ts(purchase),
            "order_approved_at": "",
            "order_delivered_carrier_date": "",
            "order_delivered_customer_date": "",
            # Promise is set at purchase time and is what the SLA is measured
            # against. This window is tuned so the resulting late rate lands
            # near the ~8% seen in the real Olist data — a metric that is
            # neither trivially zero nor implausibly bad.
            "order_estimated_delivery_date": self._ts(
                purchase + timedelta(days=self.rng.randint(14, 42))
            ),
        }
        if status in {"unavailable", "canceled"} and self.rng.random() < 0.6:
            return out  # never approved

        # ~0.3% of approved orders lose their approval timestamp upstream.
        approved = purchase + timedelta(minutes=self.rng.randint(8, 2880))
        if self.rng.random() > 0.003:
            out["order_approved_at"] = self._ts(approved)

        if status in {"processing", "invoiced", "canceled", "unavailable"}:
            return out

        carrier = approved + timedelta(hours=self.rng.randint(6, 220))
        out["order_delivered_carrier_date"] = self._ts(carrier)
        if status == "shipped":
            return out

        # status == "delivered": ~0.1% are delivered with no customer timestamp,
        # which is a real quirk and a good singular-test target.
        if self.rng.random() < 0.001:
            return out
        transit_days = max(1, int(self.rng.lognormvariate(1.9, 0.62)))
        out["order_delivered_customer_date"] = self._ts(
            carrier + timedelta(days=transit_days, hours=self.rng.randint(0, 23))
        )
        return out

    # --- driver -----------------------------------------------------------

    def generate(self) -> dict[str, list[dict]]:
        rng = self.rng

        sellers, seller_zips = [], {}
        for _ in range(self.n_sellers):
            state = self._state()
            zip_prefix = self._zip_for_state(state)
            seller_zips[zip_prefix] = state
            sellers.append(
                {
                    "seller_id": self._hex_id(),
                    "seller_zip_code_prefix": zip_prefix,
                    "seller_city": rng.choice(CITIES_BY_STATE[state]),
                    "seller_state": state,
                }
            )

        products = self._build_products()
        unique_customer_ids = [self._hex_id() for _ in range(self.n_unique_customers)]

        customers, orders, order_items, payments, reviews = [], [], [], [], []
        customer_zips: dict[str, str] = {}

        for _ in range(self.n_orders):
            state = self._state()
            zip_prefix = self._zip_for_state(state)
            customer_zips[zip_prefix] = state

            # In Olist, customer_id is unique per order; customer_unique_id
            # is the stable person key that repeats across repeat purchases.
            customer_id = self._hex_id()
            customers.append(
                {
                    "customer_id": customer_id,
                    "customer_unique_id": rng.choice(unique_customer_ids),
                    "customer_zip_code_prefix": zip_prefix,
                    "customer_city": rng.choice(CITIES_BY_STATE[state]),
                    "customer_state": state,
                }
            )

            order_id = self._hex_id()
            status = self._weighted(ORDER_STATUSES)
            span = int((self.end - self.start).total_seconds())
            purchase = self.start + timedelta(seconds=rng.randrange(span))
            order = {"order_id": order_id, "customer_id": customer_id, "order_status": status}
            order.update(self._order_timeline(purchase, status))
            orders.append(order)

            # Line items: mostly single-item baskets, occasionally larger.
            n_items = rng.choices([1, 2, 3, 4, 5], weights=[70, 17, 7, 4, 2], k=1)[0]
            order_total = 0.0
            for item_no in range(1, n_items + 1):
                product = rng.choice(products)
                seller = rng.choice(sellers)
                price = self._price_for(product["product_category_name"] or "cool_stuff")
                freight = round(max(0.0, rng.gauss(19.0, 8.0)), 2)
                order_total += price + freight
                order_items.append(
                    {
                        "order_id": order_id,
                        "order_item_id": item_no,
                        "product_id": product["product_id"],
                        "seller_id": seller["seller_id"],
                        "shipping_limit_date": self._ts(
                            purchase + timedelta(days=rng.randint(2, 12))
                        ),
                        "price": price,
                        "freight_value": freight,
                    }
                )

            # Payments: usually one row, sometimes split across vouchers.
            if status == "unavailable" and rng.random() < 0.3:
                pass  # some unavailable orders never generate a payment row
            else:
                n_pay = rng.choices([1, 2, 3], weights=[92, 6, 2], k=1)[0]
                remaining = round(order_total, 2)
                for seq in range(1, n_pay + 1):
                    last = seq == n_pay
                    ptype = self._weighted(PAYMENT_TYPES)
                    value = remaining if last else round(remaining * rng.uniform(0.2, 0.6), 2)
                    remaining = round(remaining - value, 2)
                    payments.append(
                        {
                            "order_id": order_id,
                            "payment_sequential": seq,
                            "payment_type": ptype,
                            "payment_installments": (
                                rng.choices(
                                    [1, 2, 3, 4, 6, 10],
                                    weights=[52, 12, 12, 10, 9, 5],
                                    k=1,
                                )[0]
                                if ptype == "credit_card"
                                else 1
                            ),
                            "payment_value": max(0.0, round(value, 2)),
                        }
                    )

            # Reviews land on ~71% of orders, skewed positive.
            if rng.random() < 0.71:
                delivered = order["order_delivered_customer_date"]
                base = (
                    datetime.strptime(delivered, "%Y-%m-%d %H:%M:%S")
                    if delivered
                    else purchase + timedelta(days=rng.randint(5, 30))
                )
                created = base + timedelta(days=rng.randint(0, 3))
                score = rng.choices([5, 4, 3, 2, 1], weights=[57, 19, 8, 3, 13], k=1)[0]
                title = rng.choice(REVIEW_TITLES)
                reviews.append(
                    {
                        "review_id": self._hex_id(),
                        "order_id": order_id,
                        "review_score": score,
                        "review_comment_title": title,
                        "review_comment_message": (
                            f"score {score} apos {rng.randint(1, 30)} dias" if title else ""
                        ),
                        "review_creation_date": self._ts(created),
                        "review_answer_timestamp": self._ts(
                            created + timedelta(hours=rng.randint(2, 240))
                        ),
                    }
                )

        all_zips = {**seller_zips, **customer_zips}
        return {
            "customers": customers,
            "geolocation": self._build_geolocation(all_zips),
            "order_items": order_items,
            "order_payments": payments,
            "order_reviews": reviews,
            "orders": orders,
            "products": products,
            "sellers": sellers,
            "product_category_translation": [
                {"product_category_name": pt, "product_category_name_english": en}
                for pt, en, *_ in CATEGORIES
            ],
        }

    def write_csvs(self, target_dir: Path) -> dict[str, int]:
        """Write every table to `target_dir`, returning row counts per table."""
        target_dir.mkdir(parents=True, exist_ok=True)
        data = self.generate()
        counts: dict[str, int] = {}
        for table in SOURCE_TABLES:
            rows = data[table.name]
            path = target_dir / table.filename
            _write_csv(path, table, rows)
            counts[table.name] = len(rows)
        return counts


def _write_csv(path: Path, table: SourceTable, rows: list[dict]) -> None:
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=table.column_names)
        writer.writeheader()
        writer.writerows(rows)


def ensure_source_data(source_dir: Path, n_orders: int, seed: int) -> bool:
    """Generate synthetic CSVs unless a complete source export already exists.

    Returns True if data was generated, False if real files were found.
    """
    missing = [t.filename for t in SOURCE_TABLES if not (source_dir / t.filename).exists()]
    if not missing:
        return False
    OlistGenerator(n_orders=n_orders, seed=seed).write_csvs(source_dir)
    return True


def apply_seller_drift(source_dir: Path, fraction: float = 0.05, seed: int | None = None) -> int:
    """Relocate a sample of sellers in place, to exercise Type 2 history.

    The Olist export is a static snapshot, so nothing in it ever changes and an
    SCD Type 2 dimension would have exactly one version per key forever. This
    mutates the sellers file the way a real source system would — same keys,
    different city/state — so re-running the pipeline and `dbt snapshot`
    produces genuine second versions with real `valid_from` / `valid_to`
    boundaries. Returns the number of sellers moved.
    """
    table = next(t for t in SOURCE_TABLES if t.name == "sellers")
    path = source_dir / table.filename
    if not path.exists():
        raise FileNotFoundError(f"No sellers file to drift at {path}")

    with path.open(newline="", encoding="utf-8") as fh:
        rows = list(csv.DictReader(fh))
    if not rows:
        return 0

    rng = random.Random(seed)
    n_to_move = max(1, int(len(rows) * fraction))
    for row in rng.sample(rows, n_to_move):
        current = row["seller_state"]
        new_state = rng.choice([s for s, _ in STATES if s != current])
        row["seller_state"] = new_state
        row["seller_city"] = rng.choice(CITIES_BY_STATE[new_state])
        # Keep zip consistent with the new state, mirroring the generator's rule.
        base = 1000 + (sum(ord(c) for c in new_state) % 9) * 8000
        row["seller_zip_code_prefix"] = f"{base + rng.randrange(0, 900):05d}"

    _write_csv(path, table, rows)
    return n_to_move
