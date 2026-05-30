"""Synthetic CUR data generator.

Produces 300 rows across 10 AWS services, 4 regions, and 3 months (Jan–Mar 2024).
Column layout matches the canonical CUR column names expected by duckdb_engine.py.
"""
import io
import random
from datetime import date, timedelta

SERVICES = [
    "Amazon EC2",
    "Amazon RDS",
    "Amazon S3",
    "AWS Lambda",
    "Amazon CloudFront",
    "Amazon DynamoDB",
    "AWS Glue",
    "Amazon Redshift",
    "Amazon EKS",
    "Amazon SQS",
]

REGIONS = ["us-east-1", "us-west-2", "eu-west-1", "ap-southeast-1"]

# Realistic per-service cost weights (EC2/RDS/Redshift skew expensive)
SERVICE_COST_RANGES: dict[str, tuple[float, float]] = {
    "Amazon EC2": (50.0, 500.0),
    "Amazon RDS": (30.0, 400.0),
    "Amazon S3": (0.5, 50.0),
    "AWS Lambda": (0.1, 20.0),
    "Amazon CloudFront": (1.0, 80.0),
    "Amazon DynamoDB": (0.5, 60.0),
    "AWS Glue": (2.0, 100.0),
    "Amazon Redshift": (40.0, 450.0),
    "Amazon EKS": (20.0, 300.0),
    "Amazon SQS": (0.1, 15.0),
}

ACCOUNT_IDS = ["123456789012", "234567890123", "345678901234"]

_HEADER = (
    "line_item_product_code,"
    "line_item_unblended_cost,"
    "line_item_usage_start_date,"
    "line_item_usage_account_id,"
    "product_region"
)


def generate_sample_cur_csv(rows: int = 300, seed: int | None = None) -> str:
    """Return a CUR CSV string with *rows* synthetic line items."""
    rng = random.Random(seed)
    start = date(2024, 1, 1)
    lines = [_HEADER]

    for _ in range(rows):
        svc = rng.choice(SERVICES)
        lo, hi = SERVICE_COST_RANGES[svc]
        cost = round(rng.uniform(lo, hi), 6)
        day = start + timedelta(days=rng.randint(0, 89))  # 3 months
        account = rng.choice(ACCOUNT_IDS)
        region = rng.choice(REGIONS)
        lines.append(f"{svc},{cost},{day},{account},{region}")

    return "\n".join(lines)
