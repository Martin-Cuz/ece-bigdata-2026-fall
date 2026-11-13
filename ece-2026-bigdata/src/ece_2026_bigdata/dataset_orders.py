import argparse
import datetime

from faker import Faker
from faker.providers import DynamicProvider

from .dataset_users import users_generate
from .serialize import serialize

fake = Faker()
Faker.seed(42)
fake.add_provider(
    DynamicProvider(
        provider_name="product",
        elements=["bread", "brioche", "cookie", "croissant", "donut", "drink"],
    )
)


def orders_generate(count_min=0, count_max=100, count_users=50, date_from=None, output=""):
    orders = []
    date_start = date_from or datetime.datetime(2020, 1, 1, tzinfo=datetime.UTC)
    for user in users_generate(count_users):
        for _ in range(fake.pyint(min_value=count_min, max_value=count_max)):
            orders.append(
                {
                    "uuid": fake.uuid4(),
                    "user_uuid": user["uuid"],
                    "date": fake.date_time_between(
                        date_start, date_start + datetime.timedelta(hours=1), tzinfo=datetime.UTC
                    ),
                    "quantity": fake.pyint(min_value=1, max_value=5),
                    "product": fake.product(),
                }
            )
            date_start += datetime.timedelta(hours=1)
    serialize(orders, output)
    return orders


def main():
    parser = argparse.ArgumentParser(prog="dataset-orders", description="Orders generator")
    parser.add_argument(
        "-C",
        "--count-min",
        help="Minimum number of orders to generate per user.",
        type=int,
        default=0,
    )
    parser.add_argument(
        "-c",
        "--count-max",
        help="Maximum number of orders to generate per user.",
        type=int,
        default=100,
    )
    parser.add_argument(
        "-d",
        "--date-from",
        help="Date of the first order, in ISO format (default: 2020-01-01).",
        type=lambda value: datetime.datetime.fromisoformat(value).replace(tzinfo=datetime.UTC),
    )
    parser.add_argument(
        "-o",
        "--output",
        help="Output format.",
        default="json",
        choices=["csv", "json", "jsonline"],
    )
    parser.add_argument(
        "-u", "--count-users", help="Number of users to generate.", type=int, default=50
    )
    args = parser.parse_args()
    orders_generate(args.count_min, args.count_max, args.count_users, args.date_from, args.output)


if __name__ == "__main__":
    main()
