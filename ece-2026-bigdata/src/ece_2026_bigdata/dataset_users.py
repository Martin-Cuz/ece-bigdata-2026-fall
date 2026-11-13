import argparse

from faker import Faker

from .serialize import serialize

fake = Faker()
Faker.seed(42)


def users_generate(count=50, output=""):
    users = []
    for _ in range(count):
        user = {"uuid": fake.uuid4(), **fake.simple_profile()}
        users.append(user)
    serialize(users, output)
    return users


def main():
    parser = argparse.ArgumentParser(prog="dataset-users", description="Users generator")
    parser.add_argument(
        "-c", "--count", help="Number of users to generate.", type=int, default=50
    )
    parser.add_argument(
        "-o",
        "--output",
        help="Output format.",
        default="json",
        choices=["csv", "json", "jsonline"],
    )
    args = parser.parse_args()
    users_generate(args.count, args.output)


if __name__ == "__main__":
    main()
