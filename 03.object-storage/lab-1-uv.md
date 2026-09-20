---
duration: 1h
category:
  - name: LAB
components:
  - name: PYTHON
  - name: UV
platforms:
  - name: LINUX
resources:
  - title: uv (official homepage)
    url: https://docs.astral.sh/uv/
  - title: Installing uv (official documentation)
    url: https://docs.astral.sh/uv/getting-started/installation/
  - title: Faker (official homepage)
    url: https://pypi.org/project/Faker/
  - title: argparse (official homepage)
    url: https://docs.python.org/3/library/argparse.html
revisions:
  - date: 2026-09-13
    comment: Initial page
    author: david@adaltas.com
tags:
  - name: TUTORIAL
---

# Lab: Python project with uv and dataset generation

## Objectives

- Create and manage a Python project with uv
- Generate random datasets of users and orders

The datasets generated in this lab are uploaded to object storage in the next lab and transformed in the following
modules.

## Vscode initialisation

In your Onyxia portal, go to the "My Services" page and create a new `vscode-pyspark` service.

When started, login to Vscode, choose your favorite theme between dark and light and complete the bootstrap process by
clicking `Mark Done`.

## Terminal activation

In Vscode, press `F1` to print the "Show all commands" prompt. Start filtering the commands by entering `terminal` and
select `Create new Terminal (with profile)`.

## Git project

An initial work directory is created in `/home/onyxia/work`.

```bash
pwd
#> /home/onyxia/work
```

Initialize Git. The `.gitignore` file excludes the hidden files created by the platform and by Python, except the ones
which belong to the project.

```bash
git config --global init.defaultBranch main
git init
cat <<'INI' >.gitignore
.*
!.gitignore
!.python-version
__pycache__/
INI
```

Your username and email are already set. If necessary, update them accordingly.

```bash
git config --global user.name
#> gollum
git config --global user.email
#> gollum@adaltas.com
```

## UV project creation

UV is already installed inside the `vscode-pyspark` service.

```bash
command -v uv
```

The project is created using the `init` command. The `--package` argument creates a project which can be built and
installed, with its source code inside the `src` directory and command-line entry points.

```bash
uv init --package
#> Initialized project `work`
```

The following files and directories are generated:

- `.python-version`  
  Simple text file that specifies which Python version a project is using.
- `pyproject.toml`  
  Description of the project and its dependencies.
- `README.md`  
  Description and presentation of the project, empty on initialisation.
- `src/work/`  
  The `work` Python package, named after the project directory, with its `__init__.py` file. If your directory has
  another name, adapt the package name in the commands and imports of this lab.

Additional files are generated on the first dependency installation or `uv sync`.

```bash
uv sync
ls -a
#> ...
#> .venv
#> uv.lock
#> ...
```

- `.venv`  
  Directory containing the virtual environment.
- `uv.lock`  
  Lock file containing all the dependencies and their exact versions. It is committed to reproduce the same environment
  everywhere.

`pyproject.toml` includes several sections:

- `[project]`: basic information about the project and its dependencies.
- `[project.scripts]`: command-line entry points
- `[build-system]`: specify how to build/package the project

```bash
cat pyproject.toml
#> [project]
#> name = "work"
#> version = "0.1.0"
#> description = "Add your description here"
#> readme = "README.md"
#> authors = [
#>     { name = "gollum", email = "gollum@adaltas.com" }
#> ]
#> requires-python = ">=3.13"
#> dependencies = []
#>
#> [project.scripts]
#> work = "work:main"
#>
#> [build-system]
#> requires = ["uv_build>=0.12.5,<0.13.0"]
#> build-backend = "uv_build"
```

The Python and uv versions depend on your environment.

## Initial git commit

```bash
git add \
  .gitignore \
  .python-version \
  pyproject.toml \
  README.md \
  src \
  uv.lock
git commit -m "feat: initial project layout"
```

Using your GitHub account or your preferred Git provider, create a new repository, eg
`https://github.com/<owner>/ece-2026-bigdata.git`.

```bash
owner="<username>"
git remote add origin "https://github.com/$owner/ece-2026-bigdata.git"
git push -u origin main
```

The `-u` argument links your local branch to a remote branch, allowing to use the shortcut `git push` or `git pull`
without specifying the remote name and branch name in the future.

The `GitHub` Vscode extension opens a popup requesting permissions to connect to your GitHub account. The authorization
flow generates a code and redirects the user to a GitHub page where the code must be pasted.

## Readme creation

`uv init` creates an empty `README.md` file. It is updated with an introduction and a usage section.

````bash
cat <<'MD' >README.md
# Dataset generator

This project generates a random dataset consisting of users and orders. Scripts are written in Python and the project
uses [uv](https://docs.astral.sh/uv/).

## Usage

```bash
uv run dataset-users -h
#> usage: dataset-users [-h] [-c COUNT] [-o {csv,json,jsonline}]
uv run dataset-orders -h
#> usage: dataset-orders [-h] [-C COUNT_MIN] [-c COUNT_MAX] [-d DATE_FROM] [-o {csv,json,jsonline}] [-u COUNT_USERS]
```
MD
````

The readme is committed.

```bash
git add README.md
git commit -m "docs: project introduction and usages"
git push
```

## Readme best practices

A README file in the project directory is crucial for introducing the project, documenting its purpose, usage, and other
essential information to help users understand the basics.

Several good points to have are mentioned on the [TLDP
website](https://tldp.org/HOWTO/Software-Release-Practice-HOWTO/distpractice.html#readme):

1. A brief description of the project.
2. A pointer to the project website (if it has one)
3. Notes on the developer's build environment and potential portability problems.
4. An architecture introduction describing important files and subdirectories (usually
   [`ARCHITECTURE.md`](https://matklad.github.io//2021/02/06/ARCHITECTURE.md.html)).
5. Either build/installation instructions or a pointer to a file containing same (usually `INSTALL`).
6. Either a maintainers/credits list or a pointer to a file containing same (usually `CREDITS`).
7. Either recent project news or a pointer to a file containing same (usually `NEWS`).

## Serialization library script

First, a utility module used to serialize a dataset into CSV, JSON and JSON line format is created. The `serialize`
function accepts 3 formats: `csv`, `json`, and `jsonline`. `jsonline` is a format where each line contains a JSON
document. An empty format prints nothing.

```bash
cat <<'PY' >src/work/serialize.py
import csv
import io
import json


def serialize(dataset, output):
    """Print a list of dictionaries in the requested format."""
    if output == "":
        return
    elif output == "json":
        print(json.dumps(dataset, default=str))
    elif output == "jsonline":
        for record in dataset:
            print(json.dumps(record, default=str))
    elif output == "csv":
        buffer = io.StringIO()
        writer = csv.DictWriter(buffer, fieldnames=dataset[0].keys())
        writer.writeheader()
        writer.writerows(dataset)
        print(buffer.getvalue(), end="")
    else:
        raise ValueError("Unsupported output format.")
PY
```

## Project dependencies

The `serialize.py` module uses 3 modules of the Python standard library: `csv`, `io` and `json`.

The `dataset_users.py` script creates a dataset of users. It uses [Faker](https://faker.readthedocs.io/en/master/) to
randomly generate the users dataset, [argparse](https://docs.python.org/3/library/argparse.html) from the standard
library to parse CLI arguments, and the `serialize` module to print the dataset in the requested format.

Standard library modules are not installed. External dependencies are managed through `uv add` and `uv remove`. The
dependency information is recorded in `pyproject.toml` and `uv.lock` instead of being scattered across scripts.

```bash
uv add faker
# or, with an explicit version constraint
uv add "faker>=40.37.0"
# Verify
cat pyproject.toml
#> [project]
#> name = "work"
#> ...
#> dependencies = [
#>     "faker>=40.37.0",
#> ]
```

## Users generation script

The `users_generate` function creates a default of 50 users serialized as JSON. Faker is seeded to generate the same
dataset on every execution.

```bash
cat <<'PY' >src/work/dataset_users.py
import argparse

from faker import Faker

from work.serialize import serialize

fake = Faker()
# Generate the same dataset on every execution
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
PY
```

## Orders dataset generation

The `dataset_orders.py` script creates a dataset of order records linked to users via `user_uuid`. Each user places a
random number of orders, between `--count-min` and `--count-max`. Each order includes a product from a predefined list,
a quantity, and a timestamp. Orders are distributed across an hourly timeline starting from `--date-from`, January 1,
2020 by default.

```bash
cat <<'PY' >src/work/dataset_orders.py
import argparse
import datetime

from faker import Faker
from faker.providers import DynamicProvider

from work.dataset_users import users_generate
from work.serialize import serialize

fake = Faker()
# Generate the same dataset on every execution
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
            # Each order is placed in the hour following the previous one
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
PY
```

## Command-line entry points

The `[project.scripts]` section of `pyproject.toml` declares the commands provided by the project. Replace the default
`work = "work:main"` entry, which is not used, with the two generators:

```toml
[project.scripts]
dataset-users = "work.dataset_users:main"
dataset-orders = "work.dataset_orders:main"
```

## Users script execution

The commands are executed with `uv run`, which synchronizes the environment and installs the project before running
them. For example, the `-h` argument prints the command usage.

```bash
uv run dataset-users -h
#> usage: dataset-users [-h] [-c COUNT] [-o {csv,json,jsonline}]
#>
#> Users generator
#>
#> options:
#>   -h, --help            show this help message and exit
#>   -c, --count COUNT     Number of users to generate.
#>   -o, --output {csv,json,jsonline}
#>                         Output format.
uv run dataset-users -c 2 -o jsonline
#> {"uuid": "bdd640fb-0667-4ad1-9c80-317fa3b1799d", "username": "garzaanthony", "name": "Charles Garcia", ...}
#> {"uuid": "17be3111-1a2a-43ed-962b-0f79c37459ee", "username": "blairamanda", "name": "Ryan Munoz", ...}
```

## Orders script execution

```bash
uv run dataset-orders -u 2 -C 1 -c 2 -o json | jq '.[0]'
#> {
#>   "uuid": "a2bc372f-7412-4293-8729-4739614ff3d7",
#>   "user_uuid": "bdd640fb-0667-4ad1-9c80-317fa3b1799d",
#>   "date": "2020-01-01 00:50:02.797536+00:00",
#>   "quantity": 2,
#>   "product": "cookie"
#> }
```

Notice that `user_uuid` matches the `uuid` of the first user generated previously.

## Commit

Changes are committed to Git.

```bash
git add \
  pyproject.toml \
  uv.lock \
  src
git commit -m "feat: dataset generation scripts"
git push
```
