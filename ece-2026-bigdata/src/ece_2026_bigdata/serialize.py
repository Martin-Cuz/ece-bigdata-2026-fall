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
