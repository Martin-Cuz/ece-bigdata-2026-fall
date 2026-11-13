import re
import sys
from pathlib import Path

import duckdb

here = Path(__file__).parent
project = here.parents[1] / "ece-2026-bigdata"

con = duckdb.connect(str(project / "analytics.duckdb"))
con.execute((here / "init.sql").read_text())

sql = (here / "queries.sql").read_text()
statements = [s.strip() for s in re.split(r";\s*\n", sql) if s.strip()]
out = []
for stmt in statements:
    title = next((l for l in stmt.splitlines() if l.startswith("--")), "")
    body = "\n".join(l for l in stmt.splitlines() if not l.startswith("--")).strip()
    if not body:
        continue
    out.append(f"## {title.lstrip('- ').strip() or 'Query'}\n\n```sql\n{body};\n```\n")
    res = con.execute(body)
    if res.description:
        rows = res.fetchall()
        cols = [d[0] for d in res.description]
        out.append("| " + " | ".join(cols) + " |")
        out.append("|" + "---|" * len(cols))
        for r in rows[:30]:
            out.append("| " + " | ".join(str(c).replace("\n", " ") for c in r) + " |")
        out.append("")
(here / "results.md").write_text("# Lab DuckDB: execution results\n\n" + "\n".join(out), encoding="utf-8")
print("ok", len(statements))
