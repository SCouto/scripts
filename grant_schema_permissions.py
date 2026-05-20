# Databricks notebook source
# MAGIC %md
# MAGIC ## Schema Permission Granter
# MAGIC
# MAGIC Parameters:
# MAGIC - `schema`: fully qualified schema (e.g. `catalog.schema`)
# MAGIC - `principals`: list of dicts with `principal` and `mode` (`read` or `write`)

# COMMAND ----------

# ── CONFIG ────────────────────────────────────────────────────────────────────
schema = "dev.products_competitors_motorflash"

principals = [
    {"principal": "principal", "mode": "write"},
    {"principal": "principal", "mode": "write"},
    {"principal": "principal", "mode": "write"},
    {"principal": "principal",       "mode": "write"},
    {"principal": "principal",       "mode": "write"}
    {"principal": "principal",       "mode": "write"}

]
# ─────────────────────────────────────────────────────────────────────────────

READ_GRANTS = [
    "USE SCHEMA",
    "SELECT",
    "READ VOLUME",
]

WRITE_GRANTS = [
    "USE SCHEMA",
    "SELECT",
    "READ VOLUME",
    "MODIFY",
    "WRITE VOLUME",
    "CREATE VOLUME",
    "MANAGE",
]

def build_grants(schema: str, principal: str, mode: str) -> list[str]:
    grants = READ_GRANTS if mode == "read" else WRITE_GRANTS
    return [
        f"GRANT {priv} ON SCHEMA {schema} TO `{principal}`;"
        for priv in grants
    ]

# COMMAND ----------

all_statements = []
for entry in principals:
    principal = entry["principal"]
    mode      = entry["mode"].lower()
    if mode not in ("read", "write"):
        raise ValueError(f"Unknown mode '{mode}' for principal '{principal}'. Use 'read' or 'write'.")
    stmts = build_grants(schema, principal, mode)
    all_statements.extend(stmts)

print(f"Executing {len(all_statements)} GRANT statements on schema: {schema}\n")
for stmt in all_statements:
    print(f"  {stmt}")
    spark.sql(stmt)
    print("  ✓")

print("\nDone.")