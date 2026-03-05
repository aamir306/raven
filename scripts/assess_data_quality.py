#!/usr/bin/env python3
"""Assess data quality gaps in schema catalog and content awareness."""
import json

with open('data/schema_catalog.json') as f:
    catalog = json.load(f)

total = len(catalog)
empty_desc = sum(1 for t in catalog if not t.get('description') or t['description'].strip() == '')
gold = [t for t in catalog if 'gold' in t.get('table_name','').lower() or 'gold' in t.get('schema','').lower()]
gold_empty = sum(1 for t in gold if not t.get('description') or t['description'].strip() == '')
silver = [t for t in catalog if 'silver' in t.get('table_name','').lower() or 'silver' in t.get('schema','').lower()]
silver_empty = sum(1 for t in silver if not t.get('description') or t['description'].strip() == '')

cols_total = sum(len(t.get('columns',[])) for t in catalog)
cols_empty = sum(1 for t in catalog for c in t.get('columns',[]) if not c.get('description') or c['description'].strip() == '')

print("=== Schema Catalog Assessment ===")
print(f"Total tables: {total}")
print(f"Empty descriptions: {empty_desc} ({empty_desc/total*100:.1f}%)")
print(f"Gold tables: {len(gold)}, empty desc: {gold_empty} ({gold_empty/max(len(gold),1)*100:.1f}%)")
print(f"Silver tables: {len(silver)}, empty desc: {silver_empty} ({silver_empty/max(len(silver),1)*100:.1f}%)")
print(f"Total columns: {cols_total}")
print(f"Columns missing descriptions: {cols_empty} ({cols_empty/max(cols_total,1)*100:.1f}%)")

# Content Awareness
try:
    with open('data/content_awareness.json') as f:
        ca = json.load(f)
    ca_tables = len(ca)
    ca_cols = sum(len(v) for v in ca.values())
    null_stats = 0
    for tbl_cols in ca.values():
        for col_info in tbl_cols.values() if isinstance(tbl_cols, dict) else tbl_cols:
            info = col_info if isinstance(col_info, dict) else {}
            if not info.get('distinct_count') and not info.get('null_pct'):
                null_stats += 1
    print(f"\n=== Content Awareness Assessment ===")
    print(f"Tables with awareness: {ca_tables}")
    print(f"Column entries: {ca_cols}")
    print(f"Entries missing stats: {null_stats}")
except Exception as e:
    print(f"\nContent awareness: {e}")

# Sample a gold table with empty desc
for t in gold[:5]:
    if not t.get('description') or t['description'].strip() == '':
        print(f"\nSample gold table needing description:")
        print(f"  Table: {t.get('schema','')}.{t.get('table_name','')}")
        print(f"  Columns: {[c.get('column_name','') for c in t.get('columns',[])[:10]]}")
        break
