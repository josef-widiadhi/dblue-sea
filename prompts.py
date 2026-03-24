"""DB Blueprint v2 — Prompt builder with dedup hints injected."""
import json

INDUSTRY_HINTS = {
    "banking":       "acc/acct=account, cust=customer, txn=transaction, amt=amount, bal=balance, curr=currency, stat=status(A/I/C/P), gl=general ledger. Expect: accounts, customers, transactions, branches, products, currencies, audit_log.",
    "healthcare":    "pt/pat=patient, adm=admission, dx=diagnosis, rx=prescription, icd=ICD code, dob=date of birth, mrn=medical record number. Expect: patients, visits, diagnoses, prescriptions, staff, departments.",
    "logistics":     "awb=air waybill, pod=proof of delivery, wh=warehouse, sku=stock keeping unit, eta=estimated arrival. Expect: shipments, legs, waypoints, warehouses, carriers, customers.",
    "ecommerce":     "sku=stock keeping unit, qty=quantity, ord=order, inv=invoice, pmt=payment. Expect: products, orders, order_items, customers, cart, payments, inventory.",
    "manufacturing": "bom=bill of materials, wip=work in progress, wo=work order, po=purchase order, fg=finished goods. Expect: products, bom, work_orders, inventory, suppliers.",
    "telco":         "msisdn=mobile number, cdr=call detail record, apn=access point name, subs=subscriber. Expect: subscribers, plans, cdrs, invoices, devices.",
    "insurance":     "pol=policy, prm=premium, clm=claim, ben=beneficiary, cov=coverage. Expect: policies, claims, customers, agents, premiums.",
    "other":         "Generic system — infer domain from names and sample data.",
}


def build_analysis_prompt(schema: dict, industry: str, subdomain: str,
                           region: str, hints: list[str], dedup_results: dict = None) -> str:
    tables_payload = [
        {"name": n, "row_count": t.get("row_count", 0),
         "columns": t.get("columns", []), "sample_rows": t.get("samples", [])[:3]}
        for n, t in schema.get("tables", {}).items()
    ]

    dedup_section = ""
    if dedup_results:
        inferred = dedup_results.get("inferred_fks", [])
        similar  = dedup_results.get("similar_tables", [])
        if inferred or similar:
            dedup_section = f"""
=== PRE-COMPUTED SIMILARITY HINTS (high confidence — use these first) ===
Inferred FK candidates (name similarity + value overlap):
{json.dumps(inferred[:30], indent=2)}

Similar table pairs (possible duplicates/aliases):
{json.dumps(similar[:10], indent=2)}
"""

    return f"""You are a senior database architect performing reverse engineering.

=== DOMAIN CONTEXT ===
Industry: {industry or 'unknown'} — {INDUSTRY_HINTS.get(industry or 'other', INDUSTRY_HINTS['other'])}
Sub-domain: {subdomain or 'not specified'}
Region: {region or 'not specified'}
DB: {schema.get('db_type','?')} / schema: {schema.get('schema_name','?')}
User hints: {', '.join(hints) if hints else 'none'}
{dedup_section}
=== SCHEMA ({len(tables_payload)} tables) ===
{json.dumps(tables_payload, indent=2, default=str)}

=== EXPLICIT FKs ===
{json.dumps(schema.get('explicit_fks', []), indent=2)}

Return ONLY valid JSON (no markdown fences, no preamble):
{{
  "domain_detected": "string",
  "domain_confidence": "high|medium|low",
  "executive_summary": "2-3 sentences",
  "tables": [{{
    "name": "...", "description": "...",
    "purpose": "core|lookup|junction|audit|config|staging",
    "estimated_importance": "high|medium|low",
    "columns": [{{"name":"...","description":"...","likely_fk_to":"table.col or null","notes":"..."}}]
  }}],
  "relations": [{{
    "from_table":"...","from_col":"...","to_table":"...","to_col":"...",
    "type":"many-to-one|one-to-many|one-to-one|many-to-many",
    "confidence":"high|medium|low","explicit_fk":false,"reason":"..."
  }}],
  "missing_tables": [{{"name":"...","reason":"..."}}],
  "design_observations": ["..."],
  "mermaid_erd": "complete erDiagram code"
}}"""
