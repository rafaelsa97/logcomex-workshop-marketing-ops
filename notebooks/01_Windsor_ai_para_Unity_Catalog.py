# Databricks notebook source
# MAGIC %md
# MAGIC # 01 · Windsor.ai → Unity Catalog, direto (sem banco intermediário)
# MAGIC
# MAGIC O Windsor.ai tem um **destino nativo Databricks**: ele se conecta a um **SQL Warehouse** e grava direto em
# MAGIC `catalogo.schema.tabela` do Unity Catalog. Neste lab nós **simulamos** exatamente essa mecânica com dados fictícios:
# MAGIC
# MAGIC 1. conexão no **SQL Warehouse** (o "Server hostname + HTTP path" que o Windsor pede);
# MAGIC 2. `CREATE TABLE IF NOT EXISTS` das tabelas de destino;
# MAGIC 3. `MERGE` de uma **janela móvel** de dias (as plataformas revisam conversões retroativamente; o `MERGE` torna a carga idempotente).
# MAGIC
# MAGIC | Conector Windsor.ai | Tabela UC |
# MAGIC |---|---|
# MAGIC | Google Ads | `windsor_google_ads` |
# MAGIC | Meta Ads | `windsor_facebook_ads` |
# MAGIC | TikTok Ads | `windsor_tiktok_ads` |
# MAGIC | ChatGPT (OpenAI) Ads | `windsor_chatgpt_ads` |
# MAGIC | Google Analytics 4 | `windsor_ga4` |
# MAGIC | Google Search Console | `windsor_searchconsole` |
# MAGIC
# MAGIC > ⚠️ Nada aqui chama Windsor.ai, Google, Meta, TikTok ou OpenAI. Os números vêm de um gerador determinístico (`lib/midia_ficticia.py`).
# MAGIC
# MAGIC <details><summary><b>Como fica em produção (leitura)</b></summary>
# MAGIC
# MAGIC 1. Um admin cria um **service principal** dedicado (ex.: `sp-windsor-ai`) e um *OAuth secret* para ele.
# MAGIC 2. Concede o mínimo: `USE CATALOG`, `USE SCHEMA` + `CREATE TABLE` no schema de destino e `CAN USE` no warehouse.
# MAGIC 3. No Windsor.ai: *Destination → Databricks*, informa hostname, HTTP path, catálogo, schema e as credenciais do SP.
# MAGIC 4. O owner das tabelas passa a ser o SP do Windsor, e cada carga vira uma versão Delta (auditoria e *time travel*).
# MAGIC
# MAGIC *Neste workshop não criamos service principals: a carga roda com a sua própria identidade, dentro do seu schema.*
# MAGIC </details>

# COMMAND ----------

dbutils.widgets.text("dias", "180", "Janela de reprocessamento (dias)")
DIAS = int(dbutils.widgets.get("dias"))

# COMMAND ----------

from lib import config, uc
from lib.midia_ficticia import (CAMPANHAS, TABELA_WINDSOR, GA4_FONTES, LANDING_PAGES, GSC_QUERIES,
                                UTM_SOURCE, UTM_MEDIUM, janela, hoje, metricas_campanha, rng_for)

cfg = config.carregar_config(spark)
sql = uc.ExecutorSQL(spark, cfg["warehouse_id"])
print("Destino:", cfg["fq"], "| execução via", sql.via)

# COMMAND ----------

# MAGIC %md ## 1) Tabelas de destino (campos no padrão Windsor.ai)

# COMMAND ----------

ADS = """date DATE, datasource STRING, account_name STRING, campaign_id STRING, campaign STRING, ad_group STRING,
  impressions BIGINT, clicks BIGINT, spend DOUBLE, conversions BIGINT, ctr DOUBLE, cpc DOUBLE, currency STRING,
  _windsor_synced_at TIMESTAMP"""
GA4 = """date DATE, datasource STRING, session_source_medium STRING, landing_page STRING, sessions BIGINT,
  engaged_sessions BIGINT, new_users BIGINT, key_events BIGINT, _windsor_synced_at TIMESTAMP"""
GSC = """date DATE, datasource STRING, query STRING, page STRING, clicks BIGINT, impressions BIGINT, ctr DOUBLE,
  position DOUBLE, _windsor_synced_at TIMESTAMP"""

destinos = [(tb, ADS) for tb in TABELA_WINDSOR.values()] + [("windsor_ga4", GA4), ("windsor_searchconsole", GSC)]
for tabela, cols in destinos:
    sql(f"CREATE TABLE IF NOT EXISTS {uc.t(cfg, tabela)} ({cols}) "
        "COMMENT 'FICTICIO - gravada pela destination task Databricks do Windsor.ai (simulada)' "
        "TBLPROPERTIES ('lcx_workshop' = 'true', 'dados' = 'ficticios', 'origem' = 'windsor.ai (simulado)')")
print("Tabelas de destino OK")

# COMMAND ----------

# MAGIC %md ## 2) "Coleta" das plataformas e gravação idempotente com `MERGE`

# COMMAND ----------

def lit(v):
    if v is None:
        return "NULL"
    if isinstance(v, (int, float)):
        return repr(v)
    return "'" + str(v).replace("\\", "\\\\").replace("'", "\\'") + "'"


def merge(tabela, linhas, chaves, bloco=500):
    if not linhas:
        return 0
    cols = list(linhas[0].keys())
    for i in range(0, len(linhas), bloco):
        values = ",\n".join("(" + ", ".join(lit(r[c]) for c in cols) + ")" for r in linhas[i:i + bloco])
        sel = ", ".join(f"CAST(s.{c} AS DATE) AS {c}" if c == "date" else f"s.{c}" for c in cols)
        sets = ", ".join(f"t.{c} = s.{c}" for c in cols if c not in chaves)
        sql(f"""MERGE INTO {uc.t(cfg, tabela)} t
USING (SELECT {sel}, current_timestamp() AS _windsor_synced_at FROM VALUES {values} AS s({', '.join(cols)})) s
ON {' AND '.join(f't.{k} = s.{k}' for k in chaves)}
WHEN MATCHED THEN UPDATE SET {sets}, t._windsor_synced_at = s._windsor_synced_at
WHEN NOT MATCHED THEN INSERT *""")
    return len(linhas)


datas = janela(DIAS)
resumo = {}
for ds, tabela in TABELA_WINDSOR.items():
    linhas = [m for d in datas for c in CAMPANHAS if c[0] == ds for m in [metricas_campanha(d, c)] if m]
    resumo[tabela] = merge(tabela, linhas, ["date", "campaign_id"])
    print(f"  {tabela:24s} {resumo[tabela]:>6,} linhas")

ga4 = []
for d in datas:
    r = rng_for("ga4", d.isoformat())
    dias_atras = (hoje() - d).days
    fds = 0.5 if d.weekday() >= 5 else 1.0
    pagos = {}
    for c in CAMPANHAS:
        m = metricas_campanha(d, c)
        if m:
            sm = f"{UTM_SOURCE[c[0]]} / {UTM_MEDIUM[c[0]]}"
            s, k = pagos.get(sm, (0, 0))
            pagos[sm] = (s + int(m["clicks"] * 0.86), k + m["conversions"])
    fontes = [(sm, s, k / max(s, 1)) for sm, (s, k) in pagos.items()]
    for sm, base, kr in GA4_FONTES:
        mult = 1 + 3.2 * (1 - dias_atras / 180) if sm.startswith("chatgpt.com") else 1.0
        fontes.append((sm, int(base * mult * fds * r.uniform(0.85, 1.15)), kr))
    for sm, sessoes, kr in fontes:
        resto = sessoes
        for j, lp in enumerate(LANDING_PAGES):
            s = resto if j == len(LANDING_PAGES) - 1 else max(0, min(int(sessoes * r.uniform(0.05, 0.25)), resto))
            resto -= s
            if s:
                ga4.append({"date": d.isoformat(), "datasource": "googleanalytics4", "session_source_medium": sm,
                            "landing_page": lp, "sessions": s, "engaged_sessions": int(s * r.uniform(0.45, 0.7)),
                            "new_users": int(s * r.uniform(0.55, 0.8)),
                            "key_events": int(round(s * kr * r.uniform(0.8, 1.2)))})
print(f"  {'windsor_ga4':24s} {merge('windsor_ga4', ga4, ['date', 'session_source_medium', 'landing_page']):>6,} linhas")

gsc = []
for d in datas:
    for q, page, imp, pos in GSC_QUERIES:
        r = rng_for("gsc", q, d.isoformat())
        impressions = int(imp / 30 * r.uniform(0.7, 1.3) * (0.6 if d.weekday() >= 5 else 1))
        position = round(max(1.0, pos * r.uniform(0.85, 1.15)), 1)
        clicks = int(impressions * max(0.004, 0.32 / position ** 0.9) * r.uniform(0.8, 1.2))
        gsc.append({"date": d.isoformat(), "datasource": "searchconsole", "query": q, "page": page, "clicks": clicks,
                    "impressions": impressions, "ctr": round(clicks / impressions, 4) if impressions else 0.0,
                    "position": position})
print(f"  {'windsor_searchconsole':24s} {merge('windsor_searchconsole', gsc, ['date', 'query', 'page']):>6,} linhas")

# COMMAND ----------

# MAGIC %md ## 3) Visão unificada de mídia
# MAGIC Todas as plataformas no mesmo formato, base para CAC, dashboards e Genie.

# COMMAND ----------

uniao = "\nUNION ALL\n".join(
    f"SELECT date, datasource, account_name, campaign_id, campaign, ad_group, impressions, clicks, spend, "
    f"conversions FROM {uc.t(cfg, tb)}" for tb in TABELA_WINDSOR.values())
spark.sql(f"CREATE OR REPLACE VIEW {uc.t(cfg, 'vw_midia_unificada')} "
          f"COMMENT 'FICTICIO - todas as plataformas de midia' AS {uniao}")
display(spark.sql(f"""SELECT datasource, round(sum(spend), 2) investimento_brl, sum(clicks) cliques,
  sum(conversions) leads, round(sum(spend) / nullif(sum(conversions), 0), 2) cpl_brl
FROM {uc.t(cfg, 'vw_midia_unificada')} GROUP BY 1 ORDER BY investimento_brl DESC"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 🔎 Explore
# MAGIC * **Catalog Explorer** → seu schema → `windsor_google_ads` → abas **History** (cada carga é uma versão) e **Lineage**.
# MAGIC * Mude o widget `dias` para `3` e rode o notebook de novo: é o que o Windsor faria a cada sincronização. O total de linhas não duplica (`MERGE`).
# MAGIC
# MAGIC ✅ Siga para o **02_ETL_HubSpot_para_Unity_Catalog**.
