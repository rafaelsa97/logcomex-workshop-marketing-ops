# Databricks notebook source
# MAGIC %md
# MAGIC # 02 · ETL HubSpot → Unity Catalog (ingestão incremental)
# MAGIC
# MAGIC Primeira metade do ciclo **HubSpot → Databricks → HubSpot**: trazer para o UC o que o time comercial mantém no CRM
# MAGIC (owner, lifecycle, negócios abertos, valor em pipeline) para cruzar com os dados de produto.
# MAGIC
# MAGIC **Padrão incremental:** guardamos a *marca d'água* (maior `hs_lastmodifieddate` já ingerido) e, a cada execução,
# MAGIC pedimos ao HubSpot só o que mudou desde então (`POST /crm/v3/objects/companies/search`), gravando com `MERGE` na
# MAGIC tabela `bronze_hubspot_companies`.
# MAGIC
# MAGIC > 🔒 O HubSpot aqui é **fictício** e roda **em memória** dentro do notebook, sem rede. O cliente HTTP (`lib/hubspot_client.py`)
# MAGIC > recusa qualquer outro endereço.
# MAGIC
# MAGIC <details><summary><b>Alternativas em produção (leitura)</b></summary>
# MAGIC
# MAGIC | Opção | Observações |
# MAGIC |---|---|
# MAGIC | **Lakeflow Connect, conector HubSpot** | Gerenciado, incremental, serverless, com lineage. Verifique na documentação quais objetos estão disponíveis (os de CRM estão em Beta). |
# MAGIC | **Notebook + API** (este lab) | Total controle, mesmo padrão de marca d'água. Em produção, o token fica em `dbutils.secrets`. |
# MAGIC | **Windsor.ai** | Se o conector HubSpot dele atender, entra pelo mesmo caminho do lab 01. |
# MAGIC </details>

# COMMAND ----------

dbutils.widgets.dropdown("reprocessar_tudo", "false", ["false", "true"], "Ignorar marca d'água")
REPROCESSAR = dbutils.widgets.get("reprocessar_tudo") == "true"

# COMMAND ----------

import datetime as dt
from lib import config, uc
from lib.hubspot_client import HubSpot
from lib.mock_hubspot import MockHubSpot

cfg = config.carregar_config(spark)
BRONZE = uc.t(cfg, "bronze_hubspot_companies")

spark.sql(f"""CREATE TABLE IF NOT EXISTS {BRONZE} (
  hs_object_id STRING, logcomex_company_id STRING, name STRING, domain STRING, city STRING, state STRING,
  industry STRING, hubspot_owner_id STRING, lifecyclestage STRING, num_deals_abertos INT,
  valor_pipeline_brl DOUBLE, data_ultimo_contato DATE, hs_lastmodifieddate BIGINT,
  propriedades_json STRING, _ingerido_em TIMESTAMP)
COMMENT 'FICTICIO - empresas do HubSpot (bronze, incremental por hs_lastmodifieddate)'
TBLPROPERTIES ('lcx_workshop' = 'true', 'dados' = 'ficticios')""")
spark.sql(f"""CREATE TABLE IF NOT EXISTS {uc.t(cfg, 'hubspot_ingestao_log')} (
  ts TIMESTAMP, marca_dagua_ms BIGINT, registros INT, nova_marca_dagua_ms BIGINT)
COMMENT 'FICTICIO - auditoria das ingestoes HubSpot -> UC'""")

marca = 0 if REPROCESSAR else (spark.sql(f"SELECT coalesce(max(hs_lastmodifieddate), 0) m FROM {BRONZE}").first().m)
print("Marca d'água atual:", marca, "" if not marca else f"({dt.datetime.utcfromtimestamp(marca / 1000)} UTC)")

# COMMAND ----------

# MAGIC %md ## 1) Busca incremental no HubSpot (fictício)

# COMMAND ----------

PROPS = ["logcomex_company_id", "name", "domain", "city", "state", "industry", "hubspot_owner_id", "lifecyclestage",
         "num_deals_abertos", "valor_pipeline_brl", "data_ultimo_contato", "hs_lastmodifieddate"]

with MockHubSpot(f"{cfg['volume']}/crm.db") as crm:
    hs = HubSpot(crm.base_url, crm.adapter())
    # marca + 1: a marca d'água é inclusiva no HubSpot (GTE)
    resultados = hs.buscar_modificadas(marca + 1 if marca else 0, PROPS)
print(f"Empresas novas/alteradas desde a última ingestão: {len(resultados):,}")

# COMMAND ----------

# MAGIC %md ## 2) `MERGE` na bronze

# COMMAND ----------

import json
from pyspark.sql import functions as F

if resultados:
    linhas = [{"hs_object_id": r["id"], **{k: r["properties"].get(k) for k in PROPS},
               "propriedades_json": json.dumps(r["properties"], ensure_ascii=False)} for r in resultados]
    df = (spark.createDataFrame(linhas, schema=", ".join(f"{c} string" for c in linhas[0].keys()))
          .withColumn("num_deals_abertos", F.col("num_deals_abertos").cast("int"))
          .withColumn("valor_pipeline_brl", F.col("valor_pipeline_brl").cast("double"))
          .withColumn("data_ultimo_contato", F.to_date("data_ultimo_contato"))
          .withColumn("hs_lastmodifieddate", F.col("hs_lastmodifieddate").cast("bigint"))
          .withColumn("_ingerido_em", F.current_timestamp()))
    df.createOrReplaceTempView("_lote_hubspot")
    spark.sql(f"""MERGE INTO {BRONZE} t USING _lote_hubspot s ON t.hs_object_id = s.hs_object_id
                 WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *""")
    nova = max(int(r["properties"]["hs_lastmodifieddate"]) for r in resultados)
else:
    nova = marca
spark.sql(f"INSERT INTO {uc.t(cfg, 'hubspot_ingestao_log')} VALUES "
          f"(current_timestamp(), {marca}, {len(resultados)}, {nova})")
dbutils.jobs.taskValues.set("ingeridas", len(resultados))

display(spark.sql(f"""SELECT count(*) empresas, sum(num_deals_abertos) negocios_abertos,
  round(sum(valor_pipeline_brl)) pipeline_brl, max(_ingerido_em) ultima_ingestao FROM {BRONZE}"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 🔎 Explore
# MAGIC Rode o notebook de novo: como nada mudou no CRM, a ingestão traz **0 registros** (só o que mudou é lido).
# MAGIC
# MAGIC ✅ Siga para o **03_Calculo_de_Metricas_de_Produto**.
