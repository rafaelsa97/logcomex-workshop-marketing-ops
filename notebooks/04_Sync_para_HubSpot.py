# Databricks notebook source
# MAGIC %md
# MAGIC # 04 · Write-back Unity Catalog → HubSpot (sync incremental)
# MAGIC
# MAGIC Fecha o ciclo: pega `gold_hubspot_companies`, envia **só as contas cujo `row_hash` mudou** desde o último envio e faz
# MAGIC **batch upsert** (`POST /crm/v3/objects/companies/batch/upsert`, até 100 por chamada) usando `logcomex_company_id` como
# MAGIC chave única (`idProperty`), sem precisar conhecer o ID interno do HubSpot.
# MAGIC
# MAGIC Boas práticas incluídas:
# MAGIC * cria as **propriedades customizadas** (grupo "LogComex · Produto") se não existirem;
# MAGIC * **retry com backoff** em `429` (rate limit) e `5xx`;
# MAGIC * **erros por registro** (`207`) não derrubam o lote e ficam registrados;
# MAGIC * **auditoria** em `hubspot_sync_log` e estado por conta em `hubspot_sync_state`.
# MAGIC
# MAGIC > 🔒 Destino: o HubSpot **fictício** local. Em produção, a diferença seria `base_url = "https://api.hubapi.com"` + token de
# MAGIC > Private App em `dbutils.secrets`. **Neste workshop isso está bloqueado de propósito.**

# COMMAND ----------

dbutils.widgets.dropdown("simular_rate_limit", "false", ["false", "true"], "Simular rate limit do HubSpot")
dbutils.widgets.dropdown("reenviar_tudo", "false", ["false", "true"], "Reenviar tudo")
dbutils.widgets.text("run_id", "manual", "Run id (preenchido pelo job)")
RATE_LIMIT = dbutils.widgets.get("simular_rate_limit") == "true"
FULL = dbutils.widgets.get("reenviar_tudo") == "true"
RUN_ID = dbutils.widgets.get("run_id") or "manual"

# COMMAND ----------

import datetime as dt
from lib import config, uc
from lib.hubspot_client import HubSpot
from lib.mock_hubspot import MockHubSpot
from lib import crm_view

cfg = config.carregar_config(spark)
T = lambda n: uc.t(cfg, n)

PROPRIEDADES = {  # nome interno: (rótulo, type, fieldType, opções)
    "logcomex_cnpj": ("CNPJ", "string", "text", None),
    "logcomex_plano": ("Plano", "enumeration", "select", ["Trial", "Starter", "Pro", "Enterprise"]),
    "logcomex_status_conta": ("Status da conta", "enumeration", "select", ["trial", "trial_expirado", "ativo", "cancelado"]),
    "logcomex_mrr_brl": ("MRR (R$)", "number", "number", None),
    "logcomex_licencas": ("Licenças", "number", "number", None),
    "logcomex_usuarios_ativos_30d": ("Usuários ativos (30d)", "number", "number", None),
    "logcomex_buscas_30d": ("Buscas de importação (30d)", "number", "number", None),
    "logcomex_perguntas_ia_30d": ("Perguntas à IA (30d)", "number", "number", None),
    "logcomex_ultima_atividade": ("Última atividade na plataforma", "date", "date", None),
    "logcomex_health_score": ("Health score", "number", "number", None),
    "logcomex_risco_churn": ("Risco de churn", "enumeration", "select", ["Baixo", "Médio", "Alto"]),
    "logcomex_pql_score": ("PQL score", "number", "number", None),
    "logcomex_sinal_expansao": ("Sinal de expansão", "enumeration", "select", ["Nenhum", "Licenças", "Upgrade de plano"]),
    "logcomex_prioridade_cs": ("Prioridade CS", "enumeration", "select", ["Urgente", "Alta", "Média", "Normal"]),
    "logcomex_canal_aquisicao": ("Canal de aquisição", "string", "text", None),
    "logcomex_campanha_aquisicao": ("Campanha de aquisição", "string", "text", None),
    "logcomex_cac_canal_brl": ("CAC do canal (R$)", "number", "number", None),
}

spark.sql(f"""CREATE TABLE IF NOT EXISTS {T('hubspot_sync_state')} (
  logcomex_company_id STRING, row_hash STRING, hs_object_id STRING, synced_at TIMESTAMP, run_id STRING)
COMMENT 'FICTICIO - ultimo estado enviado ao HubSpot por conta'""")
spark.sql(f"""CREATE TABLE IF NOT EXISTS {T('hubspot_sync_log')} (
  run_id STRING, ts TIMESTAMP, lote INT, http_status INT, tentativas INT, latencia_ms INT, registros INT,
  criados INT, atualizados INT, erros INT, detalhe_erros STRING)
COMMENT 'FICTICIO - auditoria de cada batch upsert ao HubSpot'""")

# COMMAND ----------

# MAGIC %md ## 1) O que mudou desde o último envio?

# COMMAND ----------

gold = spark.table(T("gold_hubspot_companies"))
estado = spark.table(T("hubspot_sync_state")).select("logcomex_company_id", "row_hash")
delta = gold if FULL else gold.join(estado, ["logcomex_company_id", "row_hash"], "left_anti")
colunas = ["logcomex_company_id", "name", "domain", "city", "state"] + list(PROPRIEDADES)
envio = [r.asDict() for r in delta.select(*colunas).collect()]
hashes = {r.logcomex_company_id: r.row_hash for r in delta.select("logcomex_company_id", "row_hash").collect()}
print(f"Contas no gold: {gold.count():,} | a enviar: {len(envio):,} {'(reenvio total)' if FULL else '(só alteradas)'}")

# COMMAND ----------

# MAGIC %md ## 2) Propriedades + batch upsert no HubSpot (fictício)

# COMMAND ----------

with MockHubSpot(f"{cfg['volume']}/crm.db", rate_limit=RATE_LIMIT) as crm:
    hs = HubSpot(crm.base_url, crm.adapter())
    criadas = hs.garantir_propriedades("logcomex_produto", "LogComex · Produto", PROPRIEDADES)
    print(f"Propriedades customizadas criadas agora: {criadas}")
    if criadas and not FULL:  # portal novo/resetado: o estado local não vale mais
        print("Portal sem as propriedades -> reenviando todas as contas")
        envio = [r.asDict() for r in gold.select(*colunas).collect()]
        hashes = {r.logcomex_company_id: r.row_hash for r in gold.select("logcomex_company_id", "row_hash").collect()}
    # dono de cada propriedade: nome/domínio/cidade são do time comercial -> só enviamos para contas que
    # ainda NÃO existem no CRM (para criá-las); nas existentes, enviamos apenas as propriedades logcomex_*
    no_crm = {c.get("logcomex_company_id") for c in crm.listar()}
    envio = [{k: v for k, v in r.items() if k not in ("name", "domain", "city", "state")}
             if r["logcomex_company_id"] in no_crm else r for r in envio]
    log, resultados = hs.upsert_lotes(envio, "logcomex_company_id")
    empresas_crm, chamadas = crm.listar(), list(crm.log)

# COMMAND ----------

# MAGIC %md ## 3) Auditoria e estado no Unity Catalog

# COMMAND ----------

agora = dt.datetime.utcnow()
if log:
    spark.createDataFrame([{"run_id": RUN_ID, "ts": agora, **l} for l in log],
                          "run_id string, ts timestamp, lote int, http_status int, tentativas int, latencia_ms int, "
                          "registros int, criados int, atualizados int, erros int, detalhe_erros string") \
        .write.mode("append").saveAsTable(T("hubspot_sync_log"))
if resultados:
    estado_novo = [(x["properties"]["logcomex_company_id"], hashes.get(x["properties"]["logcomex_company_id"]),
                    x["id"], agora, RUN_ID) for x in resultados]
    spark.createDataFrame(estado_novo, "logcomex_company_id string, row_hash string, hs_object_id string, "
                          "synced_at timestamp, run_id string").createOrReplaceTempView("_novo_estado")
    spark.sql(f"""MERGE INTO {T('hubspot_sync_state')} t USING _novo_estado s
                 ON t.logcomex_company_id = s.logcomex_company_id
                 WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *""")
erros = sum(l["erros"] for l in log)
print(f"Sync concluído: {len(resultados):,} contas atualizadas no HubSpot, {erros} erros.")
dbutils.jobs.taskValues.set("enviadas", len(resultados))

# COMMAND ----------

# MAGIC %md ## 4) Como ficou o HubSpot (fictício)

# COMMAND ----------

displayHTML(crm_view.render(empresas_crm, "Empresas (mais recentes primeiro)", destacar=set(hashes) if len(hashes) <= 30 else None))

# COMMAND ----------

displayHTML(crm_view.render_log(chamadas))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 🔎 Explore
# MAGIC * Rode de novo **sem mudar nada**: 0 contas enviadas (o `row_hash` não mudou).
# MAGIC * Ligue `simular_rate_limit = true` e `reenviar_tudo = true`: veja os `429` no log e o retry automático.
# MAGIC * `SELECT * FROM hubspot_sync_log ORDER BY ts DESC` mostra a auditoria de cada lote.
# MAGIC
# MAGIC ✅ Siga para o **05_Orquestracao_com_Lakeflow_Jobs**.
