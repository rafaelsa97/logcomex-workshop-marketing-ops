# Databricks notebook source
# MAGIC %md
# MAGIC # 06 · Mudanças no produto e no CRM + sync incremental
# MAGIC
# MAGIC Vamos provocar mudanças **dos dois lados** e rodar o job do lab 05:
# MAGIC
# MAGIC | Lado | Mudança simulada | Efeito esperado no HubSpot |
# MAGIC |---|---|---|
# MAGIC | Produto | 5 clientes saudáveis **param de usar** a plataforma (últimas 3 semanas) | risco de churn vira **Alto** |
# MAGIC | Produto | 3 trials passam a usar muito (inclusive a IA) | **PQL** dispara |
# MAGIC | CRM | o time comercial **abre negócios** em 12 empresas | se a conta tem risco Alto, **prioridade CS = Urgente** |
# MAGIC
# MAGIC Resultado esperado: a ingestão traz **só as empresas que o comercial alterou** e o sync envia **só as contas cujas métricas mudaram**.

# COMMAND ----------

dbutils.widgets.text("rodada", "1", "Rodada (mude para repetir com outras contas)")
RODADA = dbutils.widgets.get("rodada")

# COMMAND ----------

import datetime as dt
from databricks.sdk import WorkspaceClient
from lib import config, uc
from lib.mock_hubspot import MockHubSpot

w = WorkspaceClient()
cfg = config.carregar_config(spark)
T = lambda n: uc.t(cfg, n)
assert cfg.get("job_id"), "Rode antes o notebook 05 (cria o job)."

# COMMAND ----------

# MAGIC %md ## 1) Mudanças no produto

# COMMAND ----------

ref = spark.sql(f"SELECT max(data) d FROM {T('produto_uso_diario')}").first().d
saudaveis = [r.company_id for r in spark.sql(f"""
  SELECT g.company_id FROM {T('_gabarito_perfil_uso')} g
  JOIN {T('gold_hubspot_companies')} c ON c.logcomex_company_id = g.company_id
  WHERE g.perfil_uso = 'saudavel' AND c.logcomex_risco_churn = 'Baixo'
  ORDER BY sha2(concat(g.company_id, '{RODADA}'), 256) LIMIT 5""").collect()]
frios = [r.company_id for r in spark.sql(f"""
  SELECT c.logcomex_company_id company_id FROM {T('gold_hubspot_companies')} c
  WHERE c.logcomex_status_conta = 'trial' AND coalesce(c.logcomex_pql_score, 0) < 40
  ORDER BY sha2(concat(c.logcomex_company_id, '{RODADA}'), 256) LIMIT 3""").collect()]

ids = ", ".join(f"'{c}'" for c in saudaveis)
spark.sql(f"DELETE FROM {T('produto_uso_diario')} WHERE company_id IN ({ids}) AND data > date'{ref}' - 21")
boost = [(ref - dt.timedelta(days=k), cid, 3, 7, 95, 30, 6, 28) for cid in frios for k in range(10)
         if (ref - dt.timedelta(days=k)).weekday() < 5]
spark.createDataFrame(boost, "data date, company_id string, usuarios_ativos int, logins int, buscas_importacao int, "
                      "consultas_ncm int, relatorios_exportados int, perguntas_logcomex_ai int") \
    .createOrReplaceTempView("_boost")
spark.sql(f"""MERGE INTO {T('produto_uso_diario')} t USING _boost s
  ON t.company_id = s.company_id AND t.data = s.data
  WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *""")
print("Pararam de usar:", saudaveis)
print("Trials engajados:", frios)

# COMMAND ----------

# MAGIC %md ## 2) Mudanças no CRM (o time comercial abre negócios)

# COMMAND ----------

with MockHubSpot(f"{cfg['volume']}/crm.db") as crm:
    alteradas = crm.simular_atividade_comercial(12, semente=f"rodada-{RODADA}")
print("Empresas com novos negócios no CRM:", alteradas)

# COMMAND ----------

# MAGIC %md ## 3) Roda o job (ingestão → cálculo → sync)

# COMMAND ----------

run = w.jobs.run_now(cfg["job_id"]).result()
print("Resultado:", run.state.result_state, "|", f"{w.config.host}/jobs/{cfg['job_id']}/runs/{run.run_id}")
for t in run.tasks:
    print(f"  {t.task_key:18s} {t.state.result_state}")

ing = spark.sql(f"SELECT registros FROM {T('hubspot_ingestao_log')} ORDER BY ts DESC LIMIT 1").first().registros
env = spark.sql(f"SELECT coalesce(sum(registros), 0) n FROM {T('hubspot_sync_log')} WHERE run_id = '{run.run_id}'").first().n
print(f"\nIngeridas do HubSpot nesta rodada: {ing:,}  (alteradas no CRM desde a última ingestão)")
print(f"Enviadas ao HubSpot nesta rodada:  {env:,}  (só as contas cujas métricas mudaram)")

# COMMAND ----------

display(spark.sql(f"""SELECT logcomex_company_id, name, logcomex_status_conta, logcomex_health_score,
  logcomex_risco_churn, logcomex_pql_score, logcomex_prioridade_cs
FROM {T('gold_hubspot_companies')}
WHERE logcomex_company_id IN ({ids}, {', '.join(f"'{c}'" for c in frios)})
   OR (logcomex_prioridade_cs = 'Urgente' AND name IN ({', '.join("'" + n.replace("'", "") + "'" for n in alteradas)}))"""))

# COMMAND ----------

from lib import crm_view
with MockHubSpot(f"{cfg['volume']}/crm.db") as crm:
    displayHTML(crm_view.render(crm.listar(), "Empresas alteradas nesta rodada", limite=20,
                                destacar=set(saudaveis + frios)))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 🔎 Explore
# MAGIC * Rode o job de novo **sem mudanças**: a ingestão traz as empresas que acabamos de atualizar (o "eco" do write-back), mas o sync envia **0**. É o anti-loop.
# MAGIC * Mude `rodada` para `2` e repita: outras contas serão afetadas.
# MAGIC
# MAGIC ✅ Opcional: **07_Dashboard_AI_BI**. No fim, **99_Limpeza**.
