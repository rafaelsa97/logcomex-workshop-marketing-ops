# Databricks notebook source
# MAGIC %md
# MAGIC # 99 · Limpeza
# MAGIC
# MAGIC Remove **somente** o que este workshop criou para o seu usuário:
# MAGIC * o job `[Workshop LCX] …` (lab 05) e o dashboard (lab 07), se existirem;
# MAGIC * o schema `lcx_workshop_<seu_usuário>` (tabelas, view e Volume com o HubSpot fictício). **Só se tiver a marca de workshop**;
# MAGIC * o arquivo de configuração na sua pasta de usuário.
# MAGIC
# MAGIC Mude o widget `confirmar` para **SIM** e rode todas as células.

# COMMAND ----------

dbutils.widgets.dropdown("confirmar", "NAO", ["NAO", "SIM"], "Confirmar limpeza")

# COMMAND ----------

from databricks.sdk import WorkspaceClient
from lib import config

w = WorkspaceClient()
cfg = config.carregar_config(spark)  # valida que o schema é do workshop
print("Será removido:", cfg["fq"], "| job:", cfg.get("job_id"), "| dashboard:", cfg.get("dashboard_id"))
if dbutils.widgets.get("confirmar") != "SIM":
    dbutils.notebook.exit("Nada foi removido. Mude 'confirmar' para SIM.")

# COMMAND ----------

if cfg.get("job_id"):
    try:
        j = w.jobs.get(cfg["job_id"])
        if j.settings.name == config.nome_recurso(cfg, "Produto ↔ HubSpot (diário)"):
            w.jobs.delete(cfg["job_id"])
            print("Job removido")
    except Exception as e:  # noqa: BLE001
        print("Job:", str(e)[:120])
if cfg.get("dashboard_id"):
    try:
        w.api_client.do("DELETE", f"/api/2.0/lakeview/dashboards/{cfg['dashboard_id']}")
        print("Dashboard enviado para a lixeira")
    except Exception as e:  # noqa: BLE001
        print("Dashboard:", str(e)[:120])

config.validar_schema(spark, cfg)  # confere a marca de novo antes de apagar
spark.sql(f"DROP SCHEMA {cfg['fq']} CASCADE")
print("Schema removido:", cfg["fq"])
w.workspace.delete(config.caminho_config(spark))
print("Configuração removida. Limpeza concluída ✅")
