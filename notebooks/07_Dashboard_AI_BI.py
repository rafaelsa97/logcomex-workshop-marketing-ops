# Databricks notebook source
# MAGIC %md
# MAGIC # 07 · (Opcional) Dashboard AI/BI
# MAGIC
# MAGIC Cria um dashboard com duas páginas sobre as tabelas **do seu schema de workshop**:
# MAGIC * **Mídia & Aquisição**: investimento por canal, CAC por canal (mídia × produto), funil, GA4 por origem e Search Console;
# MAGIC * **Produto → HubSpot**: clientes em risco, MRR em risco, trials quentes e a auditoria do sync.
# MAGIC
# MAGIC Requer um **SQL Warehouse** (o configurado no notebook 00).

# COMMAND ----------

from databricks.sdk import WorkspaceClient
from lib import config, dashboard_def

w = WorkspaceClient()
cfg = config.carregar_config(spark)
assert cfg["warehouse_id"], "Configure um SQL Warehouse no notebook 00 para criar o dashboard."
nome = config.nome_recurso(cfg, "Marketing & Produto")
pasta = f"/Users/{cfg['usuario']}"
corpo = {"display_name": nome, "warehouse_id": cfg["warehouse_id"],
         "serialized_dashboard": dashboard_def.definicao(cfg["fq"]), "parent_path": pasta}

dash_id = cfg.get("dashboard_id")
try:
    if dash_id:
        w.api_client.do("PATCH", f"/api/2.0/lakeview/dashboards/{dash_id}", body=corpo)
        print("Dashboard atualizado")
except Exception:  # noqa: BLE001
    dash_id = None
if not dash_id:
    dash_id = w.api_client.do("POST", "/api/2.0/lakeview/dashboards", body=corpo)["dashboard_id"]
    cfg["dashboard_id"] = dash_id
    config.salvar_config(spark, cfg)
    print("Dashboard criado")
w.api_client.do("POST", f"/api/2.0/lakeview/dashboards/{dash_id}/published",
                body={"warehouse_id": cfg["warehouse_id"], "embed_credentials": False})
print(f"Abra: {w.config.host}/dashboardsv3/{dash_id}/published")

# COMMAND ----------

# MAGIC %md ✅ Fim dos labs! Quando terminar, rode o **99_Limpeza**.
