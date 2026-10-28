# Databricks notebook source
# MAGIC %md
# MAGIC # 05 · Orquestração com Lakeflow Jobs
# MAGIC
# MAGIC Transforma os labs 02 → 03 → 04 num **processo operacional recorrente**:
# MAGIC
# MAGIC ```
# MAGIC  [ingestao_hubspot] ──► [calculo_metricas] ──► [sync_hubspot]
# MAGIC   02 HubSpot → UC        03 gold + row_hash      04 UC → HubSpot
# MAGIC ```
# MAGIC
# MAGIC * **Serverless:** sem cluster para gerenciar.
# MAGIC * **Agendamento diário às 06:00 (BRT), criado PAUSADO.** Nada roda sozinho sem você despausar.
# MAGIC * `max_concurrent_runs = 1`, retry na task de sync, e-mail para você em caso de falha.
# MAGIC * Criado via **Python SDK**. O mesmo job pode ser versionado como código com **Declarative Automation Bundles** (veja o YAML no final).

# COMMAND ----------

from databricks.sdk import WorkspaceClient
from databricks.sdk.service import jobs
from lib import config

w = WorkspaceClient()
cfg = config.carregar_config(spark)
pasta = "/".join(dbutils.notebook.entry_point.getDbutils().notebook().getContext()
                 .notebookPath().get().split("/")[:-1])
nome_job = config.nome_recurso(cfg, "Produto ↔ HubSpot (diário)")
print("Notebooks em:", pasta)
print("Job:", nome_job)

# COMMAND ----------

def task(chave, notebook, depende=None, params=None, retries=0):
    return jobs.Task(task_key=chave, notebook_task=jobs.NotebookTask(
        notebook_path=f"{pasta}/{notebook}", base_parameters=params or {}, source=jobs.Source.WORKSPACE),
        depends_on=[jobs.TaskDependency(task_key=depende)] if depende else None,
        max_retries=retries, min_retry_interval_millis=30000 if retries else None)


definicao = dict(
    name=nome_job,
    description="Workshop LogComex (dados FICTÍCIOS): ingestão HubSpot -> cálculo de métricas -> write-back HubSpot.",
    tasks=[task("ingestao_hubspot", "02_ETL_HubSpot_para_Unity_Catalog"),
           task("calculo_metricas", "03_Calculo_de_Metricas_de_Produto", "ingestao_hubspot"),
           task("sync_hubspot", "04_Sync_para_HubSpot", "calculo_metricas",
                {"run_id": "{{job.run_id}}", "simular_rate_limit": "false", "reenviar_tudo": "false"}, retries=1)],
    schedule=jobs.CronSchedule(quartz_cron_expression="0 0 6 * * ?", timezone_id="America/Sao_Paulo",
                               pause_status=jobs.PauseStatus.PAUSED),
    max_concurrent_runs=1,
    email_notifications=jobs.JobEmailNotifications(on_failure=[cfg["usuario"]]),
    tags={"lcx_workshop": "true", "dados": "ficticios"},
)

job_id = cfg.get("job_id")
existe = False
if job_id:
    try:
        existe = w.jobs.get(job_id).settings.name == nome_job
    except Exception:  # noqa: BLE001
        existe = False
if existe:
    w.jobs.reset(job_id, new_settings=jobs.JobSettings(**definicao))
    print("Job atualizado:", job_id)
else:
    job_id = w.jobs.create(**definicao).job_id
    cfg["job_id"] = job_id
    config.salvar_config(spark, cfg)
    print("Job criado:", job_id)
print(f"Abra: {w.config.host}/jobs/{job_id}")

# COMMAND ----------

# MAGIC %md ## Executa uma vez agora (Run now) e acompanha

# COMMAND ----------

run = w.jobs.run_now(job_id).result()  # espera terminar
print("Resultado:", run.state.result_state, "|", f"{w.config.host}/jobs/{job_id}/runs/{run.run_id}")
for t in run.tasks:
    print(f"  {t.task_key:18s} {t.state.result_state}")

from lib import uc
ing = spark.sql(f"SELECT registros FROM {uc.t(cfg, 'hubspot_ingestao_log')} ORDER BY ts DESC LIMIT 1").first().registros
env = spark.sql(f"SELECT coalesce(sum(registros), 0) n FROM {uc.t(cfg, 'hubspot_sync_log')} "
                f"WHERE run_id = '{run.run_id}'").first().n
print(f"\nIngeridas do HubSpot: {ing:,}  <- as empresas que o próprio sync do lab 04 atualizou (o 'eco' do write-back)")
print(f"Enviadas ao HubSpot:  {env:,}  <- nada mudou de fato, então nada é reenviado (anti-loop)")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 🔎 Explore
# MAGIC * **Anti-loop:** o write-back do lab 04 mudou o `hs_lastmodifieddate` de todas as empresas, então a ingestão as trouxe de volta.
# MAGIC   Mas o cálculo não usa o que nós mesmos escrevemos: o `row_hash` não mudou e o sync enviou **0**. Sem isso, o job ficaria
# MAGIC   reescrevendo o CRM para sempre.
# MAGIC * Abra o job: veja o **grafo** das tasks, o histórico de execuções e os parâmetros.
# MAGIC * O agendamento está **PAUSADO**. Para o workshop, deixe assim.
# MAGIC * Alternativa ao cron: *trigger* por **atualização de tabela** (o job dispara quando `produto_uso_diario` muda).
# MAGIC
# MAGIC <details><summary><b>O mesmo job como código (Declarative Automation Bundles)</b></summary>
# MAGIC
# MAGIC ```yaml
# MAGIC resources:
# MAGIC   jobs:
# MAGIC     produto_hubspot:
# MAGIC       name: "Produto ↔ HubSpot (diário)"
# MAGIC       schedule: {quartz_cron_expression: "0 0 6 * * ?", timezone_id: America/Sao_Paulo, pause_status: PAUSED}
# MAGIC       max_concurrent_runs: 1
# MAGIC       tasks:
# MAGIC         - task_key: ingestao_hubspot
# MAGIC           notebook_task: {notebook_path: ./notebooks/02_ETL_HubSpot_para_Unity_Catalog.py}
# MAGIC         - task_key: calculo_metricas
# MAGIC           depends_on: [{task_key: ingestao_hubspot}]
# MAGIC           notebook_task: {notebook_path: ./notebooks/03_Calculo_de_Metricas_de_Produto.py}
# MAGIC         - task_key: sync_hubspot
# MAGIC           depends_on: [{task_key: calculo_metricas}]
# MAGIC           max_retries: 1
# MAGIC           notebook_task:
# MAGIC             notebook_path: ./notebooks/04_Sync_para_HubSpot.py
# MAGIC             base_parameters: {run_id: "{{job.run_id}}"}
# MAGIC ```
# MAGIC </details>
# MAGIC
# MAGIC ✅ Siga para o **06_Mudancas_e_Sync_Incremental**.
