# Databricks notebook source
# MAGIC %md
# MAGIC # 00 · Configuração e dados fictícios
# MAGIC
# MAGIC **Workshop LogComex · Marketing Ops na Databricks**
# MAGIC
# MAGIC | Lab | O que você vai construir |
# MAGIC |---|---|
# MAGIC | 00 | Ambiente isolado + dados **fictícios** de produto + um HubSpot **fictício** |
# MAGIC | 01 | "Windsor.ai" gravando mídia (Google Ads, Meta, TikTok, ChatGPT Ads, GA4, Search Console) **direto no Unity Catalog** |
# MAGIC | 02 | ETL **HubSpot → Unity Catalog** (ingestão incremental) |
# MAGIC | 03 | Cálculo de métricas de produto (health score, churn, PQL, CAC, prioridade de CS) |
# MAGIC | 04 | **Write-back** Unity Catalog → HubSpot (batch upsert incremental, retry, auditoria) |
# MAGIC | 05 | Orquestração com **Lakeflow Jobs** (02 → 03 → 04) |
# MAGIC | 06 | Mudanças no produto e no CRM + sync incremental (e como evitar loop) |
# MAGIC | 07 | Dashboard AI/BI (opcional) |
# MAGIC | 99 | Limpeza |
# MAGIC
# MAGIC ## 🔒 Garantias de segurança deste workshop
# MAGIC * **Somente dados fictícios.** Empresas, CNPJs (com dígitos inválidos), e-mails (domínio reservado `.example`), campanhas e números são **inventados**.
# MAGIC * **Nada sai do Databricks.** O "HubSpot" é um CRM simulado que roda **em memória dentro do notebook** (sem rede). O cliente HTTP do workshop
# MAGIC   **recusa** qualquer outro endereço, inclusive `api.hubapi.com`. Não há tokens, chaves ou conexões com Windsor.ai, Google, Meta, TikTok ou OpenAI.
# MAGIC * **Isolado.** Tudo é criado num schema exclusivo `lcx_workshop_<seu_usuário>`, marcado como de workshop. O código se recusa a escrever
# MAGIC   fora dele e não lê nenhuma tabela da empresa.
# MAGIC * **Não interfere em processos.** Sem service principals, sem permissões a terceiros. O único job criado (lab 05) fica com o agendamento **PAUSADO**.
# MAGIC * **Reversível.** O notebook `99_Limpeza` apaga tudo o que foi criado.
# MAGIC
# MAGIC ## Pré-requisitos
# MAGIC * Notebook em **Serverless** (recomendado) ou cluster DBR 15.4+.
# MAGIC * Permissão de `USE CATALOG` + `CREATE SCHEMA` em algum catálogo (ex.: um catálogo de sandbox/desenvolvimento).
# MAGIC * (Recomendado) `CAN USE` em um SQL Warehouse, usado nos labs 01 e 07.

# COMMAND ----------

dbutils.widgets.text("catalogo", "", "1. Catálogo (vazio = catálogo padrão)")
dbutils.widgets.text("warehouse_id", "", "2. SQL Warehouse ID (vazio = automático)")

# COMMAND ----------

from databricks.sdk import WorkspaceClient
from lib import config

w = WorkspaceClient()
cfg = config.montar_config(spark, dbutils.widgets.get("catalogo"), dbutils.widgets.get("warehouse_id"))

if not cfg["warehouse_id"]:
    try:  # escolhe um warehouse acessível (prefere serverless e já ligado)
        whs = sorted(w.warehouses.list(), key=lambda x: (not x.enable_serverless_compute,
                                                          str(x.state) != "State.RUNNING"))
        cfg["warehouse_id"] = whs[0].id if whs else ""
    except Exception as e:  # noqa: BLE001
        print("Sem acesso para listar warehouses:", str(e)[:120])

print(f"Usuário ....... {cfg['usuario']}")
print(f"Catálogo ...... {cfg['catalogo']}")
print(f"Schema ........ {cfg['schema']}   (exclusivo deste workshop)")
print(f"SQL Warehouse . {cfg['warehouse_id'] or '(nenhum, os labs usarão o Spark do notebook)'}")

# COMMAND ----------

# MAGIC %md ## 1) Cria o schema isolado (com a marca de workshop) e um Volume para o HubSpot fictício

# COMMAND ----------

config.validar_schema(spark, cfg, criar=True)
spark.sql(f"CREATE VOLUME IF NOT EXISTS {cfg['fq']}.crm_ficticio "
          "COMMENT 'Arquivo do HubSpot fictício (SQLite) do workshop'")
config.salvar_config(spark, cfg)
# re-execução do lab 00 = recomeçar: limpa as tabelas derivadas dos labs seguintes (só dentro do schema do workshop)
for tb in ["bronze_hubspot_companies", "hubspot_ingestao_log", "gold_hubspot_companies", "gold_marketing_canal",
           "hubspot_sync_state", "hubspot_sync_log"]:
    spark.sql(f"DROP TABLE IF EXISTS {cfg['fq']}.`{tb}`")
print("Schema e Volume prontos. Configuração salva em", config.caminho_config(spark))

# COMMAND ----------

# MAGIC %md ## 2) Gera as tabelas de produto (fictícias)
# MAGIC
# MAGIC | Tabela | Representa |
# MAGIC |---|---|
# MAGIC | `produto_empresas` | contas da plataforma (clientes, trials, cancelados), plano, MRR, licenças, canal de aquisição |
# MAGIC | `produto_usuarios` | usuários das contas |
# MAGIC | `produto_uso_diario` | uso diário por conta: buscas de importação, consultas NCM, relatórios, perguntas à IA |
# MAGIC | `marketing_leads` | cadastros no site com UTM (casam com as campanhas do lab 01) |

# COMMAND ----------

import pandas as pd
from lib import dados_ficticios, uc

dados = dados_ficticios.gerar()
emp = pd.DataFrame(dados["empresas"])
print("Gravando tabelas:")
uc.salvar_pandas(spark, cfg, emp.drop(columns=["perfil_uso"]), "produto_empresas",
                 "FICTICIO - contas da plataforma")
uc.salvar_pandas(spark, cfg, pd.DataFrame(dados["usuarios"]), "produto_usuarios", "FICTICIO - usuarios")
uc.salvar_pandas(spark, cfg, pd.DataFrame(dados["uso"]), "produto_uso_diario", "FICTICIO - uso diario por conta")
uc.salvar_pandas(spark, cfg, pd.DataFrame(dados["leads"]), "marketing_leads", "FICTICIO - leads com UTM")
uc.salvar_pandas(spark, cfg, emp[["company_id", "perfil_uso"]], "_gabarito_perfil_uso",
                 "FICTICIO - gabarito interno do workshop (perfil simulado)")

# COMMAND ----------

# MAGIC %md ## 3) Cria o HubSpot fictício com as empresas que o "time comercial" já cadastrou

# COMMAND ----------

from lib.mock_hubspot import MockHubSpot

arquivo = f"{cfg['volume']}/crm.db"
import os
if os.path.exists(arquivo):
    os.remove(arquivo)  # recomeça o CRM fictício do zero
with MockHubSpot(arquivo) as crm:
    n = crm.semear(dados["empresas"])
    print(f"HubSpot fictício em {crm.base_url} com {n} empresas cadastradas pelo time comercial.")
print("Estado do CRM salvo em", arquivo)

# COMMAND ----------

display(spark.sql(f"SHOW TABLES IN {cfg['fq']}"))

# COMMAND ----------

# MAGIC %md ✅ **Pronto!** Siga para o notebook **01_Windsor_ai_para_Unity_Catalog**.
