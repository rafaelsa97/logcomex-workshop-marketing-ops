# Databricks notebook source
# MAGIC %md
# MAGIC # 03 · Cálculo de métricas de produto (o "processo operacional")
# MAGIC
# MAGIC Cruza **produto** (lab 00) + **mídia** (lab 01) + **CRM** (lab 02) e calcula, por conta, o que Vendas, CS e Marketing
# MAGIC precisam ver **dentro do HubSpot**:
# MAGIC
# MAGIC | Propriedade HubSpot | Regra |
# MAGIC |---|---|
# MAGIC | `logcomex_health_score` (0–100) | 35% tendência de uso (30d vs 30d anteriores), 30% uso das licenças, 20% recência, 15% adoção da IA |
# MAGIC | `logcomex_risco_churn` | Alto (<40), Médio (40–64), Baixo (≥65), só clientes ativos |
# MAGIC | `logcomex_pql_score` (0–100) | engajamento do trial (usuários ativos, buscas, uso de IA) |
# MAGIC | `logcomex_sinal_expansao` | "Licenças" (pico de ativos ≥ 90% das licenças) ou "Upgrade de plano" (buscas acima do p90 do plano) |
# MAGIC | `logcomex_prioridade_cs` | **usa o dado do CRM**: Urgente = risco Alto **e** negócio aberto; Alta = risco Alto; Média = risco Médio |
# MAGIC | `logcomex_canal_aquisicao`, `logcomex_cac_canal_brl` | atribuição *first-touch* + CAC do canal (investimento do lab 01 ÷ clientes pagantes) |
# MAGIC
# MAGIC Saída: `gold_hubspot_companies`, com os **nomes internos das propriedades do HubSpot** e um `row_hash` para o lab 04
# MAGIC enviar **só o que mudou**.
# MAGIC
# MAGIC > 🔁 **Anti-loop:** do CRM usamos só campos que **o time comercial** mantém (`num_deals_abertos`, owner). Nunca usamos
# MAGIC > `hs_lastmodifieddate` nem as propriedades `logcomex_*` (que nós mesmos escrevemos) como entrada do cálculo.

# COMMAND ----------

from lib import config, uc

cfg = config.carregar_config(spark)
T = lambda n: uc.t(cfg, n)

# COMMAND ----------

# MAGIC %md ## CAC e funil por canal (mídia do Windsor.ai × produto)

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {T('gold_marketing_canal')}
COMMENT 'FICTICIO - Funil e CAC por canal de mídia: investimento (Windsor.ai) x leads x trials x clientes pagantes (produto)'
AS
WITH midia AS (
  SELECT datasource, sum(spend) investimento_brl, sum(clicks) cliques, sum(impressions) impressoes,
         sum(conversions) leads_plataforma
  FROM {T('vw_midia_unificada')} GROUP BY 1),
leads AS (SELECT datasource, count(*) leads FROM {T('marketing_leads')} GROUP BY 1),
contas AS (
  SELECT canal_aquisicao datasource, count(*) trials,
         count_if(status IN ('ativo', 'cancelado') AND plano <> 'Trial') clientes_pagantes,
         sum(mrr_brl) mrr_brl
  FROM {T('produto_empresas')} GROUP BY 1)
SELECT m.datasource canal,
       round(m.investimento_brl, 2) investimento_brl, m.impressoes, m.cliques,
       l.leads, c.trials, c.clientes_pagantes, round(c.mrr_brl, 2) mrr_brl,
       round(m.investimento_brl / nullif(l.leads, 0), 2) cpl_brl,
       round(c.trials / nullif(l.leads, 0), 4) taxa_lead_trial,
       round(c.clientes_pagantes / nullif(c.trials, 0), 4) taxa_trial_pago,
       round(m.investimento_brl / nullif(c.clientes_pagantes, 0), 2) cac_brl,
       round(c.mrr_brl * 12 / nullif(m.investimento_brl, 0), 2) roi_arr
FROM midia m LEFT JOIN leads l USING (datasource) LEFT JOIN contas c USING (datasource)
""")
display(spark.table(f"{T('gold_marketing_canal')}"))

# COMMAND ----------

# MAGIC %md ## Health score, risco de churn, PQL e expansão (por conta)

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TEMP VIEW _gold_props AS
WITH ref AS (SELECT max(data) AS d FROM {T('produto_uso_diario')}),
u AS (
  SELECT company_id,
    max(usuarios_ativos) FILTER (WHERE data > d - 30)                     AS pico_ativos_30d,
    coalesce(sum(usuarios_ativos) FILTER (WHERE data > d - 30), 0) / 21.4 AS media_ativos_dia_util,
    coalesce(sum(buscas_importacao) FILTER (WHERE data > d - 30), 0)      AS buscas_30d,
    coalesce(sum(buscas_importacao) FILTER (WHERE data BETWEEN d - 59 AND d - 30), 0) AS buscas_30d_ant,
    coalesce(sum(consultas_ncm) FILTER (WHERE data > d - 30), 0)          AS ncm_30d,
    coalesce(sum(perguntas_logcomex_ai) FILTER (WHERE data > d - 30), 0) AS ia_30d,
    coalesce(sum(relatorios_exportados) FILTER (WHERE data > d - 30), 0)  AS relatorios_30d,
    max(data) AS ultima_atividade,
    max(d) AS d_ref
  FROM {T('produto_uso_diario')} CROSS JOIN ref GROUP BY company_id),
usr AS (
  SELECT company_id, count(*) AS usuarios_cadastrados FROM {T('produto_usuarios')} GROUP BY 1),
cac AS (SELECT canal, cac_brl FROM {T('gold_marketing_canal')}),
crm AS (SELECT logcomex_company_id AS company_id, num_deals_abertos, hubspot_owner_id
        FROM {T('bronze_hubspot_companies')}),
base AS (
  SELECT e.*, coalesce(u.pico_ativos_30d, 0) ativos_30d, coalesce(u.media_ativos_dia_util, 0) media_ativos, coalesce(u.buscas_30d, 0) buscas_30d,
         coalesce(u.buscas_30d_ant, 0) buscas_30d_ant, coalesce(u.ncm_30d, 0) ncm_30d,
         coalesce(u.ia_30d, 0) ia_30d, coalesce(u.relatorios_30d, 0) relatorios_30d,
         u.ultima_atividade, coalesce(datediff(u.d_ref, u.ultima_atividade), 999) dias_sem_uso,
         usr.usuarios_cadastrados, cac.cac_brl, coalesce(crm.num_deals_abertos, 0) num_deals_abertos
  FROM {T('produto_empresas')} e
  LEFT JOIN u USING (company_id) LEFT JOIN usr USING (company_id)
  LEFT JOIN cac ON cac.canal = e.canal_aquisicao
  LEFT JOIN crm USING (company_id)),
p90 AS (SELECT plano, percentile(buscas_30d, 0.9) p90_buscas FROM base WHERE status = 'ativo' GROUP BY 1),
score AS (
  SELECT b.*, p90.p90_buscas,
    least(1.0, ativos_30d / greatest(licencas, 1))                                         AS pico_utilizacao,
    least(1.0, media_ativos / greatest(licencas, 1) / 0.6)                                 AS utilizacao_licencas,
    CASE WHEN buscas_30d_ant = 0 THEN IF(buscas_30d > 0, 1.0, 0.0)
         ELSE least(1.0, greatest(0.0, 0.8 + (buscas_30d - buscas_30d_ant) / buscas_30d_ant)) END AS tendencia,
    greatest(0.0, 1 - dias_sem_uso / 14.0)                                                AS recencia,
    least(1.0, ia_30d / greatest(media_ativos * 30.0, 1))                                  AS adocao_ia
  FROM base b LEFT JOIN p90 USING (plano))
SELECT
  company_id                                    AS logcomex_company_id,
  razao_social                                  AS name,
  dominio                                       AS domain,
  cidade                                        AS city,
  uf                                            AS state,
  cnpj                                          AS logcomex_cnpj,
  plano                                         AS logcomex_plano,
  status                                        AS logcomex_status_conta,
  mrr_brl                                       AS logcomex_mrr_brl,
  licencas                                      AS logcomex_licencas,
  ativos_30d                                    AS logcomex_usuarios_ativos_30d,
  buscas_30d                                    AS logcomex_buscas_30d,
  ia_30d                                        AS logcomex_perguntas_ia_30d,
  CAST(ultima_atividade AS STRING)              AS logcomex_ultima_atividade,
  CASE WHEN status = 'ativo' THEN CAST(round(100 * (0.35 * tendencia + 0.30 * utilizacao_licencas
       + 0.20 * recencia + 0.15 * adocao_ia)) AS INT) END                                   AS logcomex_health_score,
  CASE WHEN status <> 'ativo' THEN NULL
       WHEN round(100 * (0.35 * tendencia + 0.30 * utilizacao_licencas + 0.20 * recencia + 0.15 * adocao_ia)) < 40 THEN 'Alto'
       WHEN round(100 * (0.35 * tendencia + 0.30 * utilizacao_licencas + 0.20 * recencia + 0.15 * adocao_ia)) < 65 THEN 'Médio'
       ELSE 'Baixo' END                                                                     AS logcomex_risco_churn,
  CASE WHEN status = 'trial' THEN CAST(least(100, round(
         40 * least(1.0, ativos_30d / 3.0) + 35 * least(1.0, buscas_30d / 120.0)
       + 25 * least(1.0, ia_30d / 25.0))) AS INT) END                                       AS logcomex_pql_score,
  CASE WHEN status <> 'ativo' THEN NULL
       WHEN pico_utilizacao >= 0.9 AND licencas >= 3 THEN 'Licenças'
       WHEN buscas_30d > p90_buscas AND plano <> 'Enterprise' THEN 'Upgrade de plano'
       ELSE 'Nenhum' END                                                                    AS logcomex_sinal_expansao,
  CASE WHEN status <> 'ativo' THEN NULL
       WHEN round(100 * (0.35 * tendencia + 0.30 * utilizacao_licencas + 0.20 * recencia + 0.15 * adocao_ia)) < 40
            AND num_deals_abertos > 0 THEN 'Urgente'
       WHEN round(100 * (0.35 * tendencia + 0.30 * utilizacao_licencas + 0.20 * recencia + 0.15 * adocao_ia)) < 40 THEN 'Alta'
       WHEN round(100 * (0.35 * tendencia + 0.30 * utilizacao_licencas + 0.20 * recencia + 0.15 * adocao_ia)) < 65 THEN 'Média'
       ELSE 'Normal' END                                                                    AS logcomex_prioridade_cs,
  coalesce(canal_aquisicao, 'Desconhecido')     AS logcomex_canal_aquisicao,
  campanha_aquisicao                            AS logcomex_campanha_aquisicao,
  cac_brl                                       AS logcomex_cac_canal_brl,
  current_timestamp()                           AS calculado_em
FROM score
""")

# hash das propriedades (exceto calculado_em) -> sync incremental
props = [c for c in spark.table("_gold_props").columns if c != "calculado_em"]
spark.sql(f"""CREATE OR REPLACE TABLE {T('gold_hubspot_companies')} AS
SELECT *, sha2(to_json(named_struct({', '.join(f"'{c}', {c}" for c in props)})), 256) AS row_hash
FROM _gold_props""")
uc.marcar(spark, cfg, "gold_hubspot_companies", "FICTICIO - propriedades por conta prontas para o HubSpot + row_hash")
uc.marcar(spark, cfg, "gold_marketing_canal", "FICTICIO - funil e CAC por canal")

display(spark.sql(f"""SELECT logcomex_status_conta, logcomex_risco_churn, count(*) contas,
  round(avg(logcomex_health_score)) health_medio, round(avg(logcomex_pql_score)) pql_medio,
  count_if(logcomex_prioridade_cs = 'Urgente') prioridade_urgente
FROM {T('gold_hubspot_companies')} GROUP BY ALL ORDER BY 1, 2"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 🔎 Explore
# MAGIC * Catalog Explorer → `gold_hubspot_companies` → **Lineage**: veja a tabela nascendo de produto + mídia + CRM.
# MAGIC * Altere um peso do health score e rode de novo: só as contas afetadas mudarão de `row_hash`.
# MAGIC
# MAGIC ✅ Siga para o **04_Sync_para_HubSpot**.
