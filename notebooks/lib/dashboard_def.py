"""Definição do dashboard AI/BI do workshop (dados FICTÍCIOS). Use `definicao(fq)`."""
import json
import re


def ds(name, display, sql):
    return {"name": name, "displayName": display, "queryLines": [l + "\n" for l in sql.strip().split("\n")]}

DATASETS = [
    ds("ds_midia", "Mídia diária (Windsor.ai)", """
SELECT date, CASE datasource WHEN 'google_ads' THEN 'Google Ads' WHEN 'facebook' THEN 'Meta Ads'
  WHEN 'tiktok' THEN 'TikTok Ads' WHEN 'chatgpt_ads' THEN 'ChatGPT Ads' END AS canal,
  campaign, impressions, clicks, spend, conversions AS leads
FROM vw_midia_unificada"""),
    ds("ds_canal", "Funil e CAC por canal", """
SELECT CASE canal WHEN 'google_ads' THEN 'Google Ads' WHEN 'facebook' THEN 'Meta Ads'
  WHEN 'tiktok' THEN 'TikTok Ads' WHEN 'chatgpt_ads' THEN 'ChatGPT Ads' END AS canal,
  investimento_brl, cliques, leads, trials, clientes_pagantes, mrr_brl, cpl_brl,
  taxa_lead_trial, taxa_trial_pago, cac_brl, roi_arr
FROM gold_marketing_canal ORDER BY cac_brl"""),
    ds("ds_ga4", "GA4 por origem (Windsor.ai)", """
SELECT date, CASE WHEN session_source_medium LIKE 'chatgpt.com%' THEN 'ChatGPT (referral + ads)'
  WHEN session_source_medium LIKE 'perplexity%' THEN 'Perplexity'
  WHEN session_source_medium LIKE '%organic%' THEN 'Google orgânico'
  WHEN session_source_medium IN ('google / cpc') THEN 'Google Ads'
  WHEN session_source_medium LIKE '%paid_social%' THEN 'Social pago'
  ELSE 'Direto / outros' END AS origem,
  sessions, key_events
FROM windsor_ga4"""),
    ds("ds_gsc", "Search Console (Windsor.ai)", """
SELECT query, page, sum(clicks) AS cliques, sum(impressions) AS impressoes,
  round(sum(clicks) / sum(impressions), 4) AS ctr, round(avg(position), 1) AS posicao_media
FROM windsor_searchconsole WHERE date > date_sub(current_date(), 30)
GROUP BY query, page ORDER BY cliques DESC"""),
    ds("ds_contas", "Contas (gold HubSpot)", """
SELECT logcomex_company_id, name, logcomex_plano AS plano, logcomex_status_conta AS status,
  logcomex_mrr_brl AS mrr, logcomex_health_score AS health_score, logcomex_risco_churn AS risco_churn,
  logcomex_pql_score AS pql_score, logcomex_sinal_expansao AS sinal_expansao,
  logcomex_usuarios_ativos_30d AS ativos_30d, logcomex_licencas AS licencas,
  logcomex_canal_aquisicao AS canal_aquisicao, logcomex_ultima_atividade AS ultima_atividade,
  CASE WHEN logcomex_risco_churn = 'Alto' THEN logcomex_mrr_brl ELSE 0 END AS mrr_em_risco,
  CASE WHEN logcomex_pql_score >= 70 THEN 1 ELSE 0 END AS trial_quente
FROM gold_hubspot_companies"""),
    ds("ds_sync", "Execuções do sync HubSpot", """
SELECT run_id, min(ts) AS inicio, max(ts) AS fim, 'HubSpot fictício' AS modo, count(*) AS lotes,
  sum(registros) AS registros, sum(criados) AS criados, sum(atualizados) AS atualizados,
  sum(erros) AS erros, sum(tentativas - 1) AS retries_429, round(avg(latencia_ms)) AS latencia_media_ms
FROM hubspot_sync_log GROUP BY run_id ORDER BY inicio DESC"""),
]

def text(name, md, x, y, w, h):
    return {"widget": {"name": name, "multilineTextboxSpec": {"lines": [md]}},
            "position": {"x": x, "y": y, "width": w, "height": h}}

def counter(name, dataset, expr, title, fmt, x, y, w=3, h=3):
    f = {"name": name + "_v", "expression": expr}
    return {"widget": {"name": name, "queries": [{"name": "main_query", "query": {
        "datasetName": dataset, "fields": [f], "disaggregated": False}}],
        "spec": {"version": 2, "widgetType": "counter",
                 "encodings": {"value": {"fieldName": f["name"], "displayName": title, "format": fmt}},
                 "frame": {"title": title, "showTitle": True}}},
        "position": {"x": x, "y": y, "width": w, "height": h}}

BRL = {"type": "number-currency", "currencyCode": "BRL", "abbreviation": "compact",
       "decimalPlaces": {"type": "max", "places": 1}}
INT = {"type": "number-plain", "abbreviation": "compact", "decimalPlaces": {"type": "max", "places": 1}}
PCT = {"type": "number-percent", "decimalPlaces": {"type": "max", "places": 1}}

CANAL_CORES = [{"value": "Google Ads", "color": "#6613D0"}, {"value": "Meta Ads", "color": "#1192E8"},
               {"value": "TikTok Ads", "color": "#9F1853"}, {"value": "ChatGPT Ads", "color": "#10A37F"}]

def chart(name, wtype, dataset, fields, enc, title, x, y, w, h, version=3, disagg=False, extra=None):
    spec = {"version": version, "widgetType": wtype, "encodings": enc,
            "frame": {"title": title, "showTitle": True}}
    if extra:
        spec.update(extra)
    return {"widget": {"name": name, "queries": [{"name": "main_query", "query": {
        "datasetName": dataset, "fields": fields, "disaggregated": disagg}}], "spec": spec},
        "position": {"x": x, "y": y, "width": w, "height": h}}

def table(name, dataset, cols, title, x, y, w, h):
    fields = [{"name": c[0], "expression": f"`{c[0]}`"} for c in cols]
    columns = []
    for c in cols:
        col = {"fieldName": c[0], "displayName": c[1]}
        if len(c) > 2 and c[2]:
            col["format"] = c[2]
        columns.append(col)
    return {"widget": {"name": name, "queries": [{"name": "main_query", "query": {
        "datasetName": dataset, "fields": fields, "disaggregated": True}}],
        "spec": {"version": 2, "widgetType": "table", "encodings": {"columns": columns},
                 "frame": {"title": title, "showTitle": True}}},
        "position": {"x": x, "y": y, "width": w, "height": h}}

W = "DATE_TRUNC(\"WEEK\", `date`)"
p1 = [
    text("h1", "# LogComex · Mídia & Aquisição (dados fictícios)\nDados de **Google Ads, Meta, TikTok, ChatGPT Ads, GA4 e Search Console** gravados pelo **Windsor.ai direto no Unity Catalog** (sem banco intermediário), cruzados com as tabelas de produto para chegar ao **CAC real por canal**.", 0, 0, 12, 2),
    counter("k_inv", "ds_midia", "SUM(`spend`)", "Investimento em mídia", BRL, 0, 2),
    counter("k_leads", "ds_midia", "SUM(`leads`)", "Leads (cadastros)", INT, 3, 2),
    counter("k_cliques", "ds_midia", "SUM(`clicks`)", "Cliques", INT, 6, 2),
    counter("k_cpl", "ds_midia", "SUM(`spend`) / SUM(`leads`)", "CPL médio", BRL, 9, 2),
    chart("c_inv", "bar", "ds_midia",
          [{"name": "weekly(date)", "expression": W}, {"name": "canal", "expression": "`canal`"},
           {"name": "sum(spend)", "expression": "SUM(`spend`)"}],
          {"x": {"fieldName": "weekly(date)", "scale": {"type": "temporal"}, "displayName": "Semana"},
           "y": {"fieldName": "sum(spend)", "scale": {"type": "quantitative"}, "displayName": "Investimento (R$)", "format": BRL},
           "color": {"fieldName": "canal", "scale": {"type": "categorical", "mappings": CANAL_CORES}, "displayName": "Canal"}},
          "Investimento semanal por canal (Windsor.ai)", 0, 5, 6, 6),
    chart("c_cac", "bar", "ds_canal",
          [{"name": "canal", "expression": "`canal`"}, {"name": "cac_brl", "expression": "`cac_brl`"}],
          {"x": {"fieldName": "canal", "scale": {"type": "categorical", "sort": {"by": "custom-order", "orderedValues": ["ChatGPT Ads", "Google Ads", "Meta Ads", "TikTok Ads"]}}, "displayName": "Canal"},
           "y": {"fieldName": "cac_brl", "scale": {"type": "quantitative"}, "displayName": "CAC (R$)", "format": BRL},
           "color": {"fieldName": "canal", "scale": {"type": "categorical", "mappings": CANAL_CORES}, "displayName": "Canal"}},
          "CAC por canal = investimento (Windsor.ai) ÷ clientes pagantes (produto)", 6, 5, 6, 6, disagg=True),
    table("t_funil", "ds_canal", [("canal", "Canal"), ("investimento_brl", "Investimento", BRL), ("leads", "Leads"),
          ("cpl_brl", "CPL", BRL), ("trials", "Trials"), ("taxa_lead_trial", "Lead→Trial", PCT),
          ("clientes_pagantes", "Clientes"), ("taxa_trial_pago", "Trial→Pago", PCT), ("cac_brl", "CAC", BRL),
          ("mrr_brl", "MRR gerado", BRL), ("roi_arr", "ARR / investimento")],
          "Funil completo por canal: mídia → produto", 0, 11, 12, 4),
    chart("c_ga4", "line", "ds_ga4",
          [{"name": "weekly(date)", "expression": W}, {"name": "origem", "expression": "`origem`"},
           {"name": "sum(sessions)", "expression": "SUM(`sessions`)"}],
          {"x": {"fieldName": "weekly(date)", "scale": {"type": "temporal"}, "displayName": "Semana"},
           "y": {"fieldName": "sum(sessions)", "scale": {"type": "quantitative"}, "displayName": "Sessões"},
           "color": {"fieldName": "origem", "scale": {"type": "categorical"}, "displayName": "Origem"}},
          "GA4: sessões por origem (o tráfego vindo do ChatGPT cresce semana a semana)", 0, 15, 7, 6),
    table("t_gsc", "ds_gsc", [("query", "Consulta"), ("cliques", "Cliques"), ("impressoes", "Impressões"),
          ("ctr", "CTR", PCT), ("posicao_media", "Posição")], "Search Console: top consultas (30d)", 7, 15, 5, 6),
]

p2 = [
    text("h2", "# Produto → HubSpot\nMétricas calculadas no Databricks (Lakeflow Job diário) e enviadas ao HubSpot como propriedades da empresa: **health score, risco de churn, PQL, sinal de expansão, canal e CAC**.", 0, 0, 12, 2),
    counter("k_ativos", "ds_contas", "SUM(CASE WHEN `status` = 'ativo' THEN 1 ELSE 0 END)", "Clientes ativos", INT, 0, 2),
    counter("k_risco", "ds_contas", "SUM(CASE WHEN `risco_churn` = 'Alto' THEN 1 ELSE 0 END)", "Clientes com risco alto", INT, 3, 2),
    counter("k_mrr_risco", "ds_contas", "SUM(`mrr_em_risco`)", "MRR em risco", BRL, 6, 2),
    counter("k_pql", "ds_contas", "SUM(`trial_quente`)", "Trials quentes (PQL ≥ 70)", INT, 9, 2),
    chart("c_risco", "bar", "ds_contas",
          [{"name": "risco_churn", "expression": "`risco_churn`"}, {"name": "sum(mrr)", "expression": "SUM(`mrr`)"}],
          {"x": {"fieldName": "risco_churn", "scale": {"type": "categorical", "sort": {"by": "custom-order", "orderedValues": ["Alto", "Médio", "Baixo"]}}, "displayName": "Risco de churn"},
           "y": {"fieldName": "sum(mrr)", "scale": {"type": "quantitative"}, "displayName": "MRR (R$)", "format": BRL},
           "color": {"fieldName": "risco_churn", "scale": {"type": "categorical", "mappings": [
               {"value": "Alto", "color": "#FA4D56"}, {"value": "Médio", "color": "#F5C26B"}, {"value": "Baixo", "color": "#00BDA5"}]}, "displayName": "Risco"}},
          "MRR por risco de churn", 0, 5, 4, 6),
    table("t_risco", "ds_contas", [("name", "Empresa"), ("plano", "Plano"), ("mrr", "MRR", BRL),
          ("health_score", "Health"), ("risco_churn", "Risco"), ("ativos_30d", "Ativos 30d"), ("licencas", "Licenças"),
          ("ultima_atividade", "Última atividade"), ("canal_aquisicao", "Canal")],
          "Contas (filtre por risco no gráfico ao lado)", 4, 5, 8, 6),
    table("t_sync", "ds_sync", [("inicio", "Início"), ("modo", "Destino"), ("lotes", "Lotes"), ("registros", "Registros"),
          ("criados", "Criados"), ("atualizados", "Atualizados"), ("erros", "Erros"), ("retries_429", "Retries 429"),
          ("latencia_media_ms", "Latência média (ms)")],
          "Execuções do sync com o HubSpot fictício (auditoria no UC)", 0, 11, 12, 4),
]

def _montar():
    dash = {"datasets": DATASETS, "pages": [
        {"name": "midia", "displayName": "Mídia & Aquisição", "pageType": "PAGE_TYPE_CANVAS", "layoutVersion": "GRID_V1", "layout": p1},
        {"name": "produto", "displayName": "Produto → HubSpot", "pageType": "PAGE_TYPE_CANVAS", "layoutVersion": "GRID_V1", "layout": p2},
        {"name": "filtros", "displayName": "Filtros", "pageType": "PAGE_TYPE_GLOBAL_FILTERS", "layoutVersion": "GRID_V1", "layout": [
            {"widget": {"name": "f_data", "queries": [
                {"name": "q_midia", "query": {"datasetName": "ds_midia", "fields": [{"name": "date", "expression": "`date`"}], "disaggregated": False}},
                {"name": "q_ga4", "query": {"datasetName": "ds_ga4", "fields": [{"name": "date", "expression": "`date`"}], "disaggregated": False}}],
                "spec": {"version": 2, "widgetType": "filter-date-range-picker", "encodings": {"fields": [
                    {"fieldName": "date", "queryName": "q_midia"}, {"fieldName": "date", "queryName": "q_ga4"}]},
                    "frame": {"showTitle": True, "title": "Período"}}},
             "position": {"x": 0, "y": 0, "width": 4, "height": 2}},
            {"widget": {"name": "f_canal", "queries": [
                {"name": "q_canal", "query": {"datasetName": "ds_midia", "fields": [{"name": "canal", "expression": "`canal`"}], "disaggregated": False}}],
                "spec": {"version": 2, "widgetType": "filter-multi-select", "encodings": {"fields": [
                    {"fieldName": "canal", "queryName": "q_canal"}]}, "frame": {"showTitle": True, "title": "Canal"}}},
             "position": {"x": 4, "y": 0, "width": 4, "height": 2}}]}],
        "uiSettings": {"theme": {
            "canvasBackgroundColor": {"light": "#FAF9F7", "dark": "#0B0D19"},
            "widgetBackgroundColor": {"light": "#FFFFFF", "dark": "#151826"},
            "widgetBorderColor": {"light": "#FFFFFF", "dark": "#151826"},
            "fontColor": {"light": "#0B0D19", "dark": "#ECEAF5"},
            "selectionColor": {"light": "#6613D0", "dark": "#B58CF0"},
            "visualizationColors": ["#6613D0", "#1192E8", "#10A37F", "#F5821F", "#9F1853", "#005D5D", "#FA4D56"],
            "widgetHeaderAlignment": "LEFT", "widgetCornerRadius": 10}}}
    return dash


def definicao(fq: str) -> str:
    """JSON serializado com as tabelas qualificadas pelo schema do workshop."""
    d = _montar()
    for ds in d["datasets"]:
        ds["queryLines"] = [re.sub(r"FROM ([a-z_]+)", lambda m: f"FROM {fq}.`{m.group(1)}`", l) for l in ds["queryLines"]]
    return json.dumps(d, ensure_ascii=False)
