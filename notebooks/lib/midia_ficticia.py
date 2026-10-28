"""Gerador determinístico de dados FICTÍCIOS de mídia (o que o Windsor.ai traria de
Google Ads, Meta, TikTok, ChatGPT Ads, GA4 e Search Console). Nenhuma API real é chamada.

O gerador de métricas de mídia é determinístico por (data, campanha): o simulador
do Windsor.ai e o gerador de leads usam a MESMA função, então gasto, cliques e
leads reconciliam entre as tabelas (CPL/CAC batem).
"""
import datetime as dt
import hashlib
import math
import random

JANELA_DIAS = 180

# Campanhas por conector Windsor.ai. Valores diários base em BRL.
# (datasource, account, campaign_id, campaign, ad_group, budget, cpm, ctr, cvr, inicio_offset_dias)
#   inicio_offset_dias = dias atrás em que a campanha começou (None = toda a janela)
CAMPANHAS = [
    ("google_ads", "[FICTÍCIO] Google Ads", "ga-1001", "GA | Search | Dados de Importação", "Importadores por NCM", 620, 95, 0.062, 0.052, None),
    ("google_ads", "[FICTÍCIO] Google Ads", "ga-1002", "GA | Search | Consulta NCM", "Classificação fiscal", 410, 80, 0.071, 0.041, None),
    ("google_ads", "[FICTÍCIO] Google Ads", "ga-1003", "GA | Search | Concorrentes", "Marcas concorrentes", 280, 120, 0.048, 0.035, None),
    ("google_ads", "[FICTÍCIO] Google Ads", "ga-1004", "GA | PMax | Comex Inteligente", "Performance Max", 520, 38, 0.014, 0.030, 120),
    ("facebook", "[FICTÍCIO] Meta Ads", "fb-2001", "META | Leadgen | Radar Comex", "Lookalike clientes", 480, 28, 0.011, 0.060, None),
    ("facebook", "[FICTÍCIO] Meta Ads", "fb-2002", "META | Retargeting | Trial", "Visitantes 30d", 210, 45, 0.016, 0.075, None),
    ("facebook", "[FICTÍCIO] Meta Ads", "fb-2003", "META | Awareness | LogComex.AI", "Interesses comex", 330, 14, 0.006, 0.012, 90),
    ("tiktok", "[FICTÍCIO] TikTok Ads", "tt-3001", "TT | Awareness | Comex para PMEs", "PMEs importadoras", 300, 9, 0.009, 0.020, 150),
    ("tiktok", "[FICTÍCIO] TikTok Ads", "tt-3002", "TT | Video | LogComex.AI em 60s", "Spark Ads", 240, 11, 0.012, 0.024, 100),
    ("chatgpt_ads", "[FICTÍCIO] ChatGPT Ads", "cg-4001", "CHATGPT | Sponsored | Pesquisa de Importadores", "Intenção: fornecedores", 260, 70, 0.038, 0.095, 84),
    ("chatgpt_ads", "[FICTÍCIO] ChatGPT Ads", "cg-4002", "CHATGPT | Sponsored | Classificação NCM", "Intenção: NCM", 190, 65, 0.041, 0.088, 70),
]

# Probabilidade lead -> trial e trial -> pago por canal (história da demo:
# ChatGPT converte melhor; TikTok gera clique barato mas lead fraco).
FUNIL = {
    "google_ads": (0.20, 0.34),
    "facebook": (0.12, 0.22),
    "tiktok": (0.05, 0.10),
    "chatgpt_ads": (0.27, 0.42),
}

TABELA_WINDSOR = {
    "google_ads": "windsor_google_ads",
    "facebook": "windsor_facebook_ads",
    "tiktok": "windsor_tiktok_ads",
    "chatgpt_ads": "windsor_chatgpt_ads",
}

UTM_SOURCE = {"google_ads": "google", "facebook": "facebook",
              "tiktok": "tiktok", "chatgpt_ads": "chatgpt.com"}
UTM_MEDIUM = {"google_ads": "cpc", "facebook": "paid_social",
              "tiktok": "paid_social", "chatgpt_ads": "sponsored"}


def hoje() -> dt.date:
    return dt.date.today()


def janela(dias: int = JANELA_DIAS) -> list[dt.date]:
    """Datas de (hoje - dias) até ontem."""
    fim = hoje() - dt.timedelta(days=1)
    return [fim - dt.timedelta(days=i) for i in range(dias - 1, -1, -1)]


def rng_for(*keys) -> random.Random:
    h = hashlib.sha256("|".join(map(str, keys)).encode()).hexdigest()
    return random.Random(int(h[:16], 16))


def poisson(r: random.Random, lam: float) -> int:
    if lam <= 0:
        return 0
    if lam > 40:
        return max(0, int(round(r.gauss(lam, math.sqrt(lam)))))
    L, k, p = math.exp(-lam), 0, 1.0
    while True:
        p *= r.random()
        if p <= L:
            return k
        k += 1


def metricas_campanha(d: dt.date, camp: tuple) -> dict | None:
    """Métricas diárias determinísticas de uma campanha (o que o Windsor.ai
    traria da API da plataforma de mídia)."""
    (ds, account, cid, cname, adg, budget, cpm, ctr, cvr, inicio) = camp
    dias_atras = (hoje() - d).days
    if inicio is not None and dias_atras > inicio:
        return None
    r = rng_for("midia", cid, d.isoformat())
    fator_semana = 0.55 if d.weekday() >= 5 else 1.0
    rampa = 1.0
    if inicio is not None:  # campanhas novas escalam budget nas 3 primeiras semanas
        rampa = min(1.0, 0.35 + 0.65 * (inicio - dias_atras) / 21)
    if ds == "chatgpt_ads":  # canal em crescimento: budget sobe ao longo do tempo
        rampa *= 1.0 + 0.9 * max(0, (inicio - dias_atras)) / inicio
    spend = budget * fator_semana * rampa * r.uniform(0.85, 1.12)
    impressions = int(spend / cpm * 1000 * r.uniform(0.9, 1.1))
    clicks = int(impressions * ctr * r.uniform(0.85, 1.15))
    conversions = poisson(r, clicks * cvr * 0.2)  # cvr de referência x 0.2 = lead qualificado (cadastro)
    return {
        "date": d.isoformat(),
        "datasource": ds,
        "account_name": account,
        "campaign_id": cid,
        "campaign": cname,
        "ad_group": adg,
        "impressions": impressions,
        "clicks": clicks,
        "spend": round(spend, 2),
        "conversions": conversions,
        "ctr": round(clicks / impressions, 5) if impressions else 0.0,
        "cpc": round(spend / clicks, 4) if clicks else 0.0,
        "currency": "BRL",
    }


# GA4 — sessões por source/medium (inclui orgânico e referral do chatgpt.com).
GA4_FONTES = [
    ("google / organic", 380, 0.020), ("(direct) / (none)", 260, 0.015),
    ("chatgpt.com / referral", 40, 0.045), ("perplexity.ai / referral", 12, 0.030),
    ("linkedin.com / referral", 55, 0.012), ("newsletter / email", 70, 0.025),
]
LANDING_PAGES = ["/", "/dados-de-importacao", "/consulta-ncm", "/logcomex-ai",
                 "/planos", "/blog/como-encontrar-fornecedores-na-china",
                 "/blog/ncm-o-que-e"]

GSC_QUERIES = [
    ("dados de importação", "/dados-de-importacao", 1900, 4.1),
    ("consulta ncm", "/consulta-ncm", 2600, 6.3),
    ("como descobrir quem importa um produto", "/dados-de-importacao", 900, 3.2),
    ("logcomex", "/", 1400, 1.1),
    ("radar comex", "/", 300, 2.4),
    ("ncm o que é", "/blog/ncm-o-que-e", 3100, 8.7),
    ("fornecedores na china", "/blog/como-encontrar-fornecedores-na-china", 2200, 11.5),
    ("inteligência de comércio exterior", "/logcomex-ai", 650, 5.6),
    ("ia para comex", "/logcomex-ai", 420, 4.8),
    ("preço logcomex", "/planos", 260, 2.0),
]
