"""Renderiza o HubSpot FICTÍCIO em HTML (estilo CRM) para `displayHTML` no notebook."""
import html
import time

CSS = """
<style>
.crm{font-family:Helvetica,Arial,sans-serif;color:#33475b;font-size:13px}
.crm .top{background:#2d3e50;color:#fff;padding:10px 14px;border-radius:6px 6px 0 0;display:flex;gap:12px;align-items:center}
.crm .dot{width:16px;height:16px;border-radius:50%;background:#ff7a59;display:inline-block}
.crm .sim{margin-left:auto;background:#425b76;padding:2px 8px;border-radius:3px;font-size:11px}
.crm .kpis{display:flex;gap:10px;margin:10px 0}
.crm .kpi{border:1px solid #dfe3eb;border-radius:4px;padding:8px 12px;min-width:130px;background:#fff}
.crm .kpi b{display:block;font-size:20px}
.crm table{border-collapse:collapse;width:100%;background:#fff}
.crm th{background:#f5f8fa;color:#516f90;text-align:left;font-size:11px;text-transform:uppercase;padding:6px 8px;border-bottom:1px solid #dfe3eb}
.crm td{padding:6px 8px;border-bottom:1px solid #eaf0f6;white-space:nowrap}
.crm .b{padding:1px 7px;border-radius:9px;font-weight:600;font-size:11px}
.crm .Alto{background:#fde8e9;color:#c0262f}.crm .Médio{background:#fef3dc;color:#a46b00}.crm .Baixo{background:#e5f8f6;color:#00806f}
.crm .int{background:#efe6fb;color:#6613d0}
.crm .nm{color:#0091ae;font-weight:600}
</style>"""


def _ago(ms):
    if not ms:
        return "–"
    s = time.time() - int(ms) / 1000
    if s < 120:
        return f"há {int(s)} s"
    if s < 7200:
        return f"há {int(s / 60)} min"
    if s < 172800:
        return f"há {int(s / 3600)} h"
    return f"há {int(s / 86400)} d"


def render(empresas: list[dict], titulo="Empresas", limite=25, ordenar="modificadas", destacar: set | None = None):
    e = lambda v: html.escape(str(v if v not in (None, "") else "–"))
    destacar = destacar or set()
    integ = [c for c in empresas if c.get("logcomex_health_score") not in (None, "")]
    kpis = [("Empresas no CRM", len(empresas)), ("Com dados do Databricks", len(integ)),
            ("Risco de churn alto", sum(1 for c in empresas if c.get("logcomex_risco_churn") == "Alto")),
            ("Trials quentes (PQL ≥ 70)", sum(1 for c in empresas if int(c.get("logcomex_pql_score") or 0) >= 70)),
            ("Prioridade CS: urgente", sum(1 for c in empresas if c.get("logcomex_prioridade_cs") == "Urgente"))]
    if ordenar == "risco":
        lst = sorted(empresas, key=lambda c: int(c.get("logcomex_health_score") or 999))
    else:
        lst = sorted(empresas, key=lambda c: -int(c.get("hs_lastmodifieddate") or 0))
    if destacar:
        lst = [c for c in lst if c.get("logcomex_company_id") in destacar] + \
              [c for c in lst if c.get("logcomex_company_id") not in destacar]
    linhas = []
    for c in lst[:limite]:
        risco = c.get("logcomex_risco_churn")
        hl = ' style="background:#fff8e1"' if c.get("logcomex_company_id") in destacar else ""
        badge_risco = f'<span class="b {risco}">{risco}</span>' if risco else "–"
        badge_int = '<span class="b int">Databricks</span> ' if c.get("logcomex_health_score") not in (None, "") else ""
        linhas.append(
            f"<tr{hl}><td><span class='nm'>{e(c.get('name'))}</span><br><small>{e(c.get('logcomex_company_id'))} · {e(c.get('domain'))}</small></td>"
            f"<td>{e(c.get('hubspot_owner_id'))}</td><td>{e(c.get('lifecyclestage'))}</td>"
            f"<td>{e(c.get('num_deals_abertos'))}</td><td>{e(c.get('logcomex_plano'))}</td>"
            f"<td>{e(c.get('logcomex_health_score'))}</td>"
            f"<td>{badge_risco}</td>"
            f"<td>{e(c.get('logcomex_pql_score'))}</td><td>{e(c.get('logcomex_prioridade_cs'))}</td>"
            f"<td>{e(c.get('logcomex_canal_aquisicao'))}</td>"
            f"<td>{badge_int}{_ago(c.get('hs_lastmodifieddate'))}</td></tr>")
    return (CSS + "<div class='crm'><div class='top'><span class='dot'></span><b>HubSpot CRM</b> · " + e(titulo) +
            "<span class='sim'>FICTÍCIO · em memória, dentro do notebook</span></div>"
            "<div class='kpis'>" + "".join(f"<div class='kpi'>{k}<b>{v}</b></div>" for k, v in kpis) + "</div>"
            "<table><tr><th>Empresa</th><th>Owner</th><th>Lifecycle</th><th>Negócios abertos</th><th>Plano</th>"
            "<th>Health</th><th>Risco churn</th><th>PQL</th><th>Prioridade CS</th><th>Canal</th><th>Última modificação</th></tr>"
            + "".join(linhas) + f"</table><small>Mostrando {min(limite, len(lst))} de {len(lst)}</small></div>")


def render_log(chamadas: list[dict]):
    e = html.escape
    def cor(st):
        return "#a46b00" if st == 429 else "#00806f" if st < 300 or st == 207 else "#c0262f"

    linhas = "".join(
        f"<tr><td>{e(c['ts'])}</td><td>{c['metodo']}</td><td><code>{e(c['endpoint'])}</code></td>"
        f"<td style='color:{cor(c['status'])};font-weight:700'>{c['status']}</td>"
        f"<td>{c['registros'] or ''}</td><td>{c['ms']} ms</td></tr>" for c in chamadas[-40:])
    return (CSS + "<div class='crm'><div class='top'><span class='dot'></span><b>HubSpot CRM</b> · Log de API"
            "<span class='sim'>FICTÍCIO</span></div><table><tr><th>Horário UTC</th><th>Método</th><th>Endpoint</th>"
            "<th>Status</th><th>Registros</th><th>Latência</th></tr>" + linhas + "</table></div>")
