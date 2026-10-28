"""Gerador determinístico de dados FICTÍCIOS de produto (contas, usuários, uso diário,
leads). Todas as empresas, CNPJs, e-mails e pessoas são inventados.

Uso:  dados = gerar()  ->  dict com listas de dicts por tabela.
"""
import datetime as dt
from .midia_ficticia import (CAMPANHAS, FUNIL, UTM_SOURCE, UTM_MEDIUM, janela, hoje,
                             metricas_campanha, rng_for)

usados = set()

PREFIXOS = ["Atlântica", "Paraná", "Sul", "Nova", "Porto", "Rio", "Vale", "Serra", "Horizonte",
            "Global", "Prime", "Delta", "Alfa", "Brasil", "Iguaçu", "Santos", "Itajaí", "Litoral",
            "Mercosul", "Andina", "Pampa", "Cerrado", "Amazônia", "Planalto", "Oceano", "Âncora",
            "Rota", "Trilha", "Bússola", "Estrela"]
NUCLEOS = ["Trading", "Importadora", "Comércio Exterior", "Logística", "Química", "Têxtil",
           "Agro", "Autopeças", "Eletrônicos", "Máquinas", "Alimentos", "Farma", "Metais",
           "Plásticos", "Despachos Aduaneiros", "Cargas", "Distribuidora", "Componentes"]
SUFIXOS = ["Ltda", "S.A.", "Ltda", "Ltda", "EIRELI", "S.A."]
SEGMENTOS = {"Trading": "Trading", "Importadora": "Importador", "Comércio Exterior": "Trading",
             "Logística": "Agente de carga", "Cargas": "Agente de carga",
             "Despachos Aduaneiros": "Despachante aduaneiro", "Distribuidora": "Importador"}
UFS = ["PR", "SP", "SC", "RS", "RJ", "MG", "ES", "BA", "PE", "GO", "AM"]
CIDADES = {"PR": "Curitiba", "SP": "São Paulo", "SC": "Itajaí", "RS": "Porto Alegre",
           "RJ": "Rio de Janeiro", "MG": "Belo Horizonte", "ES": "Vitória", "BA": "Salvador",
           "PE": "Recife", "GO": "Goiânia", "AM": "Manaus"}
PRIMEIROS = ["Ana", "Bruno", "Carla", "Diego", "Eduarda", "Felipe", "Gabriela", "Henrique", "Isabela",
             "João", "Karina", "Lucas", "Mariana", "Nicolas", "Olívia", "Pedro", "Rafaela", "Samuel",
             "Tatiane", "Vinícius", "Larissa", "Marcelo", "Fernanda", "Rodrigo", "Juliana", "Thiago"]
SOBRENOMES = ["Silva", "Souza", "Oliveira", "Pereira", "Costa", "Rodrigues", "Almeida", "Nascimento",
              "Lima", "Araújo", "Fernandes", "Carvalho", "Gomes", "Martins", "Rocha", "Ribeiro"]
CARGOS = ["Analista de Comex", "Coordenador de Importação", "Gerente de Supply", "Comprador",
          "Diretor de Operações", "Analista de Inteligência de Mercado", "Despachante", "CEO"]



def nome_empresa(r):
    for tentativa in range(200):
        nucleo = r.choice(NUCLEOS)
        base = f"{r.choice(PREFIXOS)} {nucleo}"
        if tentativa > 50:  # espaço de combinações apertado -> diferencia com um sufixo regional
            base += f" {r.choice(['do Sul', 'do Norte', 'Brasil', 'Internacional', 'Group', 'Express'])}"
        nome = f"{base} {r.choice(SUFIXOS)}"
        if nome not in usados:
            usados.add(nome)
            return nome, SEGMENTOS.get(nucleo, "Indústria")
    nome = f"{base} {len(usados)} {r.choice(SUFIXOS)}"
    usados.add(nome)
    return nome, SEGMENTOS.get(nucleo, "Indústria")


def slug(s):
    import unicodedata
    s = unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode().lower()
    return "".join(ch for ch in s if ch.isalnum())


def cnpj(r):
    """CNPJ FICTÍCIO com dígitos verificadores propositalmente inválidos (nunca coincide com empresa real)."""
    n = "99" + "".join(str(r.randint(0, 9)) for _ in range(6))
    return f"{n[:2]}.{n[2:5]}.{n[5:8]}/0001-00"


def gerar():
    usados.clear()
    datas = janela()
    leads = []
    lid = 0
    for d in datas:
        for camp in CAMPANHAS:
            m = metricas_campanha(d, camp)
            if not m:
                continue
            for k in range(m["conversions"]):
                lid += 1
                leads.append({"lead_id": f"LD-{lid:06d}", "data_cadastro": d, "datasource": camp[0],
                              "utm_source": UTM_SOURCE[camp[0]], "utm_medium": UTM_MEDIUM[camp[0]],
                              "utm_campaign": camp[3], "campaign_id": camp[2]})
    print("leads pagos:", len(leads))

    PLANOS = {"Starter": (1490, 3), "Pro": (3990, 8), "Enterprise": (11900, 25)}
    empresas, usuarios = [], []
    cid = 0


    def nova_empresa(r, origem, data_inicio, status, plano, canal, campanha, lead_id=None):
        nonlocal cid
        cid += 1
        nome, segmento = nome_empresa(r)
        uf = r.choice(UFS)
        mrr, lic = PLANOS.get(plano, (0, 3))
        if plano == "Enterprise":
            lic = r.randint(15, 40)
        elif plano == "Pro":
            lic = r.randint(5, 12)
        elif plano == "Starter":
            lic = r.randint(2, 4)
        e = {"company_id": f"FIC-{cid:05d}", "razao_social": nome, "cnpj": cnpj(r),
             "dominio": f"{slug(nome.split(' ')[0] + nome.split(' ')[1])}-{cid}.example",  # TLD reservado (RFC 2606)
             "segmento": segmento, "uf": uf, "cidade": CIDADES[uf],
             "porte": {"Starter": "PME", "Pro": "Médio", "Enterprise": "Grande"}.get(plano, "PME"),
             "plano": plano, "status": status, "mrr_brl": float(mrr if status == "ativo" else 0),
             "licencas": lic, "data_inicio": data_inicio, "data_cancelamento": None,
             "canal_aquisicao": canal, "campanha_aquisicao": campanha, "lead_id": lead_id,
             "perfil_uso": None}
        empresas.append(e)
        return e


    r0 = rng_for("legado")
    for i in range(300):
        inicio = hoje() - dt.timedelta(days=r0.randint(200, 1500))
        plano = r0.choices(["Starter", "Pro", "Enterprise"], [0.45, 0.4, 0.15])[0]
        canal = r0.choices(["Outbound", "Indicação", "Orgânico", "Eventos"], [0.4, 0.2, 0.3, 0.1])[0]
        nova_empresa(r0, "legado", inicio, "ativo", plano, canal, None)

    for ld in leads:
        r = rng_for("lead", ld["lead_id"])
        p_trial, p_pago = FUNIL[ld["datasource"]]
        if r.random() > p_trial:
            continue
        inicio = ld["data_cadastro"] + dt.timedelta(days=r.randint(0, 3))
        if inicio >= hoje():
            continue
        dias = (hoje() - inicio).days
        if dias <= 14:
            status, plano = "trial", "Trial"
        elif r.random() < p_pago:
            status = "ativo"
            plano = r.choices(["Starter", "Pro", "Enterprise"], [0.55, 0.37, 0.08])[0]
        else:
            status, plano = "trial_expirado", "Trial"
        nova_empresa(r, "lead", inicio, status, plano, ld["datasource"], ld["utm_campaign"], ld["lead_id"])

    # cancelamentos no legado (~6%)
    for e in empresas:
        r = rng_for("perfil", e["company_id"])
        if e["status"] == "ativo" and e["canal_aquisicao"] in ("Outbound", "Indicação", "Orgânico", "Eventos") and r.random() < 0.06:
            e["status"] = "cancelado"
            e["data_cancelamento"] = hoje() - dt.timedelta(days=r.randint(10, 160))
            e["mrr_brl"] = 0.0
        # perfil de uso -> história de churn / expansão / PQL
        if e["status"] == "ativo":
            e["perfil_uso"] = r.choices(["saudavel", "crescendo", "declinando", "quase_parado"],
                                        [0.66, 0.14, 0.15, 0.05])[0]
        elif e["status"] == "trial":
            e["perfil_uso"] = r.choices(["trial_quente", "trial_morno", "trial_frio"], [0.3, 0.4, 0.3])[0]
        elif e["status"] == "trial_expirado":
            e["perfil_uso"] = "trial_frio"
        else:
            e["perfil_uso"] = "cancelado"

    print("empresas:", len(empresas), {s: sum(1 for e in empresas if e["status"] == s) for s in {e["status"] for e in empresas}})

    uid = 0
    for e in empresas:
        r = rng_for("usuarios", e["company_id"])
        n = max(1, int(e["licencas"] * r.uniform(0.6, 1.0))) if e["plano"] != "Trial" else r.randint(1, 3)
        for k in range(n):
            uid += 1
            pn, sn = r.choice(PRIMEIROS), r.choice(SOBRENOMES)
            usuarios.append({"user_id": f"USR-{uid:06d}", "company_id": e["company_id"],
                             "nome": f"{pn} {sn}", "email": f"{slug(pn)}.{slug(sn)}{k}@{e['dominio']}",
                             "cargo": r.choice(CARGOS), "papel": "admin" if k == 0 else "membro",
                             "data_criacao": e["data_inicio"] + dt.timedelta(days=r.randint(0, 30) if k else 0)})
    print("usuários:", len(usuarios))

    n_users = {}
    for u in usuarios:
        n_users[u["company_id"]] = n_users.get(u["company_id"], 0) + 1
    uso = []
    for e in empresas:
        r = rng_for("uso", e["company_id"])
        nu = n_users.get(e["company_id"], 1)
        base = {"Starter": 0.55, "Pro": 0.6, "Enterprise": 0.65, "Trial": 0.5}[e["plano"]]
        perfil = e["perfil_uso"]
        queda_ini = r.randint(25, 60)  # dias atrás em que começou a queda
        ia_afinidade = r.uniform(0.2, 1.4)
        for d in datas:
            if d < e["data_inicio"]:
                continue
            dias_atras = (hoje() - d).days
            if e["data_cancelamento"] and d >= e["data_cancelamento"]:
                continue
            if e["status"] == "trial_expirado" and d > e["data_inicio"] + dt.timedelta(days=14):
                continue
            fator = 1.0
            if perfil == "crescendo":
                fator = 0.7 + 0.8 * (1 - dias_atras / len(datas))
            elif perfil == "declinando" and dias_atras < queda_ini:
                fator = max(0.08, dias_atras / queda_ini)
            elif perfil == "quase_parado":
                fator = 0.03 if dias_atras < 45 else 0.5
            elif perfil == "trial_quente":
                fator = 1.6
            elif perfil == "trial_morno":
                fator = 0.6
            elif perfil == "trial_frio":
                fator = 0.12
            if e["data_cancelamento"]:
                fator *= 0.4
            if d.weekday() >= 5:
                fator *= 0.15
            ativos = sum(1 for _ in range(nu) if r.random() < min(0.97, base * fator))
            if ativos == 0:
                continue
            # LogComex.AI cresce em adoção nos últimos 90 dias
            ia_trend = 0.3 + 0.7 * max(0, 1 - dias_atras / 90)
            uso.append({
                "data": d, "company_id": e["company_id"], "usuarios_ativos": ativos,
                "logins": ativos + int(r.random() * ativos),
                "buscas_importacao": int(ativos * r.uniform(3, 11)),
                "consultas_ncm": int(ativos * r.uniform(1, 7)),
                "relatorios_exportados": int(ativos * r.uniform(0, 1.8)),
                "perguntas_logcomex_ai": int(ativos * r.uniform(0, 4) * ia_afinidade * ia_trend),
            })
    print("linhas de uso:", len(uso))


    return {"empresas": empresas, "usuarios": usuarios, "uso": uso, "leads": leads}
