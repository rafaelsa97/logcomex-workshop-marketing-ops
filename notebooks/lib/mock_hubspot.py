"""HubSpot CRM FICTÍCIO, executado em memória dentro do notebook.

* Implementa o subconjunto da API v3 do HubSpot usado no workshop:
    GET  /crm/v3/properties/companies           POST /crm/v3/properties/companies
    POST /crm/v3/properties/companies/groups
    POST /crm/v3/objects/companies/batch/upsert  POST /crm/v3/objects/companies/search
    GET  /crm/v3/objects/companies
* Acoplado ao `requests` por um *transport adapter* em memória: o cliente faz chamadas HTTP normais
  para `http://hubspot-ficticio.local`, mas nenhum byte trafega pela rede (não há socket, nem conexão
  com o HubSpot real).
* Estado persistido num arquivo SQLite dentro do Volume do schema do workshop
  (copiado para /tmp ao iniciar e de volta ao Volume ao parar).
* Só usa a biblioteca padrão do Python.
"""
import datetime as dt
import json
import os
import shutil
import sqlite3
import tempfile
import threading
import time
from collections import deque
from urllib.parse import parse_qs, urlparse

from requests.adapters import BaseAdapter
from requests.models import Response

from .midia_ficticia import rng_for

BASE_URL = "http://hubspot-ficticio.local"

DONOS = ["Marina Costa (Vendas)", "Rafael Lima (Vendas)", "Beatriz Souza (CS)",
         "Gustavo Rocha (CS)", "Camila Martins (Vendas)", "André Nunes (CS)"]

# Propriedades "nativas" + as que o time comercial mantém no CRM (fictício)
PROPS_PADRAO = [
    ("name", "Company name", "string", "text", "companyinformation", False),
    ("domain", "Company domain name", "string", "text", "companyinformation", False),
    ("city", "City", "string", "text", "companyinformation", False),
    ("state", "State/Region", "string", "text", "companyinformation", False),
    ("industry", "Industry", "string", "text", "companyinformation", False),
    ("hubspot_owner_id", "Company owner", "string", "text", "companyinformation", False),
    ("lifecyclestage", "Lifecycle stage", "enumeration", "select", "companyinformation", False),
    ("num_deals_abertos", "Negócios abertos", "number", "number", "sales_properties", False),
    ("valor_pipeline_brl", "Valor em pipeline (R$)", "number", "number", "sales_properties", False),
    ("data_ultimo_contato", "Último contato comercial", "date", "date", "sales_properties", False),
    ("hs_lastmodifieddate", "Last modified date", "datetime", "date", "companyinformation", False),
    ("createdate", "Create date", "datetime", "date", "companyinformation", False),
    ("logcomex_company_id", "ID da conta (plataforma)", "string", "text", "companyinformation", True),
]


def _agora_ms() -> str:
    return str(int(time.time() * 1000))


def _iso(ms: str) -> str:
    return dt.datetime.utcfromtimestamp(int(ms) / 1000).isoformat(timespec="milliseconds") + "Z"


class MockHubSpot:
    def __init__(self, arquivo_volume: str, rate_limit: bool = False, max_req: int = 2, janela_s: int = 10):
        self.arquivo_volume = arquivo_volume
        self.db_local = None
        self.rate_limit, self.max_req, self.janela_s = rate_limit, max_req, janela_s
        self._bucket = deque()
        self.log = []
        self.lock = threading.Lock()
        self.base_url = None

    # ------------------------------------------------------------------ ciclo de vida
    def start(self):
        # cópia de trabalho num diretório temporário exclusivo desta execução
        self.db_local = os.path.join(tempfile.mkdtemp(prefix="mock_hubspot_"), "crm.db")
        if os.path.exists(self.arquivo_volume):
            shutil.copyfile(self.arquivo_volume, self.db_local)
        self._init_db()
        self.base_url = BASE_URL
        return self

    def adapter(self):
        """Transport adapter do `requests` que entrega cada requisição direto ao CRM fictício."""
        return _AdapterMemoria(self)

    def stop(self):
        shutil.copyfile(self.db_local, self.arquivo_volume)
        shutil.rmtree(os.path.dirname(self.db_local), ignore_errors=True)

    def __enter__(self):
        return self.start()

    def __exit__(self, *a):
        self.stop()

    # ------------------------------------------------------------------ banco
    def _con(self):
        c = sqlite3.connect(self.db_local, check_same_thread=False)
        c.row_factory = sqlite3.Row
        return c

    def _init_db(self):
        with self.lock, self._con() as c:
            c.executescript("""
            CREATE TABLE IF NOT EXISTS props (name TEXT PRIMARY KEY, label TEXT, type TEXT, fieldType TEXT,
                groupName TEXT, options TEXT, hasUniqueValue INT, description TEXT, createdAt TEXT);
            CREATE TABLE IF NOT EXISTS groups (name TEXT PRIMARY KEY, label TEXT);
            CREATE TABLE IF NOT EXISTS companies (id INTEGER PRIMARY KEY AUTOINCREMENT, properties TEXT);
            CREATE TABLE IF NOT EXISTS history (company_id INT, prop TEXT, value TEXT, ts TEXT, source TEXT);
            """)
            if not c.execute("SELECT 1 FROM props LIMIT 1").fetchone():
                for n, l, t, f, g, u in PROPS_PADRAO:
                    c.execute("INSERT INTO props VALUES (?,?,?,?,?,?,?,?,?)",
                              (n, l, t, f, g, "[]", int(u), "Propriedade padrão (fictícia)", _agora_ms()))
                c.executemany("INSERT OR IGNORE INTO groups VALUES (?,?)",
                              [("companyinformation", "Company information"), ("sales_properties", "Sales")])

    def semear(self, empresas: list[dict]):
        """Empresas que o time comercial (fictício) já tinha cadastrado no CRM."""
        with self.lock, self._con() as c:
            if c.execute("SELECT count(*) FROM companies").fetchone()[0]:
                return 0
            n = 0
            for i, e in enumerate(empresas):
                if e["status"] not in ("ativo", "trial", "cancelado") or i % 10 == 9:
                    continue  # ~10% das contas ainda não estão no CRM
                r = rng_for("crm", e["company_id"])
                ms = str(int((time.time() - 86400 * r.randint(20, 400)) * 1000))
                deals = r.choices([0, 1, 2], [0.6, 0.3, 0.1])[0]
                p = {"name": e["razao_social"], "domain": e["dominio"], "city": e["cidade"], "state": e["uf"],
                     "industry": e["segmento"], "hubspot_owner_id": DONOS[i % len(DONOS)],
                     "lifecyclestage": {"ativo": "customer", "trial": "opportunity", "cancelado": "other"}[e["status"]],
                     "num_deals_abertos": str(deals),
                     "valor_pipeline_brl": str(deals * r.choice([9000, 18000, 36000, 72000])),
                     "data_ultimo_contato": (dt.date.today() - dt.timedelta(days=r.randint(1, 120))).isoformat(),
                     "logcomex_company_id": e["company_id"], "createdate": ms, "hs_lastmodifieddate": ms}
                c.execute("INSERT INTO companies (properties) VALUES (?)", (json.dumps(p, ensure_ascii=False),))
                n += 1
            return n

    def simular_atividade_comercial(self, n: int = 12, semente: str = "rodada"):
        """O time comercial edita algumas empresas (abre negócios) -> mudam o hs_lastmodifieddate."""
        with self.lock, self._con() as c:
            rows = c.execute("SELECT id, properties FROM companies ORDER BY id").fetchall()
            r = rng_for("atividade", semente)
            escolhidas = r.sample(rows, min(n, len(rows)))
            for row in escolhidas:
                p = json.loads(row["properties"])
                p["num_deals_abertos"] = str(int(p.get("num_deals_abertos") or 0) + 1)
                p["valor_pipeline_brl"] = str(int(float(p.get("valor_pipeline_brl") or 0)) + 36000)
                p["data_ultimo_contato"] = dt.date.today().isoformat()
                p["hs_lastmodifieddate"] = _agora_ms()
                c.execute("UPDATE companies SET properties=? WHERE id=?", (json.dumps(p, ensure_ascii=False), row["id"]))
                c.execute("INSERT INTO history VALUES (?,?,?,?,?)",
                          (row["id"], "num_deals_abertos", p["num_deals_abertos"], _agora_ms(), "USUÁRIO · time comercial"))
            return [json.loads(x["properties"])["name"] for x in escolhidas]

    def listar(self) -> list[dict]:
        with self._con() as c:
            return [{"id": r["id"], **json.loads(r["properties"])} for r in c.execute("SELECT * FROM companies")]

    def propriedades(self) -> list[dict]:
        with self._con() as c:
            return [dict(r) for r in c.execute("SELECT * FROM props ORDER BY groupName, name")]

    def historico(self, company_id: int) -> list[dict]:
        with self._con() as c:
            return [dict(r) for r in c.execute(
                "SELECT prop, value, ts, source FROM history WHERE company_id=? ORDER BY ts DESC", (company_id,))]

    # ------------------------------------------------------------------ API
    def _rotear(self, metodo, caminho, query, corpo):
        if self.rate_limit:
            t = time.time()
            while self._bucket and t - self._bucket[0] > self.janela_s:
                self._bucket.popleft()
            if len(self._bucket) >= self.max_req:
                return 429, {"status": "error", "message": "You have reached your ten_secondly_rolling limit.",
                             "errorType": "RATE_LIMIT", "policyName": "TEN_SECONDLY_ROLLING"}, {"Retry-After": "3"}
            self._bucket.append(t)
        rotas = {
            ("GET", "/crm/v3/properties/companies"): self._get_props,
            ("POST", "/crm/v3/properties/companies"): self._post_prop,
            ("POST", "/crm/v3/properties/companies/groups"): self._post_group,
            ("POST", "/crm/v3/objects/companies/batch/upsert"): self._upsert,
            ("POST", "/crm/v3/objects/companies/search"): self._search,
            ("GET", "/crm/v3/objects/companies"): self._list,
        }
        f = rotas.get((metodo, caminho))
        if not f:
            return 404, {"status": "error", "category": "OBJECT_NOT_FOUND", "message": f"{metodo} {caminho}"}, None
        status, payload = f(query, corpo)
        return status, payload, None

    def _prop_out(self, r):
        return {"name": r["name"], "label": r["label"], "type": r["type"], "fieldType": r["fieldType"],
                "groupName": r["groupName"], "options": json.loads(r["options"] or "[]"),
                "hasUniqueValue": bool(r["hasUniqueValue"]), "description": r["description"]}

    def _get_props(self, q, b):
        with self._con() as c:
            return 200, {"results": [self._prop_out(r) for r in c.execute("SELECT * FROM props")]}

    def _post_group(self, q, b):
        with self.lock, self._con() as c:
            if c.execute("SELECT 1 FROM groups WHERE name=?", (b["name"],)).fetchone():
                return 409, {"status": "error", "category": "CONFLICT", "message": "Group already exists"}
            c.execute("INSERT INTO groups VALUES (?,?)", (b["name"], b.get("label")))
            return 201, {"name": b["name"], "label": b.get("label")}

    def _post_prop(self, q, b):
        with self.lock, self._con() as c:
            if c.execute("SELECT 1 FROM props WHERE name=?", (b["name"],)).fetchone():
                return 409, {"status": "error", "category": "OBJECT_ALREADY_EXISTS", "message": "Property exists"}
            c.execute("INSERT INTO props VALUES (?,?,?,?,?,?,?,?,?)",
                      (b["name"], b.get("label"), b.get("type"), b.get("fieldType"), b.get("groupName"),
                       json.dumps(b.get("options", []), ensure_ascii=False), int(bool(b.get("hasUniqueValue"))),
                       b.get("description", ""), _agora_ms()))
            return 201, self._prop_out(c.execute("SELECT * FROM props WHERE name=?", (b["name"],)).fetchone())

    def _out(self, row, props=None):
        p = json.loads(row["properties"])
        if props:
            p = {k: p.get(k) for k in props}
        p["hs_object_id"] = str(row["id"])
        full = json.loads(row["properties"])
        return {"id": str(row["id"]), "properties": p, "createdAt": _iso(full.get("createdate", _agora_ms())),
                "updatedAt": _iso(full.get("hs_lastmodifieddate", _agora_ms())), "archived": False}

    def _upsert(self, q, b):
        inputs = (b or {}).get("inputs", [])
        if len(inputs) > 100:
            return 400, {"status": "error", "category": "VALIDATION_ERROR", "message": "Batch size must be <= 100"}
        resultados, erros = [], []
        with self.lock, self._con() as c:
            conhecidas = {r["name"]: r for r in c.execute("SELECT * FROM props")}
            indice = {}
            for inp in inputs:
                idp, chave, props = inp.get("idProperty"), str(inp.get("id")), inp.get("properties", {})
                if idp not in conhecidas or not conhecidas[idp]["hasUniqueValue"]:
                    erros.append({"status": "error", "category": "VALIDATION_ERROR",
                                  "message": f"Property {idp} is not a unique identifier", "context": {"id": [chave]}})
                    continue
                invalidas = [k for k in props if k not in conhecidas]
                for k, v in props.items():
                    opts = json.loads(conhecidas[k]["options"] or "[]") if k in conhecidas else []
                    if opts and v not in ("", None) and v not in [o["value"] for o in opts]:
                        invalidas.append(k)
                if invalidas:
                    erros.append({"status": "error", "category": "VALIDATION_ERROR",
                                  "message": f"Property values were not valid: {invalidas}", "context": {"id": [chave]}})
                    continue
                if idp not in indice:
                    indice[idp] = {json.loads(r["properties"]).get(idp): r["id"]
                                   for r in c.execute("SELECT * FROM companies")}
                rid = indice[idp].get(chave)
                row = c.execute("SELECT * FROM companies WHERE id=?", (rid,)).fetchone() if rid else None
                ts = _agora_ms()
                if row is None:
                    novo = {**{k: v for k, v in props.items() if v != ""}, idp: chave, "createdate": ts,
                            "hs_lastmodifieddate": ts}
                    cid = c.execute("INSERT INTO companies (properties) VALUES (?)",
                                    (json.dumps(novo, ensure_ascii=False),)).lastrowid
                    mudou, novo_reg = {k: v for k, v in props.items() if v != ""}, True
                    indice[idp][chave] = cid
                else:
                    atual = json.loads(row["properties"])
                    mudou = {k: v for k, v in props.items() if (atual.get(k) or "") != (v or "")}
                    for k, v in props.items():
                        if v == "":
                            atual.pop(k, None)
                        else:
                            atual[k] = v
                    if mudou:
                        atual["hs_lastmodifieddate"] = ts
                    c.execute("UPDATE companies SET properties=? WHERE id=?",
                              (json.dumps(atual, ensure_ascii=False), row["id"]))
                    cid, novo_reg = row["id"], False
                for k, v in mudou.items():
                    c.execute("INSERT INTO history VALUES (?,?,?,?,?)", (cid, k, v, ts, "INTEGRAÇÃO · Databricks"))
                o = self._out(c.execute("SELECT * FROM companies WHERE id=?", (cid,)).fetchone())
                o["new"] = novo_reg
                resultados.append(o)
        corpo = {"status": "COMPLETE", "results": resultados}
        if erros:
            corpo.update(errors=erros, numErrors=len(erros))
            return 207, corpo
        return 200, corpo

    def _search(self, q, b):
        b = b or {}
        limite = min(int(b.get("limit", 100)), 200)
        depois = int(b.get("after", 0) or 0)
        filtros = [f for g in b.get("filterGroups", []) for f in g.get("filters", [])]
        props = b.get("properties") or None
        with self._con() as c:
            rows = c.execute("SELECT * FROM companies").fetchall()

        def ok(row):
            p = json.loads(row["properties"])
            for f in filtros:
                v = p.get(f["propertyName"])
                alvo = f.get("value")
                if f["operator"] in ("GTE", "GT") and f["propertyName"] in ("hs_lastmodifieddate", "createdate"):
                    if v is None or (int(v) < int(alvo) if f["operator"] == "GTE" else int(v) <= int(alvo)):
                        return False
                elif f["operator"] == "EQ" and str(v) != str(alvo):
                    return False
            return True

        sel = sorted([r for r in rows if ok(r)],
                     key=lambda r: (int(json.loads(r["properties"]).get("hs_lastmodifieddate", 0)), r["id"]))
        pagina = sel[depois:depois + limite]
        out = {"total": len(sel), "results": [self._out(r, props) for r in pagina]}
        if depois + limite < len(sel):
            out["paging"] = {"next": {"after": str(depois + limite)}}
        return 200, out

    def _list(self, q, b):
        limite = min(int(q.get("limit", ["10"])[0]), 100)
        depois = int(q.get("after", ["0"])[0])
        with self._con() as c:
            rows = c.execute("SELECT * FROM companies WHERE id > ? ORDER BY id LIMIT ?", (depois, limite)).fetchall()
        out = {"results": [self._out(r) for r in rows]}
        if len(rows) == limite:
            out["paging"] = {"next": {"after": str(rows[-1]["id"])}}
        return 200, out


class _AdapterMemoria(BaseAdapter):
    def __init__(self, mock: MockHubSpot):
        super().__init__()
        self.mock = mock

    def send(self, request, **kwargs):
        t0 = time.time()
        url = urlparse(request.url)
        if url.hostname != "hubspot-ficticio.local":
            raise PermissionError(f"Bloqueado: {url.hostname}")
        corpo = request.body.decode() if isinstance(request.body, bytes) else request.body
        corpo = json.loads(corpo) if corpo else None
        status, payload, headers = self.mock._rotear(request.method, url.path, parse_qs(url.query), corpo)
        r = Response()
        r.status_code, r.url, r.request, r.encoding = status, request.url, request, "utf-8"
        r._content = json.dumps(payload, ensure_ascii=False).encode()
        r.headers.update({"Content-Type": "application/json", **(headers or {})})
        self.mock.log.append({"ts": dt.datetime.utcnow().isoformat(timespec="seconds"), "metodo": request.method,
                              "endpoint": url.path, "status": status,
                              "registros": len((corpo or {}).get("inputs", [])) or None,
                              "ms": int((time.time() - t0) * 1000)})
        return r

    def close(self):
        pass
