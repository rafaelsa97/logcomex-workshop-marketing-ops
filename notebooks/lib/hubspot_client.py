"""Cliente HubSpot mínimo (API v3) usado no workshop: leitura incremental (search),
criação de propriedades e batch upsert com retry.

TRAVA DE SEGURANÇA: neste workshop o cliente só aceita o HubSpot FICTÍCIO em memória
(`http://hubspot-ficticio.local` + adapter em memória). Qualquer outro host, incluindo
api.hubapi.com, é recusado, e a sessão não tem nenhum transporte de rede montado, então
nenhum dado do workshop pode ir para o CRM real da empresa.
"""
import time
from urllib.parse import urlparse

import requests

HOSTS_PERMITIDOS = {"hubspot-ficticio.local"}


class HubSpot:
    def __init__(self, base_url: str, adapter, token: str | None = None, max_tentativas: int = 8):
        host = urlparse(base_url).hostname
        if host not in HOSTS_PERMITIDOS:
            raise PermissionError(
                f"Bloqueado: '{host}' não é o HubSpot fictício. Este workshop nunca "
                "se conecta a um HubSpot real.")
        self.base, self.max = base_url.rstrip("/"), max_tentativas
        self.headers = {"Content-Type": "application/json"}
        if token:  # em produção seria o Private App token (dbutils.secrets); aqui não é usado
            self.headers["Authorization"] = f"Bearer {token}"
        self.s = requests.Session()
        self.s.trust_env = False
        # remove os transportes de rede padrão e monta SÓ o adapter em memória do CRM fictício
        self.s.adapters.clear()
        self.s.mount("http://hubspot-ficticio.local/", adapter)
        self.chamadas = []

    def call(self, metodo: str, caminho: str, corpo: dict | None = None):
        espera = 1.0
        for tentativa in range(1, self.max + 1):
            t0 = time.time()
            r = self.s.request(metodo, f"{self.base}{caminho}", json=corpo, headers=self.headers, timeout=60)
            ms = int((time.time() - t0) * 1000)
            self.chamadas.append((metodo, caminho, r.status_code, tentativa, ms))
            if r.status_code == 429 or r.status_code >= 500:
                pausa = float(r.headers.get("Retry-After", espera))
                print(f"  HTTP {r.status_code} em {caminho} (tentativa {tentativa}) -> aguardando {pausa:.0f}s e tentando de novo")
                time.sleep(pausa)
                espera = min(espera * 2, 16)
                continue
            return r, tentativa, ms
        r.raise_for_status()

    # ------------------------------------------------------------------ leitura
    def buscar_modificadas(self, desde_ms: int, propriedades: list[str]):
        """Todas as empresas com hs_lastmodifieddate >= desde_ms (paginação do search)."""
        depois, saida = 0, []
        while True:
            corpo = {"filterGroups": [{"filters": [
                {"propertyName": "hs_lastmodifieddate", "operator": "GTE", "value": str(desde_ms)}]}],
                "sorts": [{"propertyName": "hs_lastmodifieddate", "direction": "ASCENDING"}],
                "properties": propriedades, "limit": 100, "after": depois}
            r, _, _ = self.call("POST", "/crm/v3/objects/companies/search", corpo)
            r.raise_for_status()
            d = r.json()
            saida.extend(d["results"])
            prox = d.get("paging", {}).get("next", {}).get("after")
            if not prox:
                return saida
            depois = int(prox)

    # ------------------------------------------------------------------ escrita
    def garantir_propriedades(self, grupo: str, rotulo_grupo: str, definicoes: dict) -> int:
        r, _, _ = self.call("GET", "/crm/v3/properties/companies")
        r.raise_for_status()
        existentes = {p["name"] for p in r.json()["results"]}
        self.call("POST", "/crm/v3/properties/companies/groups", {"name": grupo, "label": rotulo_grupo})
        criadas = 0
        for nome, (rotulo, tipo, campo, opcoes) in definicoes.items():
            if nome in existentes:
                continue
            corpo = {"name": nome, "label": rotulo, "groupName": grupo, "type": tipo, "fieldType": campo,
                     "description": "Calculado no Databricks (workshop, dados fictícios)"}
            if opcoes:
                corpo["options"] = [{"label": o, "value": o} for o in opcoes]
            rr, _, _ = self.call("POST", "/crm/v3/properties/companies", corpo)
            rr.raise_for_status()
            criadas += 1
        return criadas

    def upsert_lotes(self, registros: list[dict], id_property: str, lote: int = 100):
        """Batch upsert de 100 em 100. Devolve (log_por_lote, resultados)."""
        log, resultados = [], []
        for i in range(0, len(registros), lote):
            bloco = registros[i:i + lote]
            corpo = {"inputs": [{"idProperty": id_property, "id": r[id_property],
                                 "properties": {k: _valor(v) for k, v in r.items()}} for r in bloco]}
            r, tent, ms = self.call("POST", "/crm/v3/objects/companies/batch/upsert", corpo)
            d = r.json()
            res, erros = d.get("results", []), d.get("errors", [])
            resultados.extend(res)
            novos = sum(1 for x in res if x.get("new"))
            log.append({"lote": i // lote + 1, "http_status": r.status_code, "tentativas": tent, "latencia_ms": ms,
                        "registros": len(bloco), "criados": novos, "atualizados": len(res) - novos,
                        "erros": len(erros), "detalhe_erros": str(erros)[:2000] if erros else None})
            print(f"lote {i // lote + 1}: HTTP {r.status_code} | {len(res)} ok ({novos} novos) | "
                  f"{len(erros)} erros | {ms} ms | tentativas {tent}")
        return log, resultados


def _valor(v):
    if v is None:
        return ""  # no HubSpot, string vazia limpa a propriedade
    if isinstance(v, float):
        return f"{v:.2f}".rstrip("0").rstrip(".")
    return str(v)
