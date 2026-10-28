"""Configuração do workshop + travas de segurança.

Regras que este módulo garante:
  * Tudo é escrito SOMENTE no schema do workshop: `<catalogo>.lcx_workshop_<usuario>`.
  * O schema é marcado (`lcx_workshop = 'true'`). Se já existir um schema com o mesmo nome
    SEM essa marca, o workshop se recusa a continuar (não mexe em nada que não criou).
  * Nenhuma tabela, job ou dashboard de fora do workshop é lido ou alterado.
"""
import json
import re

from databricks.sdk import WorkspaceClient

PREFIXO_SCHEMA = "lcx_workshop_"
MARCA = "lcx_workshop"
ARQ_CONFIG = ".lcx_workshop_config.json"


class ErroSeguranca(RuntimeError):
    pass


def _slug(s: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", s.split("@")[0].lower()).strip("_")[:40]


def usuario(spark) -> str:
    return spark.sql("SELECT current_user() AS u").first().u


def caminho_config(spark) -> str:
    return f"/Users/{usuario(spark)}/{ARQ_CONFIG}"


def salvar_config(spark, cfg: dict):
    w = WorkspaceClient()
    try:  # preserva job/dashboard já criados para não deixar recursos órfãos
        with w.workspace.download(caminho_config(spark)) as f:
            antigo = json.loads(f.read().decode())
        if antigo.get("fq") == cfg["fq"]:
            for k in ("job_id", "dashboard_id"):
                if antigo.get(k) and not cfg.get(k):
                    cfg[k] = antigo[k]
    except Exception:  # noqa: BLE001
        pass
    import io
    from databricks.sdk.service.workspace import ImportFormat
    w.workspace.upload(caminho_config(spark), io.BytesIO(json.dumps(cfg, indent=2).encode()),
                       format=ImportFormat.AUTO, overwrite=True)


def carregar_config(spark) -> dict:
    w = WorkspaceClient()
    try:
        with w.workspace.download(caminho_config(spark)) as f:
            cfg = json.loads(f.read().decode())
    except Exception as e:  # noqa: BLE001
        raise RuntimeError("Configuração não encontrada. Rode primeiro o notebook "
                           "00_Configuracao_e_Dados_Ficticios.") from e
    validar_schema(spark, cfg)
    return cfg


def montar_config(spark, catalogo: str, warehouse_id: str = "") -> dict:
    catalogo = catalogo.strip() or spark.sql("SELECT current_catalog() c").first().c
    schema = PREFIXO_SCHEMA + _slug(usuario(spark))
    return {"catalogo": catalogo, "schema": schema, "fq": f"`{catalogo}`.`{schema}`",
            "volume": f"/Volumes/{catalogo}/{schema}/crm_ficticio",
            "warehouse_id": warehouse_id.strip(), "usuario": usuario(spark)}


def validar_schema(spark, cfg: dict, criar: bool = False):
    """Garante que o schema alvo é do workshop. Cria (marcado) se `criar=True`."""
    if not cfg["schema"].startswith(PREFIXO_SCHEMA):
        raise ErroSeguranca(f"Schema {cfg['schema']} não é um schema de workshop.")
    existe = spark.sql(f"SHOW SCHEMAS IN `{cfg['catalogo']}` LIKE '{cfg['schema']}'").count() > 0
    if not existe:
        if not criar:
            raise ErroSeguranca(f"Schema {cfg['fq']} não existe. Rode o notebook 00.")
        spark.sql(f"CREATE SCHEMA {cfg['fq']} COMMENT 'Workshop LogComex Marketing Ops - "
                  f"SOMENTE dados ficticios. Pode ser apagado a qualquer momento.'")
        spark.sql(f"ALTER SCHEMA {cfg['fq']} SET DBPROPERTIES ('{MARCA}' = 'true')")
        return
    props = {r[0]: r[1] for r in spark.sql(f"DESCRIBE SCHEMA EXTENDED {cfg['fq']}").collect()}
    if f"{MARCA},true" not in props.get("Properties", "").replace(" ", ""):
        raise ErroSeguranca(
            f"O schema {cfg['fq']} já existe e NÃO foi criado por este workshop. "
            "Por segurança, nada será alterado. Escolha outro catálogo no notebook 00.")


def nome_recurso(cfg: dict, base: str) -> str:
    """Nome padronizado para jobs/dashboards criados pelo workshop."""
    return f"[Workshop LCX] {base} · {cfg['schema'].removeprefix(PREFIXO_SCHEMA)}"
