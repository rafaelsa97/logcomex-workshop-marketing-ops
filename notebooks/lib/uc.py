"""Helpers de escrita no Unity Catalog, sempre dentro do schema do workshop."""
import time

from .config import MARCA, ErroSeguranca


def t(cfg: dict, nome: str) -> str:
    """Nome totalmente qualificado de uma tabela do workshop."""
    if not nome.replace("_", "").isalnum():
        raise ErroSeguranca(f"Nome de tabela inválido: {nome}")
    return f"{cfg['fq']}.`{nome}`"


def marcar(spark, cfg: dict, nome: str, comentario: str):
    spark.sql(f"COMMENT ON TABLE {t(cfg, nome)} IS '{comentario.replace(chr(39), '')}'")
    spark.sql(f"ALTER TABLE {t(cfg, nome)} SET TBLPROPERTIES ('{MARCA}' = 'true', 'dados' = 'ficticios')")


def salvar_pandas(spark, cfg: dict, pdf, nome: str, comentario: str):
    df = spark.createDataFrame(pdf)
    df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(t(cfg, nome))
    marcar(spark, cfg, nome, comentario)
    print(f"  {nome:28s} {df.count():>8,} linhas")


class ExecutorSQL:
    """Executa SQL pelo SQL Warehouse (o mesmo caminho que o Windsor.ai usa). Se não houver
    warehouse configurado, cai para o Spark do próprio notebook."""

    def __init__(self, spark, warehouse_id: str):
        self.spark, self.wh = spark, warehouse_id
        if warehouse_id:
            from databricks.sdk import WorkspaceClient
            self.w = WorkspaceClient()

    @property
    def via(self):
        return f"SQL Warehouse {self.wh}" if self.wh else "Spark do notebook (sem warehouse)"

    def __call__(self, stmt: str):
        if not self.wh:
            return self.spark.sql(stmt)
        r = self.w.statement_execution.execute_statement(warehouse_id=self.wh, statement=stmt, wait_timeout="50s")
        while r.status.state.value in ("PENDING", "RUNNING"):
            time.sleep(1)
            r = self.w.statement_execution.get_statement(r.statement_id)
        if r.status.state.value != "SUCCEEDED":
            msg = r.status.error.message if r.status.error else ""
            raise RuntimeError(f"{r.status.state.value}: {msg}\n{stmt[:300]}")
        return r
