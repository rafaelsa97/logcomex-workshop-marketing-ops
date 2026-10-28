# Guia do participante

> Antes de começar: leia a seção **Garantias de segurança** do [README](../README.md). Tudo aqui é **fictício** e fica
> isolado no schema `lcx_workshop_<seu_usuário>`.

## Lab 00 · Configuração e dados fictícios
1. Abra `00_Configuracao_e_Dados_Ficticios`, conecte em **Serverless**.
2. Preencha o widget **Catálogo** (ex.: o catálogo de sandbox do seu time). Vazio = catálogo padrão do workspace.
3. (Opcional) Preencha **SQL Warehouse ID**. Vazio = o primeiro warehouse que você pode usar.
4. **Run all**.

**O que observar:** o schema `lcx_workshop_<usuário>` com as tabelas `produto_*` e `marketing_leads`, e o Volume `crm_ficticio`
(onde fica o arquivo do HubSpot fictício). Se aparecer *"já existe e NÃO foi criado por este workshop"*, escolha outro catálogo.
É a trava de segurança funcionando.

## Lab 01 · Windsor.ai → Unity Catalog
**Ideia central:** o Windsor.ai grava **direto** em tabelas do UC via SQL Warehouse. Não é preciso banco intermediário.

**O que observar**
* As 6 tabelas `windsor_*` e a view `vw_midia_unificada`.
* *Catalog Explorer* → `windsor_google_ads` → **History**: cada carga é uma versão Delta.
* Rode de novo com `dias = 3`: o `MERGE` atualiza a janela sem duplicar.
* Leia o bloco *"Como fica em produção"*: service principal dedicado com privilégio mínimo.

## Lab 02 · ETL HubSpot → Unity Catalog
**Ideia central:** ler só o que mudou (`hs_lastmodifieddate` ≥ marca d'água) e fazer `MERGE` na bronze.

**O que observar**
* 1ª execução: todas as empresas do CRM fictício entram na `bronze_hubspot_companies`.
* Rode de novo: **0** registros (nada mudou).
* `hubspot_ingestao_log` guarda cada execução e a marca d'água.

## Lab 03 · Cálculo de métricas
**Ideia central:** o "processo operacional" é SQL/PySpark comum, cruzando **produto + mídia + CRM**.

**O que observar**
* `gold_marketing_canal`: CAC por canal (investimento de mídia ÷ clientes pagantes). Qual canal tem o menor CAC?
* `gold_hubspot_companies`: uma linha por conta, com os nomes internos das propriedades do HubSpot e o `row_hash`.
* `logcomex_prioridade_cs = 'Urgente'` só existe porque juntamos o dado do CRM (negócio aberto) com o de produto (risco Alto).
* **Lineage** da tabela gold no Catalog Explorer.

## Lab 04 · Write-back → HubSpot
**Ideia central:** batch upsert de 100 em 100, por chave própria (`idProperty`), enviando só o que mudou.

**O que observar**
* 1ª execução: as propriedades `logcomex_*` são criadas e todas as contas são enviadas; ~10% são **criadas** (não existiam no CRM).
* Visual "estilo CRM" com health score, risco e PQL, e o **Log de API**.
* Rode de novo: **0** enviadas.
* `simular_rate_limit = true` + `reenviar_tudo = true`: aparecem `429` e o retry automático. (Leva ~1 min de propósito.)
* **Dono da propriedade:** nome/domínio/cidade são do time comercial. O sync só os envia ao **criar** uma empresa nova.

## Lab 05 · Lakeflow Jobs
**Ideia central:** 02 → 03 → 04 viram um job serverless com retry, alerta e agendamento (**pausado**).

**O que observar**
* O grafo das tasks, o histórico de execuções e o parâmetro `{{job.run_id}}` chegando ao sync (auditoria por execução).
* **Anti-loop:** a ingestão traz de volta todas as empresas que o sync do lab 04 atualizou (o "eco" do write-back), mas o sync envia
  **0**, porque o cálculo não usa o que nós mesmos escrevemos.

## Lab 06 · Mudanças e sync incremental
**Ideia central:** mudanças dos dois lados (produto e CRM) e o anti-loop.

**O que observar**
* 5 clientes viram risco **Alto**, 3 trials ganham **PQL alto**, e contas com risco Alto e negócio novo viram **Urgente**.
* A ingestão traz só as **12** empresas alteradas pelo comercial, e o sync envia só as contas cujas métricas mudaram.

## Lab 07 · Dashboard (opcional)
Abra o link impresso. Página **Mídia & Aquisição** (CAC, funil, GA4, Search Console) e **Produto → HubSpot** (MRR em risco, auditoria).

## Lab 99 · Limpeza
Widget `confirmar = SIM` → **Run all**. Remove o job, o dashboard e o schema do workshop.

---

## Problemas comuns

| Sintoma | Causa / solução |
|---|---|
| `PERMISSION_DENIED ... CREATE SCHEMA` | Use um catálogo onde você tem `CREATE SCHEMA` (peça ao admin um catálogo de sandbox). |
| `Configuração não encontrada` | Rode o notebook 00 primeiro (ele salva `~/.lcx_workshop_config.json`). |
| `ModuleNotFoundError: lib` | A pasta `lib/` precisa estar ao lado dos notebooks (importe o repositório inteiro). |
| Lab 01 lento | Sem warehouse ligado, o primeiro comando espera ele iniciar (~1 min no serverless). |
| Lab 05 falha ao criar job | O workspace precisa de *serverless jobs*. Peça ao admin ou edite o job para usar um cluster. |
| Lab 07: "Configure um SQL Warehouse" | Informe um warehouse no widget do notebook 00 e rode-o novamente. |
| `Bloqueado: '...' não é o HubSpot fictício` | Esperado: o workshop nunca se conecta a um HubSpot real. |
