# Workshop · Marketing Ops na Databricks
### Windsor.ai → Unity Catalog · HubSpot → Databricks → HubSpot (Notebooks + Lakeflow Jobs)

Workshop hands-on de ~2h para ser executado **na sua própria conta Databricks**, usando **somente dados fictícios**.

Você vai construir, de ponta a ponta:

1. **Mídia direto no Unity Catalog**: a mecânica do destino Databricks do **Windsor.ai** (Google Ads, Meta Ads, TikTok Ads,
   ChatGPT Ads, GA4, Search Console) gravando em tabelas Delta governadas, sem banco intermediário.
2. **ETL HubSpot → Unity Catalog**: ingestão incremental por marca d'água (`hs_lastmodifieddate`).
3. **Processo operacional**: cálculo de health score, risco de churn, PQL, sinal de expansão, CAC por canal e prioridade de CS,
   cruzando produto + mídia + CRM.
4. **Write-back Unity Catalog → HubSpot**: batch upsert incremental, retry em rate limit (429), auditoria e anti-loop.
5. **Orquestração** com **Lakeflow Jobs** (serverless) e um **dashboard AI/BI**.

```
 "Windsor.ai" (simulado) ──SQL Warehouse + MERGE──► windsor_* ─┐
                                                                ├─► 03 cálculo ─► gold_hubspot_companies ─┐
 produto_* (fictício) ──────────────────────────────────────────┤                                         │
                                                                │                                  04 batch upsert
 HubSpot FICTÍCIO (em memória) ─02 search incremental──► bronze_hubspot_companies                          │
        ▲                                                                                                  │
        └──────────────────────────────────────────────────────────────────────────────────────────────────┘
                      Lakeflow Job: 02 ingestão ─► 03 cálculo ─► 04 sync   (agendamento PAUSADO)
```

---

## 🔒 Garantias de segurança

Este workshop foi desenhado para **não tocar em nenhum dado ou processo real da empresa**:

| Garantia | Como é feito |
|---|---|
| **Só dados fictícios** | Tudo é gerado por código determinístico (`notebooks/lib/`). Empresas e pessoas inventadas, CNPJs com dígitos verificadores **inválidos**, e-mails/domínios no TLD reservado **`.example`** (RFC 2606), IDs `FIC-xxxxx`. |
| **Nenhuma integração externa** | O "HubSpot" é um CRM **simulado que roda em memória dentro do notebook**: o cliente faz chamadas HTTP normais para `http://hubspot-ficticio.local`, entregues por um *adapter* em memória, sem nenhum transporte de rede montado. O cliente **recusa** qualquer outro host, inclusive `api.hubapi.com`. Não há tokens, chaves nem chamadas a Windsor.ai, Google, Meta, TikTok ou OpenAI. |
| **Isolamento** | Tudo vai para um schema exclusivo `lcx_workshop_<seu_usuário>` no catálogo que você escolher, marcado com a propriedade `lcx_workshop = true`. O código **se recusa** a escrever em um schema sem essa marca e **não lê** nenhuma tabela fora dele. |
| **Não interfere em processos** | Não cria service principals, grants para terceiros, conexões ou secrets. O único job criado tem o agendamento **PAUSADO** e roda apenas quando você clica/executa. |
| **Reversível** | O notebook `99_Limpeza` remove o job, o dashboard, o schema (somente se tiver a marca) e o arquivo de configuração. |

> Recomendação: rode num **catálogo de sandbox/desenvolvimento**.

---

## Pré-requisitos

| Item | Detalhe |
|---|---|
| Workspace | Databricks com **Unity Catalog** e **compute serverless** para notebooks e jobs (recomendado). Alternativa: cluster DBR 15.4 LTS+ em modo *standard/shared*. |
| Permissões | `USE CATALOG` + `CREATE SCHEMA` em um catálogo (ex.: sandbox). Para os labs 01 e 07: `CAN USE` em um **SQL Warehouse** (sem warehouse, o lab 01 usa o Spark do notebook e o 07 fica indisponível). |
| Bibliotecas | Nenhuma instalação: usa só a biblioteca padrão do Python, `requests`, `pandas` e `databricks-sdk`, já presentes no runtime. |
| Rede | Nenhum acesso externo é necessário. |

---

## Como importar

**Opção A: Git folder (recomendado)**
1. Workspace → **Create → Git folder**.
2. URL: `https://github.com/rafaelsa97/logcomex-workshop-marketing-ops` → **Create**.

**Opção B: sem Git (upload do ZIP)**
1. Baixe o ZIP do repositório (*Code → Download ZIP*).
2. No Workspace, abra a sua pasta de usuário → **⋮ → Import** → selecione o ZIP. A estrutura de pastas é preservada
   (a subpasta `notebooks/lib/` precisa ficar ao lado dos notebooks).

## Como executar

Abra os notebooks da pasta `notebooks/` **na ordem**, conecte em **Serverless** e use **Run all**:

| # | Notebook | Tempo | O que faz |
|---|---|---|---|
| 00 | `00_Configuracao_e_Dados_Ficticios` | 5 min | Escolha o **catálogo** no widget. Cria o schema isolado, os dados fictícios e o HubSpot fictício |
| 01 | `01_Windsor_ai_para_Unity_Catalog` | 10 min | Mídia → UC via SQL Warehouse + `MERGE` |
| 02 | `02_ETL_HubSpot_para_Unity_Catalog` | 10 min | HubSpot → bronze (incremental) |
| 03 | `03_Calculo_de_Metricas_de_Produto` | 15 min | Métricas → `gold_hubspot_companies` |
| 04 | `04_Sync_para_HubSpot` | 15 min | UC → HubSpot (batch upsert incremental) |
| 05 | `05_Orquestracao_com_Lakeflow_Jobs` | 15 min | Cria o job (02→03→04) e executa |
| 06 | `06_Mudancas_e_Sync_Incremental` | 15 min | Muda produto e CRM, roda o job, mostra incremental e anti-loop |
| 07 | `07_Dashboard_AI_BI` | 5 min | (Opcional) Dashboard publicado |
| 99 | `99_Limpeza` | 2 min | Remove tudo (widget `confirmar = SIM`) |

O passo a passo detalhado, com o que observar em cada lab, está em [`docs/GUIA_DO_PARTICIPANTE.md`](docs/GUIA_DO_PARTICIPANTE.md).

---

## Do workshop para produção

| No workshop | Em produção |
|---|---|
| Simulador do Windsor gravando com a sua identidade | Windsor.ai real, destino Databricks, autenticando com um **service principal** dedicado com privilégio mínimo |
| HubSpot fictício em memória (`hubspot-ficticio.local`) | `https://api.hubapi.com` + token de **Private App** em `dbutils.secrets` (escopos `crm.objects.companies.read/write`, `crm.schemas.companies.read/write`) |
| Ingestão via API (lab 02) | API ou **Lakeflow Connect, conector HubSpot** (verifique os objetos disponíveis) |
| Job criado via SDK, pausado | Job versionado com **Declarative Automation Bundles**, CI/CD, agendado |

> A trava `HOSTS_PERMITIDOS` em `notebooks/lib/hubspot_client.py` existe **de propósito** para o workshop.
> Uma implementação de produção deve ser um projeto à parte, revisado pelo time responsável pelo CRM.

## Estrutura

```
notebooks/
  00 … 07, 99        notebooks do workshop (formato fonte .py do Databricks)
  lib/
    config.py          configuração + travas de segurança (schema marcado, recusa de escrita fora dele)
    dados_ficticios.py gerador de produto/leads (fictício)
    midia_ficticia.py  gerador de mídia: Ads, GA4, Search Console (fictício)
    mock_hubspot.py    HubSpot fictício (API v3 parcial) em memória, SQLite no Volume do workshop
    hubspot_client.py  cliente HubSpot (search, properties, batch upsert, retry), bloqueia hosts externos
    uc.py              helpers de escrita no UC
    crm_view.py        visualização "estilo CRM" no notebook
    dashboard_def.py   definição do dashboard AI/BI
docs/
  GUIA_DO_PARTICIPANTE.md
```

---

*Material de workshop com dados fictícios. Não é afiliado a HubSpot, Windsor.ai, Google, Meta, TikTok ou OpenAI; os nomes são
usados apenas para descrever integrações comuns de mercado.*
