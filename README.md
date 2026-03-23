# Case Técnico Stone: Data Engineer — CRM Data & Attribution

## Índice

1. [Estrutura do Repositório](#estrutura)
2. [Pilar A — Engenharia e Limpeza](#pilar-a)
3. [Pilar B — Modelagem de Atribuição](#pilar-b)
4. [Pilar C — Pipeline Python](#pilar-c)
5. [Pilar D — Arquitetura e Governança](#pilar-d)
6. [Como Executar](#como-executar)
7. [Suposições e Decisões Técnicas](#suposicoes)

---

## Estrutura do Repositório <a name="estrutura"></a>

```

├── data/ # CSVs
│   ├── raw_sfmc_email_logs.csv
│   ├── raw_whatsapp_provider.csv
│   └── crm_user_base.csv
│
├── sql/
│   ├── staging/  # Silver layer (limpeza e harmonização)
│   │   ├── stg_email_logs.sql
│   │   ├── stg_whatsapp_logs.sql
│   │   └── stg_crm_users.sql
│   └── marts/ # Gold layer (modelagem de negócio)
│       └── fct_attribution.sql
│
├── python_batch/
│   ├── ingest_email_logs.py  # Script de ingestão com Quality Gate
│   └── test_ingest_email_logs.py  # Testes unitários
│
└── README.md
```
---

## Pilar A — Engenharia e Limpeza <a name="pilar-a"></a>

### A.1 — Tratamento de Chaves

O maior risco de perda em JOINs entre tabelas de canais e o CRM é a
inconsistência de formato nas chaves de identidade.

**Emails** resolvidos com:
```sql
LOWER(TRIM(user_email))
```
Isso elimina diferenças de case (`MARIA.SANTOS` vs `maria.santos`) e espaços invisíveis.

**Telefones** resolvidos com lógica em cascata:

```sql
CASE
  -- Já tem DDI 55 e comprimento correto: remove não-dígitos
  WHEN LENGTH(REGEXP_REPLACE(phone, r'[^\d]', '')) >= 12
       AND STARTS_WITH(REGEXP_REPLACE(phone, r'[^\d]', ''), '55')
    THEN REGEXP_REPLACE(phone, r'[^\d]', '')

  -- Tem 10-11 dígitos (DDD + número, sem DDI): adiciona "55"
  WHEN LENGTH(REGEXP_REPLACE(phone, r'[^\d]', '')) IN (10, 11)
    THEN CONCAT('55', REGEXP_REPLACE(phone, r'[^\d]', ''))

  ELSE REGEXP_REPLACE(phone, r'[^\d]', '')
END
```

Exemplos cobertos:
| Entrada | Saída normalizada |
|---|---|
| `+55 (11) 98765-4321` | `5511987654321` |
| `55 21 99887-6543` | `5521998876543` |
| `5511912345678` | `5511912345678` |


---

### A.2 — Harmonização Temporal

Há dois fusos distintos nos dados de origem:

| Tabela | Fuso de origem | Tratamento |
|---|---|---|
| `raw_sfmc_email_logs` | **UTC** (servidor SFMC) | `DATETIME(TIMESTAMP(ts), 'America/Sao_Paulo')` |
| `raw_whatsapp_provider` | **BRT** (provedor BR) | `PARSE_DATETIME('%Y-%m-%d %H:%M:%S', ts)` — sem conversão |
| `crm_user_base` | Data local (sem hora) | `PARSE_DATE('%Y-%m-%d', conversion_date)` |

Usar `'America/Sao_Paulo'` em vez de um offset fixo como `-03:00` é
importante porque o BigQuery aplica automaticamente a transição para
horário de verão (BRST = UTC-2), evitando erros sazonais.

---

### A.3 — Parsing de Dados

**Email (JSON):** uso de `JSON_VALUE()` do BigQuery, que retorna `NULL`
silenciosamente para JSON malformado em vez de lançar erro. Isso mantém
a query executável mesmo com dados sujos e as linhas inválidas são
sinalizadas via `is_valid_json = FALSE` e filtradas na view downstream.

```sql
JSON_VALUE(message_details, '$.campaign_code') AS campaign_code
```

**WhatsApp (string suja):** uso de `REGEXP_EXTRACT` para capturar o
código entre a tag `[CAMP:` e o fechamento `]`:

```sql
REGEXP_EXTRACT(campaign_tag, r'\[CAMP:([^\]]+)\]') AS campaign_code
-- "[CAMP:CAMP_CARTAO_JAN] Mensagem lida" → "CAMP_CARTAO_JAN"
```

---

## Pilar B — Modelagem de Atribuição <a name="pilar-b"></a>

### Regra: Weighted Last Touch

A tabela `fct_attribution` responde "qual campanha gerou esta conversão?"
usando 4 CTEs encadeadas:

```
unified_interactions -> interactions_in_window -> ranked_interactions -> winning_interaction
```

#### Resolução do conflito de canais

A ordenação do `ROW_NUMBER` é:

```sql
ROW_NUMBER() OVER (
  PARTITION BY user_id
  ORDER BY
    interaction_date   DESC,  -- 1º: interação mais recente
    interaction_weight ASC,   -- 2º: menor peso = maior prioridade
    interaction_ts     DESC   -- 3º: horário (desempate final)
)
```

O caso descrito no enunciado: Email Open às 20h (peso=3) vs WA Read às 09h (peso=1) no mesmo dia.
Se ordenássemos por horário primeiro, o Email Open venceria (20h > 09h).
Ordenando por peso primeiro, o WA Read (peso=1 < peso=3) fica em rank=1, de maneira correta.

| Peso | Tipo | Canal |
|---|---|---|
| 1 | Read | WhatsApp |
| 2 | Click | Email |
| 3 | Open | Email |
| 4 | Sent / Delivered | Ambos (fallback) |

#### Regra de fallback sent/delivered

Sent e Delivered só entram na atribuição se não houver **nenhuma**
interação premium (peso < 4) no período de 7 dias. Implementado via CTE
auxiliar `users_with_premium_interaction` e anti-join:

```sql
WHERE interaction_weight < 4
   OR (
     interaction_weight = 4
     AND user_id NOT IN (SELECT user_id FROM users_with_premium_interaction)
   )
```

#### Particionamento e Clustering da tabela Gold

```sql
PARTITION BY conversion_date
CLUSTER BY campaign_code, channel
```

- **Partition pruning**: ao filtrar por data (ex: backfill de um mês),
  o BigQuery lê apenas as partições relevantes, reduzindo custo e latência.
- **Clustering**: queries de dashboard que agrupam por campanha ou canal
  percorrem menos blocos de dados.

---

## Pilar C — Pipeline Python <a name="pilar-c"></a>

### Arquitetura do script `ingest_email_logs.py`

```
read_csv()
    ↓
validate_dataframe()   ← Quality Gate
    ↓              ↓
df_valid        df_quarantine
    ↓                  ↓
load_to_bigquery    load_to_bigquery
(WRITE_TRUNCATE)   (WRITE_APPEND)
raw_sfmc_email_logs  raw_sfmc_email_logs_quarantine
```

### Quality Gate — 3 categorias de falha

| Condição | Ação |
|---|---|
| `message_details` nulo/vazio | Linha vai para quarentena |
| JSON malformado (não parseable) | Linha vai para quarentena + log WARNING |
| JSON válido mas sem `campaign_code` | Linha vai para quarentena |


### Integração com Airflow

O script retorna exit code `1` se houver linhas em quarentena, o que
permite ao Airflow:
- Continuar a DAG (dados válidos já foram carregados)
- Disparar um alerta/SLA miss para investigação

Exemplo de task no Airflow:

```python
from airflow.operators.bash import BashOperator

ingest_task = BashOperator(
    task_id="ingest_sfmc_email_logs",
    bash_command="""
        python /opt/airflow/scripts/ingest_email_logs.py \
            --source-file /data/raw_sfmc_email_logs_{{ ds }}.csv \
            --project-id stone-project-491000 \
            --dataset-raw raw
    """,
    # exit_code=1 não quebra a DAG, apenas gera alerta
    retries=2,
    retry_delay=timedelta(minutes=5),
)
```

---

## Pilar D — Arquitetura e Governança <a name="pilar-d"></a>

### Desenho das Camadas no BigQuery

```
┌─────────────────────────────────────────────────────────────────┐
│  EXTERNAL / SOURCES                                             │
│  • SFMC API (Email logs)    • WhatsApp Provider (FTP/API)       │
│  • Salesforce CRM                                               │
└──────────────────────┬──────────────────────────────────────────┘
                       │ Ingestão (Python + Airflow)
                       ▼
┌─────────────────────────────────────────────────────────────────┐
│  DATASET: raw  (Bronze)                                         │
│  • raw_sfmc_email_logs          → append diário + partição      │
│  • raw_whatsapp_provider        → append diário + partição      │
│  • crm_user_base                → full refresh diário           │
│  • raw_sfmc_email_logs_quarantine → append (auditoria erros)    │
│                                                                 │
│  dados não são alterados nesta camada.              │
│  Retenção: 90 dias (custo controlado).                          │
└──────────────────────┬──────────────────────────────────────────┘
                       │ dbt / Stored Procedures
                       ▼
┌─────────────────────────────────────────────────────────────────┐
│  DATASET: silver  (Silver)                                      │
│  • stg_email_logs               → normalização, timezone, peso  │
│  • stg_whatsapp_logs            → normalização, regex, peso     │
│  • stg_crm_users                → chaves normalizadas, flags    │
│                                                                 │
│  
│  Sem lógica de negócio — apenas qualidade e padronização.       │
└──────────────────────┬──────────────────────────────────────────┘
                       │ dbt / Stored Procedures
                       ▼
┌─────────────────────────────────────────────────────────────────┐
│  DATASET: gold  (Gold)                                          │
│  • fct_attribution              → tabela de atribuição          │
│  • dim_campaigns (sugerida)     → dimensão de campanhas         │
│  • agg_campaign_daily (sugerida)→ agregação para dashboards     │
│                                                                 │
│  
│  Consumida por: Looker, Metabase, Google Data Studio.           │
└─────────────────────────────────────────────────────────────────┘
```

### Stored Procedures

- **Silver layer**: Procedures para as stagings (executadas em sequência):
  `sp_run_stg_email_logs` → `sp_run_stg_whatsapp_logs` → `sp_run_stg_crm_users`
- **Gold layer**: `sp_run_fct_attribution(start_date DATE, end_date DATE)`
  — aceita parâmetros de data para suportar backfill.
- **Orquestração**: Airflow chama as procedures via `BigQueryInsertJobOperator`
  em ordem de dependência.

---

### Como reprocessar dados passados

#### DELETE + INSERT por partição

A tabela `fct_attribution` é particionada por `conversion_date`. O
reprocessamento opera apenas nas partições afetadas:

```sql
-- Passo 1: Remove apenas as partições do período afetado
DELETE FROM `stone-project-491000.gold.fct_attribution`
WHERE conversion_date BETWEEN '2024-01-01' AND '2024-01-31';

-- Passo 2: Reinsere com a lógica corrigida
-- (roda o script fct_attribution.sql com filtro de data)
INSERT INTO `stone-project-491000.gold.fct_attribution`
SELECT ... FROM ... WHERE conversion_date BETWEEN '2024-01-01' AND '2024-01-31';
```

Ou, com a Stored Procedure parametrizada:

```sql
CALL `stone-project-491000.gold.sp_run_fct_attribution`('2024-01-01', '2024-01-31');
```

#### Garantir que as stagings também tenham os dados do passado

A camada Raw tem o dado bruto do período completo (retenção de 90 dias).
As stagings (Silver) são `CREATE OR REPLACE TABLE AS SELECT` , um
reprocessamento com filtro de data na Silver produz exatamente os dados
corretos para alimentar a Gold.

---

## Como Executar <a name="como-executar"></a>

### SQL (BigQuery)

```bash
# Executar stagings (em ordem)
bq query --use_legacy_sql=false < sql/staging/stg_crm_users.sql
bq query --use_legacy_sql=false < sql/staging/stg_email_logs.sql
bq query --use_legacy_sql=false < sql/staging/stg_whatsapp_logs.sql

# Executar tabela de atribuição
bq query --use_legacy_sql=false < sql/marts/fct_attribution.sql
```

### Python

```bash
# Instalar dependências
pip install pandas google-cloud-bigquery pyarrow

# Dry run (sem carga real no BQ)
python python/ingest_email_logs.py \
    --source-file data/raw_sfmc_email_logs.csv \
    --project-id stone-project-491000 \
    --dry-run

# Execução real
python python/ingest_email_logs.py \
    --source-file data/raw_sfmc_email_logs.csv \
    --project-id stone-project-491000 \
    --dataset-raw raw

# Testes unitários
pytest python/test_ingest_email_logs.py -v
```

---

## Suposições e Decisões Técnicas <a name="suposicoes"></a>

| Área | Suposição / Decisão |
|---|---|
| Timezone WA | O enunciado confirma BRT — assumi offset sem DST awareness (se o provedor mudar para UTC, apenas o parsing muda) |
| Telefone sem DDI | Assumi DDI 55 (Brasil) para números de 10-11 dígitos. Números estrangeiros cairiam no fallback sem conversão |
| JSON parcialmente válido | `JSON_VALUE` do BigQuery retorna NULL para chaves ausentes, aproveitei esse comportamento para simplificar o tratamento |
| Conversões sem interação | Usuários que converteram sem nenhuma interação no período de 7 dias **não aparecem** na `fct_attribution`. 
| Schema autodetect | Usei `autodetect=True` no script Python para simplificação. Em produção, o schema deve ser explícito para garantir contratos de dados |
| Idempotência do GENERATE_UUID | UUIDs são gerados no momento da carga. Em backfill, os IDs mudam, isso é aceitável porque `attribution_id` é apenas PK técnica, não chave de negócio |
| dbt vs Stored Procedures | A solução foi escrita em SQL puro para portabilidade. Em produção, recomendo dbt para versionamento, testes automáticos (`dbt test`) e documentação de linhagem de dados |
