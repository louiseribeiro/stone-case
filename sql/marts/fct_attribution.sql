-- =============================================================================
-- PILAR B: MODELAGEM - Tabela de Atribuição (Gold Layer)
-- Tabela: fct_attribution
-- BigQuery Standard SQL
-- stone-project-491000
-- =============================================================================
-- Objetivo: responder "Qual campanha gerou esta conversão?"
--
-- Regra de Negócio :
--   1. Janela de 7 dias: apenas interações entre (conversion_date - 7 dias)
--      e conversion_date são elegíveis.
--   2. Peso por tipo de interação (menor número = maior prioridade):
--        1 = WhatsApp Read
--        2 = Email Click
--        3 = Email Open
--        4 = sent / delivered (fallback — só entra se for a única interação)
--
-- Estratégia de implementação
--   1. unified_interactions -> UNION dos dois canais resolvido via JOIN com CRM
--   2. interactions_in_window -> filtro da janela de 7 dias
--   3. ranked_interactions -> ROW_NUMBER com ordenação por data + peso + hora
--   4. winning_interaction -> filtra rank=1 aplicando a regra do fallback
--
-- Particionamento e Clustering:
--   PARTITION BY conversion_date -> backfill por período sem ler toda a tabela
--   CLUSTER BY campaign_code, channel -> acelera queries de dashboard
-- =============================================================================

CREATE OR REPLACE TABLE `stone-project-491000.gold.fct_attribution`
PARTITION BY conversion_date
CLUSTER BY attributed_campaign, attributed_channel
AS

WITH

-- -----------------------------------------------------------------------------
-- CTE 1: Unificação das interações dos dois canais em schema único
-- O JOIN com stg_crm_users resolve o user_id a partir do email (canal email)
-- ou do telefone (canal WhatsApp) — superando a falta de chave única entre
-- as tabelas de canais.
-- -----------------------------------------------------------------------------
unified_interactions AS (
  SELECT
    u.user_id,
    e.event_type,
    CAST(e.event_ts_brt AS DATE) AS interaction_date,
    e.event_ts_brt AS interaction_ts,
    e.campaign_code,
    e.interaction_weight,
    e.channel,
    e.source_id  AS raw_log_id

  FROM `stone-project-491000.silver.stg_email_logs_valid`   e
  INNER JOIN `stone-project-491000.silver.stg_crm_users`    u
    ON e.user_email_normalized = u.user_email_normalized
  WHERE u.is_converted = TRUE  -- apenas usuários que converteram

  UNION ALL

  -- Canal WhatsApp: chave de join = telefone normalizado
  SELECT
    u.user_id,
    w.event_type,
    CAST(w.event_ts_brt AS DATE) AS interaction_date,
    w.event_ts_brt AS interaction_ts,
    w.campaign_code,
    w.interaction_weight,
    w.channel,
    w.source_id AS raw_log_id

  FROM `stone-project-491000.silver.stg_whatsapp_logs_valid` w
  INNER JOIN `stone-project-491000.silver.stg_crm_users`  u
    ON w.user_phone_normalized = u.user_phone_normalized
  WHERE u.is_converted = TRUE
),

-- -----------------------------------------------------------------------------
-- CTE 2: Filtro da janela de 7 dias
-- Apenas interações entre (conversion_date - 7 dias) e conversion_date
-- são elegíveis para atribuição.
-- -----------------------------------------------------------------------------
interactions_in_window AS (
  SELECT
    i.*,
    u.conversion_date,
    DATE_DIFF(u.conversion_date, i.interaction_date, DAY) AS days_before_conversion

  FROM unified_interactions    i
  INNER JOIN `stone-project-491000.silver.stg_crm_users` u
    USING (user_id)

  WHERE
    i.interaction_date <= u.conversion_date           -- antes ou no dia da conversão
    AND i.interaction_date >= DATE_SUB(u.conversion_date, INTERVAL 7 DAY)  -- janela 7 dias
),

-- CTE 3: Ranking das interações por usuário — implementa o Weighted Last Touch
-- Ordenação do ROW_NUMBER:
--   1º interaction_date DESC  → prefere interações mais recentes
--   2º interaction_weight ASC → dentro do mesmo dia, menor peso vence
--   3º interaction_ts DESC    → desempate final por horário exato

ranked_interactions AS (
  SELECT
    iw.*,
    ROW_NUMBER() OVER (
      PARTITION BY iw.user_id
      ORDER BY
        iw.interaction_date  DESC,   -- mais recente primeiro
        iw.interaction_weight ASC,    -- menor peso (maior prioridade) primeiro
        iw.interaction_ts  DESC    -- horário como ultimo desempate
    ) AS attribution_rank

  FROM interactions_in_window iw
),

-- CTE 4: Seleciona a interação vencedora com a regra do fallback
-- Regra: sent/delivered (peso=4) só vence se NÃO houver nenhuma interação
-- premium (peso<4) no período. Implementado via anti-join com a subquery
-- users_with_premium_interaction.
users_with_premium_interaction AS (
  SELECT DISTINCT user_id
  FROM interactions_in_window
  WHERE interaction_weight < 4
),

winning_interaction AS (
  SELECT r.*
  FROM ranked_interactions r
  WHERE attribution_rank = 1
    AND (
      -- Interação premium (click, open, read) → sempre elegível
      r.interaction_weight < 4

      -- Interação fallback (sent/delivered) → elegível APENAS se não há premium
      OR (
        r.interaction_weight = 4
        AND r.user_id NOT IN (SELECT user_id FROM users_with_premium_interaction)
      )
    )
)

-- Resultado final: uma linha por usuário convertido com a campanha atribuída
SELECT
  GENERATE_UUID() AS attribution_id,     -- PK sintética
  w.user_id,
  w.conversion_date,
  w.campaign_code AS attributed_campaign,
  w.channel AS attributed_channel,
  w.event_type AS attributed_event_type,
  w.interaction_weight AS attributed_weight,
  w.interaction_date AS attributed_interaction_date,
  w.interaction_ts AS attributed_interaction_ts,
  w.days_before_conversion,
  w.raw_log_id,

  -- Label legível para dashboards, evita que analistas precisem decorar os pesos
  CASE w.interaction_weight
    WHEN 1 THEN 'WhatsApp Read'
    WHEN 2 THEN 'Email Click'
    WHEN 3 THEN 'Email Open'
    WHEN 4 THEN 'Sent/Delivered (fallback)'
    ELSE 'Unknown'
  END AS interaction_type_label,

  CURRENT_TIMESTAMP() AS _created_at

FROM winning_interaction w;


-- =============================================================================
-- QUERIES DE VALIDAÇÃO PÓS-CARGA 
-- =============================================================================

-- SELECT user_id, COUNT(*) AS cnt
-- FROM `stone-project-491000.gold.fct_attribution`
-- GROUP BY user_id
-- HAVING cnt > 1;

-- Distribuição de canais e tipos de interação atribuídos
-- SELECT attributed_channel, interaction_type_label, COUNT(*) AS conversions
-- FROM `stone-project-491000.gold.fct_attribution`
-- GROUP BY 1, 2
-- ORDER BY 3 DESC;

-- Usuários convertidos sem atribuição (nenhuma interação na janela de 7 dias)
-- SELECT u.user_id, u.conversion_date
-- FROM `stone-project-491000.silver.stg_crm_users` u
-- LEFT JOIN `stone-project-491000.gold.fct_attribution` a USING (user_id)
-- WHERE u.is_converted = TRUE
--   AND a.user_id IS NULL;
