-- =============================================================================
-- PILAR A Case: STAGING - Email Logs (SFMC)
-- Camada: Bronze -> Silver
-- BigQuery Standard SQL
-- stone-project-491000

-- Schema real da tabela de origem (raw_sfmc_email_logs):
-- event_id, user_email, event_timestamp, event_type, message_details
--
-- Decisões técnicas:
--  LOWER + TRIM para normalizar emails — garante que o JOIN com stg_crm_users não perca registros por diferença de case.

--  event_timestamp chega como STRING em UTC (servidor SFMC).
--  DATETIME(TIMESTAMP(...), 'America/Sao_Paulo') converte para BRT/BRST automaticamente, cobrindo o horário de verão.

--  JSON_VALUE extrai campaign_code do campo message_details de forma segura, retorna NULL para JSON malformado em vez de lançar erro.
--  Linhas inválidas são sinalizadas com is_valid_json = FALSE e filtradas na view stg_email_logs_valid.

--  GENERATE_UUID() cria uma surrogate key para unicidade na camada silver.
-- =============================================================================

CREATE OR REPLACE TABLE `stone-project-491000.silver.stg_email_logs` AS

WITH source AS (
  SELECT
    event_id,
    -- -------------------------------------------------------------------------
    -- Tratamento de Chaves (Pilar A.1)
    -- Normalização do email: lowercase + trim de espaços laterais.
    -- Garante que "MARIA.SANTOS@HOTMAIL.COM" faça JOIN com
    -- "maria.santos@hotmail.com" na tabela stg_crm_users.
    -- -------------------------------------------------------------------------
    LOWER(TRIM(user_email)) AS user_email_normalized,

    event_type,

    -- -------------------------------------------------------------------------
    -- Harmonização Temporal (Pilar A.2)
    -- O servidor SFMC emite em UTC (conforme nota técnica do case).
    -- TIMESTAMP() interpreta a string como UTC, e DATETIME converte
    -- para o fuso de São Paulo — cobre BRT (UTC-3) e BRST (UTC-2).
    -- Usar o nome do fuso em vez de -03:00 fixo evita erros no horário de verão.
    -- -------------------------------------------------------------------------
    DATETIME(
      TIMESTAMP(event_timestamp),           -- interpreta a string como UTC
      'America/Sao_Paulo'                   -- converte para BRT/BRST
    ) AS event_ts_brt,

    message_details,

    -- -------------------------------------------------------------------------
    -- Parsing de Dados (Pilar A.3)
    -- JSON_VALUE retorna NULL silenciosamente se a chave não existir ou se o
    -- JSON estiver malformado — nunca lança exceção em BigQuery.
    -- Isso mantém a query executável mesmo com dados sujos.
    -- -------------------------------------------------------------------------
    JSON_VALUE(message_details, '$.campaign_code') AS campaign_code,
    CASE
      WHEN message_details IS NULL THEN FALSE
      WHEN JSON_VALUE(message_details, '$.campaign_code')
           IS NULL THEN FALSE
      ELSE TRUE
    END AS is_valid_json,

    -- -------------------------------------------------------------------------
    -- Peso da interação para a regra de atribuição (Pilar B)
    -- Email Click = 2 (segundo maior peso)
    -- Email Open = 3 (terceiro maior peso)
    -- sent/delivered = 4 (só atribuído se for a única interação)
    -- WhatsApp Read = 1 (maior peso) -> definido no stg_whatsapp_logs
    -- -------------------------------------------------------------------------
    CASE event_type
      WHEN 'click'      THEN 2
      WHEN 'open'       THEN 3
      WHEN 'sent'       THEN 4
      WHEN 'delivered'  THEN 4
      ELSE 99
    END AS interaction_weight,
    'email' AS channel,
    CURRENT_TIMESTAMP() AS _loaded_at
  FROM `stone-project-491000.raw.raw_sfmc_email_logs`

  -- Eliminar linhas com email nulo
  WHERE user_email IS NOT NULL
    AND TRIM(user_email) != ''
)

SELECT
  GENERATE_UUID() AS stg_id,
  event_id AS source_id,  -- chave original para rastreabilidade
  user_email_normalized,
  NULL AS user_phone_normalized,  -- unificação de schema com WA
  event_type,
  event_ts_brt,
  campaign_code,
  interaction_weight,
  channel,
  is_valid_json,
  _loaded_at

FROM source;

-- =============================================================================
-- VIEW AUXILIAR: apenas linhas com JSON válido
-- Usada pelas camadas downstream (fct_attribution) para ignorar dados sujos
-- sem precisar repetir o filtro em cada query.
-- =============================================================================
CREATE OR REPLACE VIEW `stone-project-491000.silver.stg_email_logs_valid` AS
SELECT *
FROM `stone-project-491000.silver.stg_email_logs`
WHERE is_valid_json = TRUE;
