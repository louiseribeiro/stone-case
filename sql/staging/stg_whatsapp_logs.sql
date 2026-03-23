-- =============================================================================
-- PILAR A: STAGING - WhatsApp Logs
-- Camada: Bronze → Silver
-- BigQuery Standard SQL
-- stone-project-491000
-- =============================================================================
-- Schema real da tabela de origem (raw_whatsapp_provider):
--   message_id, phone_clean, sent_at_brt, status, campaign_tag
--
-- Decisões técnicas:
--  phone_clean chega como INT64 (o BigQuery inferiu como número no upload). Usado CAST(phone_clean AS STRING) antes do REGEXP_REPLACE para evitar
--  erro de tipo. A normalização garante consistência com o CRM.

--  sent_at_brt chega como TIMESTAMP já em BRT (conforme nota técnica).
--  DATETIME(..., 'America/Sao_Paulo') converte para DATETIME local sem
--  alterar o valor — apenas ajusta o tipo para consistência com email.

--  campaign_tag tem o formato real "[WA] - CAMPAIGN_CODE - Batch X". TRIM + REGEXP_EXTRACT captura o código entre o primeiro e o segundo " - ".

--  O campo status (equivalente ao event_type do email) mapeia para os mesmos pesos de interação usados na fct_attribution.
-- =============================================================================

CREATE OR REPLACE TABLE `stone-project-491000.silver.stg_whatsapp_logs` AS

WITH source AS (
  SELECT
    message_id AS source_id,
    -- Tratamento de Chaves (Pilar A.1)
    CASE
      -- Já tem DDI 55 e comprimento correto: remove apenas formatação
      WHEN LENGTH(REGEXP_REPLACE(CAST(phone_clean AS STRING), r'[^\d]', '')) >= 12
           AND STARTS_WITH(REGEXP_REPLACE(CAST(phone_clean AS STRING), r'[^\d]', ''), '55')
        THEN REGEXP_REPLACE(CAST(phone_clean AS STRING), r'[^\d]', '')

      -- Tem 10-11 dígitos (DDD + número, sem DDI): adiciona "55"
      WHEN LENGTH(REGEXP_REPLACE(CAST(phone_clean AS STRING), r'[^\d]', '')) IN (10, 11)
        THEN CONCAT('55', REGEXP_REPLACE(CAST(phone_clean AS STRING), r'[^\d]', ''))

      ELSE REGEXP_REPLACE(CAST(phone_clean AS STRING), r'[^\d]', '')  -- fallback
    END AS user_phone_normalized,

    -- status é o equivalente ao event_type do canal email
    status AS event_type,

    -- (Pilar A.2)

    DATETIME(sent_at_brt, 'America/Sao_Paulo') AS event_ts_brt,

    -- -------------------------------------------------------------------------
    -- (Pilar A.3)
    -- Formato real do campo: "[WA] - BLK_FRIDAY_23 - Batch A"
    -- O regex captura tudo entre o primeiro " - " e o segundo " - ",
    -- e o TRIM remove espaços residuais.
    -- Exemplo: "[WA] - BLK_FRIDAY_23 - Batch A" → "BLK_FRIDAY_23"
    -- -------------------------------------------------------------------------
    TRIM(REGEXP_EXTRACT(campaign_tag, r'\[WA\] - ([^-]+) -'))   AS campaign_code,

    -- (Pilar B)
    CASE status
      WHEN 'read' THEN 1
      WHEN 'delivered' THEN 4
      WHEN 'sent' THEN 4
      ELSE 99
    END AS interaction_weight,

    'whatsapp' AS channel,

    -- Flag de qualidade: campaign_code deve ser extraível do campaign_tag
    CASE
      WHEN TRIM(REGEXP_EXTRACT(campaign_tag, r'\[WA\] - ([^-]+) -'))
           IS NULL THEN FALSE
      ELSE TRUE
    END AS is_valid_json,

    -- Metadado de carga para rastreabilidade / backfill
    CURRENT_TIMESTAMP() AS _loaded_at

  FROM `stone-project-491000.raw.raw_whatsapp_provider`
  WHERE phone_clean IS NOT NULL
)

SELECT
  GENERATE_UUID() AS stg_id,
  source_id,
  NULL AS user_email_normalized,   -- unificação de schema com email
  user_phone_normalized,
  event_type,
  event_ts_brt,
  campaign_code,
  interaction_weight,
  channel,
  is_valid_json,
  _loaded_at

FROM source;

-- =============================================================================
-- VIEW AUXILIAR: apenas linhas com campaign_code extraído com sucesso
-- Usada pelas camadas downstream (fct_attribution) para ignorar dados sujos.
-- =============================================================================
CREATE OR REPLACE VIEW `stone-project-491000.silver.stg_whatsapp_logs_valid` AS
SELECT *
FROM `stone-project-491000.silver.stg_whatsapp_logs`
WHERE is_valid_json = TRUE;