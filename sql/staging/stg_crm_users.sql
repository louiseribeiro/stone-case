-- =============================================================================
-- PILAR A Case: STAGING - CRM User Base
-- Camada: Bronze -> Silver
-- Dialeto: BigQuery Standard SQL
-- Projeto: stone-project-491000

-- Schema real da tabela de origem (crm_user_base):
-- user_id, email, phone, conversion_at
--
-- Decisões técnicas:
-- Foi aplicado as mesmas normalizações de email e telefone usadas nas tabelas de canais. 

-- conversion_at chega como TIMESTAMP — extraímos apenas o DATE pois
-- a granularidade do negócio é dia de conversão, e a hora é irrelevante
-- para o cálculo da janela de 7 dias.

-- Manteve-se usuários sem conversão (conversion_at IS NULL) na staging, eles podem ser úteis para análises de não-conversão e funil.
-- =============================================================================

CREATE OR REPLACE TABLE `stone-project-491000.silver.stg_crm_users` AS
SELECT
  user_id,
  -- -------------------------------------------------------------------------
  -- Tratamento de Chaves (Pilar A.1)
  -- Normalização do email: lowercase + trim de espaços laterais.
  -- Mesma lógica aplicada no stg_email_logs — garante que o JOIN funcione
  -- mesmo com diferenças de case entre as tabelas.
  -- -------------------------------------------------------------------------
  LOWER(TRIM(email)) AS user_email_normalized,

  -- -------------------------------------------------------------------------
  -- Tratamento de Chaves (Pilar A.1)
  -- Normalização do telefone: remove todos os não-dígitos e garante o DDI 55.
  -- Mesma lógica aplicada no stg_whatsapp_logs — lado direito do JOIN.
  -- Exemplos cobertos:
  --   "+55 (11) 98765-4321" -> "5511987654321"
  --   "55 21 99887-6543" -> "5521998876543"
  -- -------------------------------------------------------------------------
  CASE
    -- Já tem DDI 55 e comprimento correto: remove apenas formatação
    WHEN LENGTH(REGEXP_REPLACE(phone, r'[^\d]', '')) >= 12
         AND STARTS_WITH(REGEXP_REPLACE(phone, r'[^\d]', ''), '55')
      THEN REGEXP_REPLACE(phone, r'[^\d]', '')

    -- Tem 10-11 dígitos (DDD + número, sem DDI): adiciona "55"
    WHEN LENGTH(REGEXP_REPLACE(phone, r'[^\d]', '')) IN (10, 11)
      THEN CONCAT('55', REGEXP_REPLACE(phone, r'[^\d]', ''))

    ELSE REGEXP_REPLACE(phone, r'[^\d]', '')  -- fallback: só dígitos
  END AS user_phone_normalized,

  -- -------------------------------------------------------------------------
  -- Temporal (Pilar A.2)
  -- conversion_at chega como TIMESTAMP — foi extraido o DATE pois a janela de
  -- atribuição de 7 dias opera em granularidade de dia, não de hora.
  -- -------------------------------------------------------------------------
  DATE(conversion_at) AS conversion_date,

  -- Flag: usuário convertido — usada para filtrar apenas usuários elegíveis
  -- para atribuição na fct_attribution
  CASE WHEN conversion_at IS NOT NULL THEN TRUE ELSE FALSE END  AS is_converted,

  -- Metadado de carga para rastreabilidade 
  CURRENT_TIMESTAMP() AS _loaded_at

FROM `stone-project-491000.raw.crm_user_base`;
