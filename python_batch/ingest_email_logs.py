"""
PILAR C do case: Ingestão e Quality Gate - SFMC Email Logs
Objetivo:
Simula a ingestão diária do arquivo raw_sfmc_email_logs.csv para o BigQuery,
com validação de qualidade antes da carga.

Design decisions:
O Quality Gate rejeita linhas com JSON malformado MAS não aborta toda
a execução — ele segrega as linhas ruins em uma tabela de quarentena
para análise posterior, e carrega apenas as linhas válidas. Essa
abordagem evita que um único registro corrompido bloqueie toda a pipeline.

WRITE_TRUNCATE utilizado para permitir reprocessamento seguro. Em produção, para carga incremental,
usaríamos WRITE_APPEND com deduplicação no BigQuery via MERGE.
O script é parametrizável via argparse para uso em DAGs Airflow.

necessário executar pip install google-cloud-bigquery pandas pyarrow
"""

import argparse
import json
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import pandas as pd
from google.cloud import bigquery

logging.basicConfig(
    level=logging.INFO,
    format='{"time": "%(asctime)s", "level": "%(levelname)s", "message": %(message)s}',
    datefmt="%Y-%m-%dT%H:%M:%SZ",
)
logger = logging.getLogger(__name__)

REQUIRED_COLUMNS = {"event_id", "user_email", "event_type", "event_timestamp", "message_details"}


#Campos obrigatórios dentro do JSON de message_details
REQUIRED_JSON_FIELDS = {"campaign_code"}


# FUNÇÕES DE VALIDAÇÃO (Quality Gate)

def validate_json_field(row: pd.Series) -> dict:
    """
    Valida o campo message_details de uma linha.

    Retorna um dict com:
        - is_valid (bool): True se o JSON é bem formado e contém os campos obrigatórios
        - error_reason (str | None): descrição do erro se inválido
        - parsed_json (dict | None): o JSON parseado se válido

        Retorna um dict em vez de lançar exceção para que o chamador possa
        agregar resultados sem try/except em loop — mais performático.
    """
    raw_value = row.get("message_details")

    #Caso 1: campo nulo ou vazio
    if pd.isna(raw_value) or str(raw_value).strip() == "":
        return {
            "is_valid": False,
            "error_reason": "message_details is null or empty",
            "parsed_json": None,
        }

    #Caso 2: JSON malformado (não parseable)
    try:
        parsed = json.loads(str(raw_value))
    except json.JSONDecodeError as e:
        return {
            "is_valid": False,
            "error_reason": f"JSONDecodeError: {str(e)[:200]}",
            "parsed_json": None,
        }

    #Caso 3: JSON parseable mas sem os campos obrigatorios
    missing_fields = REQUIRED_JSON_FIELDS - set(parsed.keys())
    if missing_fields:
        return {
            "is_valid": False,
            "error_reason": f"Missing required JSON fields: {missing_fields}",
            "parsed_json": parsed,
        }

    return {"is_valid": True, "error_reason": None, "parsed_json": parsed}


def validate_dataframe(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, dict]:
    """
    Aplica o Quality Gate linha a linha no DataFrame.

    Retorna:
        df_valid: DataFrame com linhas aprovadas
        df_quarantine: DataFrame com linhas rejeitadas + metadados de erro
        stats: dicionário com métricas do processo de validação

    """
    valid_indices = []
    quarantine_records = []

    #Verifica colunas obrigatorias antes de processar linha a linha
    missing_cols = REQUIRED_COLUMNS - set(df.columns)
    if missing_cols:
        raise ValueError(f"Arquivo CSV esta faltando colunas obrigatórias: {missing_cols}")

    for idx, row in df.iterrows():
        validation_result = validate_json_field(row)

        if validation_result["is_valid"]:
            valid_indices.append(idx)
        else:
            quarantine_record = row.to_dict()
            quarantine_record["_error_reason"] = validation_result["error_reason"]
            quarantine_record["_quarantined_at"] = datetime.now(timezone.utc).isoformat()
            quarantine_records.append(quarantine_record)

            # Log estruturado para cada linha rejeitada
            logger.warning(
                json.dumps({
                    "event": "row_quarantined",
                    "log_id": row.get("log_id", "UNKNOWN"),
                    "user_email": row.get("user_email", "UNKNOWN"),
                    "error_reason": validation_result["error_reason"],
                })
            )

    df_valid = df.loc[valid_indices].copy()
    df_quarantine = pd.DataFrame(quarantine_records) if quarantine_records else pd.DataFrame()

    stats = {
        "total_rows": len(df),
        "valid_rows": len(df_valid),
        "quarantined_rows": len(df_quarantine),
        "quarantine_rate_pct": round(len(df_quarantine) / len(df) * 100, 2) if len(df) > 0 else 0,
    }

    logger.info(json.dumps({"event": "validation_summary", **stats}))

    # Alerta se taxa de quarentena for alta (threshold configurável)
    quarantine_threshold_pct = 10.0
    if stats["quarantine_rate_pct"] > quarantine_threshold_pct:
        logger.error(
            json.dumps({
                "event": "high_quarantine_rate",
                "quarantine_rate_pct": stats["quarantine_rate_pct"],
                "threshold_pct": quarantine_threshold_pct,
                "message": "Quarantine rate exceeded threshold. Investigate source data.",
            })
        )

    return df_valid, df_quarantine, stats


# TRANSFORMAÇÕES (pré-carga na camada Raw)

def enrich_dataframe(df: pd.DataFrame, source_file: str) -> pd.DataFrame:
    """
    Adiciona metadados de rastreabilidade a carga.
    A transformação real (normalização, timezone) ocorre na camada Silver.
    Mantem Raw o mais próximo possível do original.
    """
    df = df.copy()
    df["_source_file"] = Path(source_file).name
    df["_ingested_at"] = datetime.now(timezone.utc).isoformat()
    return df


# ARGA NO BIGQUERY

def load_to_bigquery(
    df: pd.DataFrame,
    project_id: str,
    dataset_id: str,
    table_id: str,
    write_disposition: str = "WRITE_TRUNCATE",
) -> None:
    """
    Carrega um DataFrame no BigQuery.
    Se o Airflow rodar duas vezes no mesmo dia, não haverá duplicatas.
    WRITE_APPEND para a tabela de quarentena: queremos histórico de
    todos os erros, não apenas os do último run.
    """
    if df.empty:
        logger.info(json.dumps({"event": "skip_empty_load", "table": f"{dataset_id}.{table_id}"}))
        return

    client = bigquery.Client(project=project_id)
    table_ref = f"{project_id}.{dataset_id}.{table_id}"

    job_config = bigquery.LoadJobConfig(
        write_disposition=write_disposition,
        autodetect=True, 
    )

    logger.info(json.dumps({
        "event": "bq_load_start",
        "table": table_ref,
        "rows": len(df),
        "write_disposition": write_disposition,
    }))

    job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
    job.result()  

    logger.info(json.dumps({
        "event": "bq_load_complete",
        "table": table_ref,
        "rows_loaded": job.output_rows,
    }))


# FUNÇÃO PRINCIPAL DE INGESTÃO

def ingest_email_logs(
    source_file: str,
    project_id: str,
    dataset_raw: str = "raw",
    dry_run: bool = False,
) -> dict:
    """
    Orquestra o pipeline completo:
        1 Lê o CSV
        2 Valida (Quality Gate)
        3 Enriquece com metadados
        4 Carrega válidos na tabela raw principal
        5 Carrega rejeitados na tabela de quarentena

    Args:
        source_file: Caminho para o arquivo CSV de entrada
        project_id: GCP Project ID
        dataset_raw: Dataset BigQuery de destino (camada Raw)
        dry_run: Se True, executa tudo exceto a carga no BQ (util para testes)

    Returns:
        dict com estatisticas da execução
    """
    execution_start = datetime.now(timezone.utc)
    logger.info(json.dumps({
        "event": "ingestion_start",
        "source_file": source_file,
        "project_id": project_id,
        "dry_run": dry_run,
    }))

    try:
        df_raw = pd.read_csv(source_file, dtype=str, keep_default_na=False)
        logger.info(json.dumps({"event": "csv_read", "rows": len(df_raw), "file": source_file}))
    except FileNotFoundError:
        logger.error(json.dumps({"event": "file_not_found", "file": source_file}))
        raise

    df_valid, df_quarantine, stats = validate_dataframe(df_raw)

    #Enriquecimento com metadados

    df_valid = enrich_dataframe(df_valid, source_file)
    if not df_quarantine.empty:
        df_quarantine = enrich_dataframe(df_quarantine, source_file)

    #Carga (pular se dry_run)
    if not dry_run:
        load_to_bigquery(
            df=df_valid,
            project_id=project_id,
            dataset_id=dataset_raw,
            table_id="raw_sfmc_email_logs",
            write_disposition="WRITE_TRUNCATE",  # Idempotente
        )

        # Tabela de quarentena: dados rejeitados (append para histórico)
        if not df_quarantine.empty:
            load_to_bigquery(
                df=df_quarantine,
                project_id=project_id,
                dataset_id=dataset_raw,
                table_id="raw_sfmc_email_logs_quarantine",
                write_disposition="WRITE_APPEND",
            )
    else:
        logger.info(json.dumps({"event": "dry_run_skip_load", "valid_rows": len(df_valid)}))

        if not df_valid.empty:
            print("\n[DRY RUN] Amostra de linhas VÁLIDAS:")
            print(df_valid.head(5).to_string())

        if not df_quarantine.empty:
            print("\n[DRY RUN] Linhas REJEITADAS (quarentena):")
            print(df_quarantine[["log_id", "user_email", "_error_reason"]].to_string())

    # Resultado final
    execution_time_s = (datetime.now(timezone.utc) - execution_start).total_seconds()
    result = {
        **stats,
        "source_file": source_file,
        "execution_time_s": round(execution_time_s, 2),
        "dry_run": dry_run,
        "status": "success",
    }
    logger.info(json.dumps({"event": "ingestion_complete", **result}))
    return result


# =============================================================================
# ENTRYPOINT CLI (compativel com Airflow)
# =============================================================================

def main():
    parser = argparse.ArgumentParser(
        description="Ingestão do SFMC Email Logs com Quality Gate"
    )
    parser.add_argument("--source-file", required=True, help="Caminho do CSV de entrada")
    parser.add_argument("--project-id", required=True, help="GCP Project ID")
    parser.add_argument("--dataset-raw", default="raw", help="Dataset BigQuery (Raw layer)")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Executa validação sem carregar no BigQuery",
    )

    args = parser.parse_args()

    result = ingest_email_logs(
        source_file=args.source_file,
        project_id=args.project_id,
        dataset_raw=args.dataset_raw,
        dry_run=args.dry_run,
    )

    if result["quarantined_rows"] > 0:
        sys.exit(1)


if __name__ == "__main__":
    main()
