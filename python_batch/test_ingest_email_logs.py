"""
Testes unitários para o Quality Gate de ingestão do SFMC Email Logs.
Execute com: pytest test_ingest_email_logs.py -v
"""

import json
import pytest
import pandas as pd
from ingest_email_logs import validate_json_field, validate_dataframe

# Fixtures

def make_row(message_details) -> pd.Series:
    return pd.Series({
        "event_id": "E001",
        "user_email": "test@example.com",
        "event_type": "open",
        "event_timestamp": "2024-01-15 08:30:00",
        "message_details": message_details,
    })


# Testes de validate_json_field

class TestValidateJsonField:

    def test_valid_json_with_campaign_code(self):
        row = make_row('{"campaign_code": "CAMP_CARTAO_JAN", "template_id": "T001"}')
        result = validate_json_field(row)
        assert result["is_valid"] is True
        assert result["error_reason"] is None
        assert result["parsed_json"]["campaign_code"] == "CAMP_CARTAO_JAN"

    def test_null_message_details(self):
        row = make_row(None)
        result = validate_json_field(row)
        assert result["is_valid"] is False
        assert "null" in result["error_reason"].lower()

    def test_empty_string(self):
        row = make_row("")
        result = validate_json_field(row)
        assert result["is_valid"] is False

    def test_malformed_json(self):
        row = make_row("{campaign_code: CAMP_CARTAO_JAN BROKEN")
        result = validate_json_field(row)
        assert result["is_valid"] is False
        assert "JSONDecodeError" in result["error_reason"]

    def test_valid_json_missing_campaign_code(self):
        row = make_row('{"template_id": "T001", "version": 2}')
        result = validate_json_field(row)
        assert result["is_valid"] is False
        assert "campaign_code" in result["error_reason"]

    def test_valid_minimal_json(self):
        row = make_row('{"campaign_code": "CAMP_X"}')
        result = validate_json_field(row)
        assert result["is_valid"] is True

    def test_pd_nan(self):
        row = make_row(float("nan"))
        result = validate_json_field(row)
        assert result["is_valid"] is False


# Testes de validate_dataframe

class TestValidateDataframe:

    def make_df(self, rows: list[dict]) -> pd.DataFrame:
        return pd.DataFrame(rows)

    def test_all_valid(self):
        df = self.make_df([
            {"event_id": "E001", "user_email": "a@b.com", "event_type": "open",
             "event_timestamp": "2024-01-15 08:00:00",
             "message_details": '{"campaign_code": "CAMP_A"}'},
            {"event_id": "E002", "user_email": "c@d.com", "event_type": "click",
             "event_timestamp": "2024-01-15 09:00:00",
             "message_details": '{"campaign_code": "CAMP_B", "v": 1}'},
        ])
        df_valid, df_quarantine, stats = validate_dataframe(df)
        assert len(df_valid) == 2
        assert len(df_quarantine) == 0
        assert stats["quarantined_rows"] == 0

    def test_mixed_valid_invalid(self):
        df = self.make_df([
            {"event_id": "E001", "user_email": "a@b.com", "event_type": "open",
             "event_timestamp": "2024-01-15 08:00:00",
             "message_details": '{"campaign_code": "CAMP_A"}'},
            {"event_id": "E002", "user_email": "x@y.com", "event_type": "open",
             "event_timestamp": "2024-01-15 09:00:00",
             "message_details": "BROKEN JSON {{{}"},
        ])
        df_valid, df_quarantine, stats = validate_dataframe(df)
        assert len(df_valid) == 1
        assert len(df_quarantine) == 1
        assert df_quarantine.iloc[0]["event_id"] == "E002"
        assert "JSONDecodeError" in df_quarantine.iloc[0]["_error_reason"]

    def test_missing_required_column_raises(self):
        df = pd.DataFrame([{"event_id": "E001", "user_email": "a@b.com"}])
        with pytest.raises(ValueError, match="colunas obrigatórias"):
            validate_dataframe(df)

    def test_quarantine_rate_calculation(self):
        rows = []
        for i in range(8):
            rows.append({
                "event_id": f"E{i:03d}", "user_email": f"u{i}@x.com",
                "event_type": "open", "event_timestamp": "2024-01-15 08:00:00",
                "message_details": '{"campaign_code": "CAMP_OK"}'
            })
        for i in range(8, 10):
            rows.append({
                "event_id": f"E{i:03d}", "user_email": f"u{i}@x.com",
                "event_type": "open", "event_timestamp": "2024-01-15 08:00:00",
                "message_details": None
            })
        df = pd.DataFrame(rows)
        _, _, stats = validate_dataframe(df)
        assert stats["quarantine_rate_pct"] == 20.0
        assert stats["valid_rows"] == 8
