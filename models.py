# models.py — Pydantic schemas for Reckon Desktop → Reckon One migration
from pydantic import BaseModel, Field
from typing   import Literal, Optional
from enum     import Enum


# ── Mapping ───────────────────────────────────────────────────────────────────
class MappingStatus(str, Enum):
    approved = "approved"
    review   = "review"
    error    = "error"


class AccountMapping(BaseModel):
    source_code  : str
    source_name  : str
    target_code  : Optional[str] = None
    target_name  : Optional[str] = None
    confidence   : int = Field(ge=0, le=100)
    reasoning    : str
    status       : MappingStatus = MappingStatus.review
    reckon_type  : Optional[str] = None   # mapped Reckon One account type
    rules_applied: list[str] = Field(default_factory=list)

    def compute_status(self) -> "AccountMapping":
        if self.confidence >= 85:
            self.status = MappingStatus.approved
        elif self.confidence >= 60:
            self.status = MappingStatus.review
        else:
            self.status = MappingStatus.error
        return self


# ── Anomaly ───────────────────────────────────────────────────────────────────
class AnomalySeverity(str, Enum):
    high   = "high"
    medium = "medium"
    low    = "low"


class Anomaly(BaseModel):
    ref                : str
    issue_type         : Literal[
        "Duplicate", "Cutoff risk", "Period mismatch", "Interco risk",
        "Inactive_account_in_tx", "GST_rounding", "Multi_currency_unconverted",
        "Invoice_over_75_lines", "Blank_reference", "Duplicate_ref",
        "Special_chars_in_ref", "Due_before_invoice", "Zero_qty_with_amount",
        "Bank_account_in_item", "Other"
    ]
    severity           : AnomalySeverity
    finding            : str
    recommended_action : str


class AnomalyReport(BaseModel):
    anomalies    : list[Anomaly]
    total_high   : int
    total_medium : int
    summary      : str


# ── Reconciliation ────────────────────────────────────────────────────────────
class ReconciliationResult(BaseModel):
    overall_status : Literal["PASSED", "REVIEW", "FAILED"]
    risk_level     : Literal["low", "medium", "high"]
    net_variance   : int
    summary        : str
    next_steps     : list[str]


# ── Entity transform results ──────────────────────────────────────────────────
class TransformResult(BaseModel):
    """Result of a deterministic entity transformation."""
    entity_type    : Literal["coa", "tax", "customer", "supplier", "item", "invoice", "journal"]
    total_records  : int
    rules_triggered: int           # total rule applications across all records
    issues         : list[dict]    # from validate_migration_readiness
    records        : list[dict]    # transformed output


class ValidationIssue(BaseModel):
    type    : Literal["error", "warning", "info"]
    field   : str
    message : str
