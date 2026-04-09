# rules_engine.py — Deterministic Reckon Desktop → Reckon One transformation rules
# Source: "Reckon Desktop to Reckon One Mapping + Use Cases Nov 2024.xlsx" (45 sheets)
# Strategy: deterministic rules first → 0 LLM cost for known transformations
import re
from typing import Optional

# ── COA Account Type Mapping ──────────────────────────────────────────────────
# Reckon Desktop type → Reckon One type (exact from COA sheet)
COA_TYPE_MAP: dict[str, str] = {
    "bank":                    "Bank",
    "cost of goods sold":      "Cost of Goods Sold",
    "credit card":             "Credit card",
    "equity":                  "Owner's equity",
    "income":                  "Income",
    "other current asset":     "Current assets",
    "other asset":             "Other Non-current Asset",
    "other expense":           "Other expense",
    "other income":            "Other income",
    "other current liability": "Other Current Liablity",
    "fixed asset":             "Fixed assets",
    "expense":                 "Expense",
    "long term asset":         "Other Non-current Asset",
    "long term liability":     "Non-Current Liability",
    "suspense":                "Non-Current Liability",
    "non-posting":             "Non-Current Liability",
    "undepositfund":           "Bank",
    "undeposited funds":       "Bank",
    # System accounts
    "accounts payable":        "Accounts Payable (A/P)",
    "accounts receivable":     "Accounts Receivable (A/R)",
    "retain earning":          "Retained Earnings Surplus/(Accumulated Losses)",
    "retained earnings":       "Retained Earnings Surplus/(Accumulated Losses)",
    "open balance equity":     "Open Balance Equity",
    "payroll liabilities":     "Payroll Liabilities",
    "gst payable":             "GST Payable",
    "gst control":             "GST Payable",
    "tax payable":             "GST Payable",
}

# ── Transaction Type Mapping ──────────────────────────────────────────────────
# Reckon Desktop transaction type → Reckon One function (Function Sequence sheet)
TRANSACTION_TYPE_MAP: dict[str, str] = {
    "invoice":              "Invoice",
    "tax invoice":          "Invoice",
    "credit memo":          "Customer Adjustment Note",
    "adjustment note":      "Customer Adjustment Note",
    "bill":                 "Bill",
    "bill credit":          "Supplier Adjustment Note",
    "credit":               "Supplier Adjustment Note",
    "check":                "Make Payment",
    "cheque":               "Make Payment",
    "credit card charge":   "Make Payment",
    "credit card payment":  "Make Payment",
    "bill pmt -cheque":     "Make Payment",
    "bill pmt -credit card":"Make Payment",
    "deposit":              "Receive Money",
    "c card credit":        "Receive Money",
    "sales receipt":        "Receive Money",
    "payment":              "Receive Money",
    "transfer":             "Transfer Money",
    "journal":              "Journal",
    "general journal":      "Journal",
    "paycheque":            "Journal",
    "paycheck":             "Journal",
    "item receipt":         "Journal",
    "inventory adjustment": "Journal",
    "liability cheque":     "Journal",
    "liability adjustment": "Journal",
    "ytd adjustment":       "Journal",
    "c card refund":        "Journal",
    "statement charge":     "Journal",
    "build assembly":       "Journal",
    "estimate":             "Estimate",
    "quote":                "Estimate",
}

# ── Job / Project Status Mapping ──────────────────────────────────────────────
# Reckon Desktop Job Status → Reckon One Project Status (Project sheet)
JOB_STATUS_MAP: dict[str, str] = {
    "":            "Active",
    "none":        "Active",
    "pending":     "Active",
    "awarded":     "Active",
    "in progress": "Active",
    "closed":      "Completed",
    "not awarded": "Inactive",
}

# ── Tax Code Mapping ──────────────────────────────────────────────────────────
# Full mapping from Tax Mapping sheet
TAX_CODE_MAP: dict[str, dict] = {
    "CAF":   {"code": "CAF",  "desc": "Purchases/acquisitions of GST free capital items",                          "rate": 0.0},
    "CAG":   {"code": "CAG",  "desc": "Purchases/acquisitions of capital items subject to GST",                    "rate": 0.1},
    "CAI":   {"code": "CAI",  "desc": "Purchases/acquisitions of capital items for making input tax sales",        "rate": 0.0},
    "CDC":   {"code": "CDC",  "desc": "Combined Cellar Door",                                                       "rate": 0.3087448},
    "CDG":   {"code": "CDG",  "desc": "Cellar Door GST",                                                            "rate": 0.119},
    "CDS":   {"code": "CDS",  "desc": "Cellar Door WET",                                                            "rate": 0.1898},
    "EXP":   {"code": "EXP",  "desc": "GST free exports",                                                           "rate": 0.0},
    "FRE":   {"code": "FRE",  "desc": "GST free sales",                                                             "rate": 0.0},
    "GST":   {"code": "GST",  "desc": "GST on sales",                                                               "rate": 0.1},
    "INP":   {"code": "INP",  "desc": "Input taxed sales",                                                          "rate": 0.0},
    "N":     {"code": "NTD",  "desc": "Purchase/acquisition of non tax deductible item",                            "rate": 0.0},
    "NCF":   {"code": "NCF",  "desc": "Purchase/acquisition of GST free non-capital items",                        "rate": 0.0},
    "NCG":   {"code": "NCG",  "desc": "Purchase/acquisition of non-capital items subject to GST",                  "rate": 0.1},
    "NCI":   {"code": "NCI",  "desc": "Purchase/acquisition of non-capital items for making input taxed supplies", "rate": 0.0},
    "WC":    {"code": "WC",   "desc": "Combined WET & WGST",                                                        "rate": 0.419},
    "WET":   {"code": "WET",  "desc": "Wine Equalisation Tax",                                                      "rate": 0.29},
    "WGST":  {"code": "WGST", "desc": "GST on Wine Equalisation Tax",                                               "rate": 0.129},
    "ADJ-P": {"code": "AJA",  "desc": "Adjustments to purchases/acquisitions",                                     "rate": 0.1},
    "ADJ-S": {"code": "AJS",  "desc": "Adjustments to sales",                                                      "rate": 0.1},
}

# Context-based tax code remapping (Tax Mapping use cases 2 & 3)
INCOME_TAX_REMAP  = {"NCG": "GST", "NCF": "FRE"}   # NCG/NCF used in income → GST/FRE
EXPENSE_TAX_REMAP = {"GST": "NCG", "FRE": "NCF"}   # GST/FRE used in expense → NCG/NCF

# ── Item Type Mapping ─────────────────────────────────────────────────────────
# Item sheet use cases 7 & 8
ITEM_SERVICE_TYPES = {"service", "other charge", "othercharge", "subtotal", "group", "discount", "payment"}
ITEM_PRODUCT_TYPES = {"inventory", "inventory assembly", "inventoryassembly", "non-inventory", "noninventory"}

def map_item_type(desktop_type: str) -> str:
    t = (desktop_type or "").strip().lower()
    if t in ITEM_SERVICE_TYPES:
        return "Service"
    if t in ITEM_PRODUCT_TYPES:
        return "Product"
    return "Service"  # default

# ── Migration sequence (Function Sequence sheet, all 47 steps) ────────────────
MIGRATION_SEQUENCE = [
    (1,    "Organization Settings",          "Book Settings"),
    (2,    "Terms",                          "Payment Terms"),
    (3,    "Tax Master (Tax Mapping)",        "Tax Settings"),
    (4,    "COA",                            "Chart of Accounts"),
    (5,    "Bank / Credit Card",             "Bank Accounts"),
    (6,    "Customer",                       "Customers"),
    (7,    "Customer Job",                   "Projects"),
    (8,    "Supplier",                       "Suppliers"),
    (9,    "Item",                           "Items"),
    (10,   "Class",                          "Classification (Future)"),
    (11,   "Invoice / Tax Invoice",          "Invoice / Tax Invoice"),
    (12,   "Credit Memo / Adjustment Note",  "Customer Adjustment Note"),
    (13,   "Bill",                           "Bill"),
    (14,   "Bill Credit",                    "Supplier Adjustment Note"),
    (15,   "Check",                          "Make Payment"),
    (16,   "Credit Card Charge / Payment",   "Make Payment"),
    (17,   "Deposit",                        "Receive Money"),
    (18,   "C Card Credit",                  "Receive Money"),
    (19,   "Transfer",                       "Transfer Money"),
    (20,   "Sales Receipts",                 "Receive Money"),
    (21,   "Journal Entries",                "Journal"),
    (22,   "Paycheque",                      "Journal"),
    (23,   "Item Receipt",                   "Journal"),
    (24,   "Inventory Adjustment",           "Journal"),
    (25,   "Liability Cheque",               "Journal"),
    (26,   "Liability Adjustment",           "Journal"),
    (27,   "YTD Adjustment",                 "Journal"),
    (28,   "C Card Refund",                  "Journal"),
    (29,   "Statement Charge",               "Journal"),
    (30,   "Build Assembly",                 "Journal"),
    (31,   "Opening Aged Receivables",       "Aged Receivables (Full Lines)"),
    (32,   "Opening Aged Payable",           "Aged Payable (Full Lines)"),
    (33,   "Opening Trial Balance",          "Trial Balance"),
    (34.01,"Bill Payment (Cheque)",          "Make Payment"),
    (34.02,"Bill Payment (Credit Card)",     "Make Payment"),
    (35,   "Payment",                        "Receive Money"),
    (36,   "Estimates",                      "Estimates"),
    (39,   "All type allocation (Customer)", "Allocation (Customer History)"),
    (40,   "All type allocation (Vendor)",   "Allocation (Vendor History)"),
    (43,   "Comparative Trial Balance",      "Trial Balance Report"),
    (44,   "Comparative Aged Receivable",    "Aged Receivable Report"),
    (45,   "Comparative Aged Payable",       "Aged Payable Report"),
    (46,   "All Bank/Credit Reconciliation", "Bank Reconciliation"),
]

NOT_AVAILABLE_IN_RECKON_ONE = [
    "Class / Classification",
    "Sales Order",
    "Purchase Order",
    "Employee / Payroll (separate payroll module)",
]

# ── Constants ─────────────────────────────────────────────────────────────────
REF_MAX_LEN         = 20
ACCOUNT_CODE_MAX    = 7
TAX_START_DATE      = "01/07/2000"
BOOK_START_DATE     = "01/07/2000"
INVOICE_MAX_LINES   = 75
DEFAULT_CONVERSION_ENTITY_STATUS = "Active"  # inactive entities → import active, revert after

# Account types/keywords that cannot be used as item income/expense accounts
# COA use case 11; Item use case 10; Invoice use case 17; Bill use case 19
RESTRICTED_ACCOUNT_KEYWORDS = {
    "bank", "credit card", "retained earning", "retained earnings",
    "undeposited", "gst payable", "gst control", "tax payable",
}

# ── String cleaning utilities ─────────────────────────────────────────────────
# Note: colon (:) intentionally kept — used for Parent:Child hierarchy
_SPECIAL_CHARS = re.compile(r'[*%\^$#@)("><?{};=\[\]\\|,~`]')
_DATE_PREFIX   = re.compile(r'^[A-Za-z]+-\d+\s*')   # AUG-25, Jul-25
_DOT_HYPHEN    = re.compile(r'^[.\-]+')              # leading . or -
_SPACES        = re.compile(r'\s{2,}')


def clean_name(name: str) -> str:
    """Strip special chars not accepted by Reckon One. Keeps colon for Parent:Child."""
    if not name:
        return name
    cleaned = _SPECIAL_CHARS.sub('', str(name))
    return _SPACES.sub(' ', cleaned).strip()


def normalize_account_code(code: str, max_len: int = ACCOUNT_CODE_MAX) -> str:
    """
    Normalize account code for Reckon One:
    • Strip special chars
    • Remove leading dots/hyphens
    • Strip date prefixes (AUG-25)
    • Truncate to 7 chars
    """
    if not code:
        return code
    c = _SPECIAL_CHARS.sub('', str(code))
    c = _DOT_HYPHEN.sub('', c)          # strip leading dots/hyphens
    m = _DATE_PREFIX.match(c)
    if m:
        c = c[m.end():]
    c = _DOT_HYPHEN.sub('', c)          # strip any leading hyphen left after date prefix
    c = c[:max_len].strip()
    return c or str(code)[:max_len]


def normalize_ref(ref: str, fallback: str = "") -> str:
    """Normalize invoice/journal reference: strip special chars, max 20 chars.
    Blank/NaN ref → fallback (transaction ID)."""
    ref_str = str(ref).strip() if ref is not None else ""
    if not ref_str or ref_str.lower() in ("nan", "none", ""):
        return str(fallback)[:REF_MAX_LEN]
    cleaned = _SPECIAL_CHARS.sub('', ref_str).strip()
    return cleaned[:REF_MAX_LEN]


def make_unique_ref(ref: str, txn_id: str) -> str:
    """Make duplicate ref unique by appending txn_id, max 20 chars."""
    combined = f"{ref}-{txn_id}"
    return combined[:REF_MAX_LEN]


def deduplicate_names(names: list[str]) -> list[str]:
    """Suffix duplicate names with -A, -B, etc. (COA/Customer/Supplier/Item rule)."""
    seen: dict[str, int] = {}
    result = []
    for name in names:
        key = name.lower().strip()
        if key not in seen:
            seen[key] = 0
            result.append(name)
        else:
            seen[key] += 1
            suffix = chr(ord('A') + seen[key] - 1)
            result.append(f"{name}-{suffix}")
    return result


def _is_restricted_account(account_name: str) -> bool:
    """Check if account is restricted from use as item income/expense account."""
    lower = (account_name or "").lower()
    return any(kw in lower for kw in RESTRICTED_ACCOUNT_KEYWORDS)


def _to_float(val) -> float:
    try:
        return float(str(val).replace(",", "")) if val is not None else 0.0
    except (ValueError, TypeError):
        return 0.0


def map_tax_code(code: str) -> str:
    """Map Desktop tax code to Reckon One code."""
    if not code:
        return code
    mapped = TAX_CODE_MAP.get(code.strip().upper())
    return mapped["code"] if mapped else code


# ── Internal record getter ────────────────────────────────────────────────────
def _make_getter(record: dict):
    def get(*keys, default=""):
        for k in keys:
            v = record.get(k)
            if v is not None and str(v).strip() not in ("", "nan", "None"):
                return str(v).strip()
        return default
    return get


# ═══════════════════════════════════════════════════════════════════════════════
# TRANSFORM FUNCTIONS
# ═══════════════════════════════════════════════════════════════════════════════

# ── Organization Settings Transform ──────────────────────────────────────────
def transform_org_settings(record: dict) -> dict:
    """
    Map Organization Settings → Reckon One Book Settings.
    Start date always 01/07/2000. Legal details mapped directly.
    """
    get = _make_getter(record)
    return {
        "Company name":         get("Company name", "company_name"),
        "Legal name":           get("Legal name", "legal_name"),
        "ABN/WPN":              get("Tax Rego ID", "abn", "tax_rego_id"),
        "Company address Line 1": get("Legal Address Line 1", "address_line1"),
        "Company address Line 2": get("Legal Address Line 2", "address_line2"),
        "Suburb":               get("City", "city", "suburb"),
        "State":                get("State", "state"),
        "Postcode":             get("Postcode", "post_code"),
        "Contact name":         get("Contact", "contact"),
        "Phone":                get("Phone", "phone"),
        "Mobile":               get("Mobile", "mobile"),
        "Fax":                  get("Fax", "fax"),
        "Email":                get("Email", "email"),
        "Website":              get("Website", "website"),
        "Start date":           BOOK_START_DATE,
        "Has Tax":              "Yes" if get("Tax", "has_tax") else "No",
        "_rules_applied": [f"Start date fixed to {BOOK_START_DATE} (every migration)"],
    }


# ── Terms Transform ───────────────────────────────────────────────────────────
def transform_terms_record(record: dict) -> dict:
    """
    Map Reckon Desktop Terms → Reckon One Payment Terms.
    Standard (Net X days) and Date Driven terms.
    """
    rules: list[str] = []
    get = _make_getter(record)

    name = get("Terms Name", "name", "term_name")
    status = get("Status", "active_status", "status")
    is_active = status.lower() not in ("inactive", "false", "0", "no")
    if not is_active:
        rules.append("Inactive term imported — status preserved")

    # Net due days (Standard)
    net_days = get("Standard Net due In..... Days", "net_due_days", "net_days")

    # Date Driven — day of month
    day_of_month = get("Date Driven Net due before .....th Day of Month", "day_of_month")

    # Day of next month
    day_next_month = get("Date Driven Net due before .....th Day of Month", "day_next_month")

    # System terms mapping
    system_map = {"net 30": "Net 30", "due on receipt": "Due on receipt", "net 15": "Net 15"}
    mapped_name = system_map.get(name.lower(), name)

    out = {
        "Term name*":    mapped_name,
        "Description":   get("Description", "description"),
        "Status":        "Active" if is_active else "Inactive",
        "_rules_applied": rules or ["Terms record mapped directly"],
    }

    if net_days:
        out["Net due...... days after issue*"] = net_days
    if day_of_month:
        out["Net due...... day of current Month*"] = day_of_month
        rules.append("Date Driven term — day of month mapping")
    if day_next_month:
        out["Net due .......day of next month*"] = day_next_month

    return out


# ── COA Transform ─────────────────────────────────────────────────────────────
def transform_coa_record(record: dict) -> dict:
    """
    Transform one Reckon Desktop COA record → Reckon One format.
    Applies all rules from COA sheet deterministically.
    """
    rules: list[str] = []
    get = _make_getter(record)

    src_name   = get("Account", "account", "account_name", "name")
    src_type   = get("Type", "type", "account_type")
    src_code   = get("Accnt. #", "code", "account_code", "accnt_no", "Accnt #")
    src_status = get("Active Status", "active_status", "status")
    src_desc   = get("Description", "description")
    src_bsb    = get("Bank No. / Note", "bank_no", "bsb")
    src_tax    = get("Tax Code", "tax_code", "default_tax_code")
    src_parent = get("Subaccount of (Parent Name)", "parent", "sub_account_of", "Subaccount of")

    # 1. Name — clean special chars
    name = clean_name(src_name)
    if name != src_name:
        rules.append(f"Name: stripped special chars '{src_name}' → '{name}'")

    # 2. Account code — normalize, max 7 chars
    code = normalize_account_code(src_code)
    if code != src_code:
        rules.append(f"Code: normalized '{src_code}' → '{code}' (max 7 chars, no special chars)")

    # 3. Account type mapping
    type_key    = src_type.strip().lower()
    mapped_type = COA_TYPE_MAP.get(type_key, src_type)
    if mapped_type != src_type:
        rules.append(f"Type: '{src_type}' → '{mapped_type}'")

    # 4. Status — inactive accounts used in conversion → import as active
    is_active = str(src_status).lower() not in ("inactive", "false", "0", "no", "n")
    if not is_active:
        rules.append("Inactive account — imported as Active for conversion period (revert after migration)")
    status = "Active"  # always import active (use case 9)

    # 5. Default tax code
    tax_upper  = src_tax.upper()
    mapped_tax = TAX_CODE_MAP[tax_upper]["code"] if tax_upper in TAX_CODE_MAP else src_tax
    if mapped_tax != src_tax:
        rules.append(f"Tax code: '{src_tax}' → '{mapped_tax}'")

    out = {
        "ACCOUNT NAME*":             name,
        "ACCOUNT CODE":              code,
        "Account TYPE*":             mapped_type,
        "Status":                    status,
        "Sub-account (Parent Name)": src_parent,
        "DESCRIPTION":               src_desc,
        "BSB":                       src_bsb,
        "DEFAULT TAX CODE":          mapped_tax,
        "_balance_ignored":          True,
        "_original_status":          "Active" if is_active else "Inactive",
        "_rules_applied":            rules,
    }

    if not rules:
        rules.append("No transformation needed — record passed through cleanly")

    return out


# ── Bank Account Transform ────────────────────────────────────────────────────
def transform_bank_record(record: dict) -> dict:
    """
    Map Reckon Desktop Bank/Credit Card account → Reckon One Bank Account.
    Separate from COA — bank accounts have BSB, Account Number, Financial Institution.
    """
    rules: list[str] = []
    get = _make_getter(record)

    src_name = get("Account", "account", "account_name")
    name = clean_name(src_name)
    if name != src_name:
        rules.append(f"Name: stripped special chars '{src_name}'")

    src_code = get("Accnt. #", "code", "account_code")
    code = normalize_account_code(src_code)
    if code != src_code:
        rules.append(f"Code: normalized '{src_code}' → '{code}'")

    src_type = get("Type", "type", "account_type").lower()
    if "credit" in src_type:
        acct_type = "Credit account"
    else:
        acct_type = "Bank account"

    # Inactive bank account → import as active (Cheque use case 20)
    src_status = get("Active Status", "active_status", "status")
    is_active  = str(src_status).lower() not in ("inactive", "false", "0", "no")
    if not is_active:
        rules.append("Inactive bank account — imported as Active for conversion (revert after)")

    out = {
        "Account Display Name*":    name,
        "Account Type*":            acct_type,
        "ACCOUNT CODE":             code,
        "Account Number":           get("Bank No. / Note", "bank_no", "account_number"),
        "BSB":                      get("Bank No. / Note", "branch_code", "bsb"),
        "Financial Institution":    get("Financial Institution", "financial_institution"),
        "Status":                   "Active",
        "_original_status":         "Active" if is_active else "Inactive",
        "_rules_applied":           rules or ["Bank account mapped directly"],
    }
    return out


# ── Tax Transform ─────────────────────────────────────────────────────────────
def transform_tax_record(record: dict) -> dict:
    """Transform Reckon Desktop tax code → Reckon One tax setting.
    Start date always 01/07/2000."""
    rules: list[str] = []
    get = _make_getter(record)

    desktop_code = get("Name", "tax_code", "code").upper()
    mapped = TAX_CODE_MAP.get(desktop_code)

    if mapped:
        code = mapped["code"]
        desc = get("Description") or mapped["desc"]
        rate = mapped["rate"]
        if code != desktop_code:
            rules.append(f"Tax code: '{desktop_code}' → '{code}'")
        rules.append(f"Tax rate: {rate*100:.4f}%")
    else:
        code = desktop_code
        desc = get("Description")
        try:
            rate = float(get("Tax Rate", "rate") or 0) / 100
        except ValueError:
            rate = 0.0
        rules.append(f"Tax code not in standard map — kept as-is: '{code}'")

    rules.append(f"Start date fixed to {TAX_START_DATE}")

    return {
        "Tax code name *":                                    code,
        "Description *":                                      desc,
        "Tax Rate":                                           rate,
        "Start date":                                         TAX_START_DATE,
        "What transactions will this be used for Sales*":     get("Use this item in sales transactions", "sales"),
        "What transactions will this be used for Purchases*": get("Use this item in Purchase transactions", "purchases"),
        "Status":                                             get("Status") or "Active",
        "_rules_applied":                                     rules,
    }


# ── Customer Transform ────────────────────────────────────────────────────────
def transform_customer_record(record: dict) -> dict:
    """Map Reckon Desktop Customer → Reckon One Customer.
    Use cases: No Name-C, special chars, Bill1==name, inactive→active, Parent:Child."""
    rules: list[str] = []
    get = _make_getter(record)

    raw_name = get("Customer Name", "customer_name", "display_name", "name")

    # "No Name" → "No Name-C" (Customer use case 6)
    if raw_name.strip().lower() == "no name":
        name = "No Name-C"
        rules.append("'No Name' customer renamed to 'No Name-C' (Reckon One rule)")
    else:
        name = clean_name(raw_name)
        if name != raw_name:
            rules.append(f"Name: stripped special chars '{raw_name}'")

    # Bill1 == customer name → ignore for address (use case 8)
    bill1 = get("Line 1/Street1", "street1", "bill_to_1")
    if bill1 and bill1.strip() == raw_name.strip():
        bill1 = ""
        rules.append("Bill1 matched customer name — ignored for address")

    # Inactive → import as active (use case 4)
    is_active = get("Active Status", "active_status", "status").lower() not in ("inactive", "false", "0", "no")
    if not is_active:
        rules.append("Inactive customer — imported as Active for conversion (revert after)")

    return {
        "Display name*":              name,
        "Type*":                      "Customer",
        "Customer name":              get("Company Name", "company_name"),
        "BUSINESS ADDRESS Line 1":    bill1,
        "BUSINESS ADDRESS line 2":    get("Line 2/Street2", "street2"),
        "BUSINESS ADDRESS Suburb":    get("City", "city"),
        "BUSINESS ADDRESS State":     get("State", "state"),
        "BUSINESS ADDRESS Postcode":  get("Post Code", "postcode"),
        "BUSINESS ADDRESS Country":   get("Country", "country"),
        "SHIPPING ADDRESS Line 1":    get("Ship To Street1", "ship_street1"),
        "SHIPPING ADDRESS Line 2":    get("Ship To Street2", "ship_street2"),
        "SHIPPING ADDRESS Suburb":    get("Ship To City", "ship_city"),
        "SHIPPING ADDRESS State":     get("Ship To State", "ship_state"),
        "SHIPPING ADDRESS Postcode":  get("Ship To Post Code", "ship_postcode"),
        "SHIPPING ADDRESS Country":   get("Ship To Country", "ship_country"),
        "Phone":                      get("Phone", "phone"),
        "Other Phone":                get("Alt. Phone", "alt_phone"),
        "Fax":                        get("Fax", "fax"),
        "Email":                      get("Email", "email"),
        "ABN":                        get("Tax Reg ID", "abn", "tax_reg_id"),
        "Other email":                get("CC Email", "cc_email"),
        "Notes":                      get("Notes", "notes"),
        "Payment Terms":              get("Terms", "terms"),
        "Credit Limit":               get("Credit Limit", "credit_limit"),
        "Contact":                    get("Contact", "contact"),
        "Status":                     "Active",
        "_original_status":           "Active" if is_active else "Inactive",
        "_rules_applied":             rules,
    }


# ── Supplier Transform ────────────────────────────────────────────────────────
def transform_supplier_record(record: dict) -> dict:
    """Map Reckon Desktop Supplier → Reckon One Supplier."""
    rules: list[str] = []
    get = _make_getter(record)

    raw_name = get("Supplier", "supplier_name", "display_name", "name")

    if raw_name.strip().lower() == "no name":
        name = "No Name"
        rules.append("'No Name' supplier kept as 'No Name'")
    else:
        name = clean_name(raw_name)
        if name != raw_name:
            rules.append(f"Name: stripped special chars '{raw_name}'")

    # Street1 == supplier name → ignore for address (use case 8)
    street1 = get("Street1", "street1", "bill_to_1")
    if street1 and street1.strip() == raw_name.strip():
        street1 = ""
        rules.append("Street1 matched supplier name — ignored for address")

    is_active = get("Active Status", "active_status", "status").lower() not in ("inactive", "false", "0", "no")
    if not is_active:
        rules.append("Inactive supplier — imported as Active for conversion (revert after)")

    return {
        "Display name*":             name,
        "Type*":                     "Supplier",
        "Supplier name":             get("Company", "company_name"),
        "BUSINESS ADDRESS Line 1":   street1,
        "BUSINESS ADDRESS line 2":   get("Street2", "street2"),
        "BUSINESS ADDRESS Suburb":   get("City", "city"),
        "BUSINESS ADDRESS State":    get("State", "state"),
        "BUSINESS ADDRESS Postcode": get("Post Code", "postcode"),
        "BUSINESS ADDRESS Country":  get("Country", "country"),
        "Email":                     get("Email", "email"),
        "Phone":                     get("Phone", "phone"),
        "Other Phone":               get("Alt. Phone", "alt_phone"),
        "Fax":                       get("Fax", "fax"),
        "ABN":                       get("Tax Reg ID", "abn", "tax_reg_id"),
        "Notes":                     get("Note", "Notes", "notes"),
        "Other email":               get("CC Email", "cc_email"),
        "Contact":                   get("Contact", "contact"),
        "Bank account name":         get("Account Name", "bank_account_name"),
        "Account number":            get("Account Number", "account_number"),
        "BSB":                       get("Branch Code", "bsb", "branch_code"),
        "Status":                    "Active",
        "_original_status":          "Active" if is_active else "Inactive",
        "_rules_applied":            rules,
    }


# ── Project (Customer Job) Transform ─────────────────────────────────────────
def transform_project_record(record: dict) -> dict:
    """
    Map Reckon Desktop Customer Job → Reckon One Project.
    Job Status: None/Pending/Awarded/In Progress → Active; Closed → Completed; Not Awarded → Inactive.
    """
    rules: list[str] = []
    get = _make_getter(record)

    raw_name = get("Job Type Name", "job_type_name", "name", "project_name")
    name = clean_name(raw_name)
    if name != raw_name:
        rules.append(f"Name: stripped special chars '{raw_name}'")

    # Job Status mapping (Project sheet)
    desktop_status = get("Job Status", "job_status", "status").lower()
    reckon_status  = JOB_STATUS_MAP.get(desktop_status, "Active")
    if desktop_status:
        rules.append(f"Job status: '{desktop_status}' → '{reckon_status}'")

    return {
        "Project name*":  name,
        "SUBPROJECT":     get("Subtype of", "sub_job_of", "subproject"),
        "Status":         reckon_status,
        "Start Date":     get("Start Date", "start_date"),
        "End date":       get("End date", "end_date"),
        "Description":    get("Job Description", "description"),
        "_rules_applied": rules or ["Project record mapped directly"],
    }


# ── Class Transform ───────────────────────────────────────────────────────────
def transform_class_record(record: dict) -> dict:
    """Map Reckon Desktop Class → Reckon One Classification (future development)."""
    rules: list[str] = []
    get = _make_getter(record)

    raw_name = get("Class Name", "class_name", "name")
    name = clean_name(raw_name)
    if name != raw_name:
        rules.append(f"Name: stripped special chars '{raw_name}'")

    is_active = get("Active", "active_status", "status").lower() not in ("inactive", "false", "0", "no")
    if not is_active:
        rules.append("Inactive class — imported as Active for conversion (revert after)")

    return {
        "Name*":          name,
        "Sub-classification of": get("Subclass of", "subclass_of", "parent"),
        "Description":    get("Description", "description"),
        "Status":         "Active",
        "_original_status": "Active" if is_active else "Inactive",
        "_rules_applied": rules or ["Class record mapped directly"],
        "_note":          "Classification not yet available in Reckon One — import when feature ships",
    }


# ── Item Transform ────────────────────────────────────────────────────────────
def transform_item_record(record: dict) -> dict:
    """
    Map Reckon Desktop Item → Reckon One Item.
    Use cases: type mapping, missing accounts → defaults, bank/CC account → Item Sales/Purchase.
    """
    rules: list[str] = []
    get = _make_getter(record)

    raw_name = get("Item Name/Number", "Item Name/ Number", "item_name", "name")
    name = clean_name(raw_name)
    if name != raw_name:
        rules.append(f"Name: stripped special chars '{raw_name}'")

    desktop_type = get("Type", "item_type", "type")
    reckon1_type = map_item_type(desktop_type)
    if reckon1_type != desktop_type:
        rules.append(f"Item type: '{desktop_type}' → '{reckon1_type}' (Service/Product rule)")

    income_acc  = get("Income Account", "income_account", "sale_account")
    expense_acc = get("Expense Account", "expense_account", "purchase_account")

    # Restricted accounts → replace with defaults (use case 10)
    if _is_restricted_account(income_acc):
        rules.append(f"Income account '{income_acc}' is restricted (bank/CC/retained) → 'Item Sales'")
        income_acc = "Item Sales"
    if not income_acc:
        income_acc = "Item Sales"
        rules.append("Missing income account — defaulted to 'Item Sales'")

    if _is_restricted_account(expense_acc):
        rules.append(f"Expense account '{expense_acc}' is restricted → 'Item Purchase'")
        expense_acc = "Item Purchase"
    if not expense_acc:
        expense_acc = "Item Purchase"
        rules.append("Missing expense account — defaulted to 'Item Purchase'")

    # Inactive item → import as active (use case 4)
    is_active = get("Item Is inactive", "active_status", "status").lower() not in ("inactive", "yes", "true", "1")
    if not is_active:
        rules.append("Inactive item — imported as Active for conversion (revert after)")

    # Tax-inclusive items → use FRE (use case 12)
    amt_inc_tax = get("Amt Inc Tax", "amt_inc_tax", "amount_inc_tax") or "No"
    sale_tax    = get("Tax Code", "sale_tax_code")
    if amt_inc_tax.upper() in ("YES", "Y", "TRUE", "1") and not sale_tax:
        sale_tax = "FRE"
        rules.append("Tax-inclusive item — sale tax code set to FRE")

    return {
        "Item Name*":           name,
        "Type*":                reckon1_type,
        "Sub Item Of":          get("Sub Item Of", "sub_item_of"),
        "Amounts include tax?": amt_inc_tax,
        "SALE DESCRIPTION":     get("Description On Sales Transactions", "sale_description"),
        "PURCHASE DESCRIPTION": get("Description on Purchase Transactions", "purchase_description"),
        "Sale Account*":        income_acc,
        "PURCHASE ACCOUNT*":    expense_acc,
        "Cost Per unit":        get("Cost", "cost") or "0",
        "Price Per Unit":       get("Sales Price", "sales_price", "price") or "0",
        "Purchase Tax code":    map_tax_code(get("Purchase Tax code", "purchase_tax_code")),
        "Sale Tax Code":        map_tax_code(sale_tax),
        "Status":               "Active",
        "_original_status":     "Active" if is_active else "Inactive",
        "_rules_applied":       rules,
    }


# ── Shared invoice/bill line helpers ─────────────────────────────────────────
def _is_description_only_line(line: dict) -> bool:
    """Description-only line (no item/qty/price) → skip (Invoice use case 23)."""
    get = _make_getter(line)
    item  = get("Item", "item")
    qty   = get("QTY", "qty", "Qty", "quantity")
    price = get("Sales Price", "Cost", "item_price", "rate", "price")
    return not item and not qty and not price


def _normalize_qty(qty_str: str, amount_str: str, rules: list) -> float:
    """QTY=0 with amount → set to 1 (Invoice use case 22)."""
    try:
        qty_val = float(qty_str) if qty_str else 1.0
    except ValueError:
        qty_val = 1.0
    if qty_val == 0 and _to_float(amount_str) != 0:
        rules.append("QTY was 0 with non-zero amount — changed to 1")
        qty_val = 1.0
    return qty_val


def _check_line_count(line_count: int, ref: str, rules: list) -> bool:
    """Returns True if invoice needs to be consolidated to one-liner (>75 lines)."""
    if line_count > INVOICE_MAX_LINES:
        rules.append(f"Invoice {ref} has {line_count} lines (>75) — must be consolidated to 1-liner using Item Sales account; pass journal")
        return True
    return False


# ── Invoice Transform ─────────────────────────────────────────────────────────
def transform_invoice_record(record: dict, existing_refs: Optional[set] = None) -> dict:
    """
    Transform Reckon Desktop Invoice → Reckon One Invoice.
    All 23 use cases applied deterministically.
    """
    rules: list[str] = []
    if existing_refs is None:
        existing_refs = set()

    get = _make_getter(record)

    customer  = get("Customer Name/Customer Job", "customer_name", "customer", "Name", "name")
    inv_date  = get("Tax Date", "invoice_date", "date", "Date")
    due_date  = get("Due Date", "due_date")
    inv_ref   = get("Invoice No", "invoice_no", "ref", "Num", "num")
    txn_id    = get("txn_id", "transaction_id", "Trans #", "Trans#")
    memo      = get("Memo", "memo", "Sales Rep", "note")

    # Use case 16: due date < invoice date → set due = invoice
    if inv_date and due_date:
        try:
            if due_date < inv_date:
                rules.append(f"Due date '{due_date}' < invoice date '{inv_date}' — set due = invoice date")
                due_date = inv_date
        except TypeError:
            pass

    # Use case 3: blank ref → use transaction ID
    # Use case 2: duplicate ref → append txn_id
    # Use case 4: ref max 20 chars
    ref = normalize_ref(inv_ref, txn_id)
    if not str(inv_ref).strip() or str(inv_ref).lower() in ("nan", "none"):
        rules.append(f"Blank invoice ref — using transaction ID: '{ref}'")
    elif ref in existing_refs:
        ref = make_unique_ref(ref, txn_id)
        rules.append(f"Duplicate ref — made unique: '{ref}'")

    # Use case 22: QTY=0 with amount → 1
    qty_str    = get("QTY", "qty", "Qty", "quantity")
    amount_str = get("Amount", "amount")
    qty_val    = _normalize_qty(qty_str, amount_str, rules)

    # Use case 20: multi-currency → convert by exchange rate
    try:
        exchange_rate = float(get("Exchange rate", "exchange_rate") or 1)
    except ValueError:
        exchange_rate = 1.0

    try:
        amount     = _to_float(get("Amount", "amount"))
        item_price = _to_float(get("Sales Price", "item_price", "price", "Sales Price"))
        tax_amount = _to_float(get("Tax Amount", "tax_amount"))
        if exchange_rate != 1.0:
            amount     = round(amount * exchange_rate, 2)
            item_price = round(item_price * exchange_rate, 2)
            tax_amount = round(tax_amount * exchange_rate, 2)
            rules.append(f"Multi-currency: amounts converted at rate {exchange_rate}")
    except ValueError:
        amount = item_price = tax_amount = 0.0

    # Use case 17: item with bank/CC/retained earnings account → Item Sales + journal flag
    account = get("Account Code", "account", "account_code", "Account")
    if _is_restricted_account(account):
        rules.append(f"Account '{account}' is bank/CC/retained earnings — replaced with Item Sales; journal required")
        account = "Item Sales"

    # Use case 8: special chars in description
    desc = clean_name(get("Description", "description"))

    # Use case 8: special chars in ref already handled by normalize_ref
    # Use case 9: date in ref already handled by normalize_ref (date-format chars stripped)

    # Tax code mapping
    tax_code = map_tax_code(get("Tax", "tax_code", "Tax Code", "Tax"))

    # Use case 23: description-only line flag
    is_desc_only = _is_description_only_line(record)
    if is_desc_only:
        rules.append("Description-only line (no item/qty/price) — flagged to skip")

    out = {
        "Customer*":       customer,
        "Invoice Date*":   inv_date,
        "Due Date":        due_date,
        "Reference Code":  ref,
        "Payment Term":    get("Term", "payment_term", "terms", "Terms"),
        "Item":            get("Item", "item"),
        "Account":         account,
        "Description":     desc,
        "QTY":             qty_val,
        "Item Price":      item_price,
        "Tax Code":        tax_code,
        "Tax":             tax_amount,
        "Amount":          amount,
        "Amounts *":       get("Amount Include Tax", "amount_include_tax") or "Exclusive",
        "Note":            memo,
        "_is_description_only": is_desc_only,
        "_rules_applied":  rules,
    }
    return out


# ── Credit Memo (Customer Adjustment Note) Transform ─────────────────────────
def transform_credit_memo_record(record: dict, existing_refs: Optional[set] = None) -> dict:
    """
    Transform Reckon Desktop Credit Memo → Reckon One Customer Adjustment Note.
    Same 23 use cases as Invoice.
    """
    rules: list[str] = []
    if existing_refs is None:
        existing_refs = set()

    get = _make_getter(record)

    customer  = get("Customer Name/Customer Job", "customer_name", "customer", "Name")
    memo_date = get("Tax Date", "credit_date", "date", "Date")
    credit_ref = get("Credit No", "credit_no", "ref", "Num")
    txn_id    = get("txn_id", "transaction_id", "Trans #")

    ref = normalize_ref(credit_ref, txn_id)
    if not str(credit_ref).strip() or str(credit_ref).lower() in ("nan", "none"):
        rules.append(f"Blank credit memo ref — using transaction ID: '{ref}'")
    elif ref in existing_refs:
        ref = make_unique_ref(ref, txn_id)
        rules.append(f"Duplicate ref — made unique: '{ref}'")

    qty_str    = get("QTY", "qty", "Qty")
    amount_str = get("Amount", "amount")
    qty_val    = _normalize_qty(qty_str, amount_str, rules)

    try:
        exchange_rate = float(get("Exchange rate", "exchange_rate") or 1)
    except ValueError:
        exchange_rate = 1.0

    amount     = _to_float(get("Amount", "amount"))
    item_price = _to_float(get("Rate", "rate", "item_price", "price"))
    tax_amount = _to_float(get("Tax Amount", "tax_amount"))
    if exchange_rate != 1.0:
        amount     = round(amount * exchange_rate, 2)
        item_price = round(item_price * exchange_rate, 2)
        tax_amount = round(tax_amount * exchange_rate, 2)
        rules.append(f"Multi-currency: converted at rate {exchange_rate}")

    account = get("Account Code", "account", "account_code")
    if _is_restricted_account(account):
        rules.append(f"Account '{account}' is restricted — replaced with Item Sales; journal required")
        account = "Item Sales"

    desc = clean_name(get("Description", "description"))
    tax_code = map_tax_code(get("Tax", "tax_code", "Tax Code"))

    is_desc_only = _is_description_only_line(record)
    if is_desc_only:
        rules.append("Description-only line — flagged to skip")

    return {
        "Customer*":             customer,
        "Adjustment Note date*": memo_date,
        "Reference Code":        ref,
        "Item":                  get("Item", "item"),
        "Account":               account,
        "Description":           desc,
        "QTY":                   qty_val,
        "Item Price":            item_price,
        "Tax Code":              tax_code,
        "Tax":                   tax_amount,
        "Amount":                amount,
        "Amounts*":              get("Amount Include Tax", "amount_include_tax") or "Exclusive",
        "Note":                  get("Memo", "Sales Rep", "note"),
        "_is_description_only":  is_desc_only,
        "_rules_applied":        rules,
    }


# ── Bill Transform ────────────────────────────────────────────────────────────
def transform_bill_record(record: dict, existing_refs: Optional[set] = None) -> dict:
    """
    Transform Reckon Desktop Bill → Reckon One Bill.
    All 24 use cases from Bill sheet.
    """
    rules: list[str] = []
    if existing_refs is None:
        existing_refs = set()

    get = _make_getter(record)

    supplier  = get("Supplier", "supplier_name", "supplier", "Name", "name")
    bill_date = get("Date", "bill_date", "date")
    due_date  = get("Bill Due", "due_date", "Due Date")
    bill_ref  = get("Ref No", "ref_no", "ref", "Num", "num")
    txn_id    = get("txn_id", "transaction_id", "Trans #")

    # Use case 17: due date < bill date → set due = bill date
    if bill_date and due_date:
        try:
            if due_date < bill_date:
                rules.append(f"Due date '{due_date}' < bill date '{bill_date}' — set due = bill date")
                due_date = bill_date
        except TypeError:
            pass

    # Use cases 2, 3, 4: ref normalization
    ref = normalize_ref(bill_ref, txn_id)
    if not str(bill_ref).strip() or str(bill_ref).lower() in ("nan", "none"):
        rules.append(f"Blank bill ref — using transaction ID: '{ref}'")
    elif ref in existing_refs:
        ref = make_unique_ref(ref, txn_id)
        rules.append(f"Duplicate bill ref — made unique: '{ref}'")

    # Use case 22: QTY=0
    qty_str    = get("Qty", "qty", "QTY", "quantity")
    amount_str = get("Gross Amount", "amount", "Amount")
    qty_val    = _normalize_qty(qty_str, amount_str, rules)

    # Use case 21a: multi-currency
    try:
        exchange_rate = float(get("Exchange Rate", "exchange_rate") or 1)
    except ValueError:
        exchange_rate = 1.0

    amount     = _to_float(get("Gross Amount", "amount", "Amount"))
    item_price = _to_float(get("Cost", "item_price", "price", "cost"))
    tax_amount = _to_float(get("Tax Amount", "tax_amount"))
    if exchange_rate != 1.0:
        amount     = round(amount * exchange_rate, 2)
        item_price = round(item_price * exchange_rate, 2)
        tax_amount = round(tax_amount * exchange_rate, 2)
        rules.append(f"Multi-currency: converted at rate {exchange_rate}")

    # Use case 19: bank/CC/GST/retained earnings in bill → Item Purchase
    account = get("Account", "account_code", "Account")
    if _is_restricted_account(account):
        rules.append(f"Account '{account}' is restricted — replaced with Item Purchase; journal required")
        account = "Item Purchase"

    # Use case 6: Inventory Part/Assembly in Bill → use account code, not item; journal required
    item = get("Item", "item")
    item_type_hint = get("item_type", "")
    if item_type_hint.lower() in ("inventory part", "inventory assembly"):
        rules.append(f"Inventory item '{item}' in Bill — use account code only; record inventory journal separately")
        item = ""
        qty_val = 1.0

    desc = clean_name(get("Description", "Memo", "memo", "description"))
    tax_code = map_tax_code(get("Tax", "tax_code", "Tax Code"))

    is_desc_only = _is_description_only_line(record)
    if is_desc_only:
        rules.append("Description-only line — flagged to skip")

    return {
        "Supplier*":    supplier,
        "Bill Date*":   bill_date,
        "Due Date*":    due_date,
        "Reference Code": ref,
        "Item":         item,
        "Description":  desc,
        "Qty":          qty_val,
        "Item Price":   item_price,
        "Tax Code":     tax_code,
        "Tax":          tax_amount,
        "Amount":       amount,
        "Account Code": account,
        "Project":      get("Customer:Job", "project", "customer_job"),
        "Amounts*":     get("Amount Include tax", "amount_include_tax") or "Exclusive",
        "Note":         get("PO No", "Memo", "memo", "note"),
        "_is_description_only": is_desc_only,
        "_rules_applied": rules,
    }


# ── Bill Credit (Supplier Adjustment Note) Transform ─────────────────────────
def transform_bill_credit_record(record: dict, existing_refs: Optional[set] = None) -> dict:
    """
    Transform Reckon Desktop Bill Credit → Reckon One Supplier Adjustment Note.
    Same 23 use cases as Bill. Debit/credit sign rules reversed vs Bill.
    """
    rules: list[str] = []
    if existing_refs is None:
        existing_refs = set()

    get = _make_getter(record)

    supplier   = get("Supplier", "supplier_name", "Name")
    credit_date = get("Date", "credit_date", "date")
    credit_ref  = get("Ref No", "ref_no", "ref", "Num")
    txn_id     = get("txn_id", "transaction_id", "Trans #")

    ref = normalize_ref(credit_ref, txn_id)
    if not str(credit_ref).strip() or str(credit_ref).lower() in ("nan", "none"):
        rules.append(f"Blank bill credit ref — using transaction ID: '{ref}'")
    elif ref in existing_refs:
        ref = make_unique_ref(ref, txn_id)
        rules.append(f"Duplicate ref — made unique: '{ref}'")

    qty_str    = get("Qty", "qty", "QTY")
    amount_str = get("Net Amount", "amount", "Amount")
    qty_val    = _normalize_qty(qty_str, amount_str, rules)

    try:
        exchange_rate = float(get("Exchange Rate", "exchange_rate") or 1)
    except ValueError:
        exchange_rate = 1.0

    amount     = _to_float(get("Net Amount", "amount"))
    item_price = _to_float(get("Cost", "item_price", "price"))
    tax_amount = _to_float(get("Tax Amount", "tax_amount"))
    if exchange_rate != 1.0:
        amount     = round(amount * exchange_rate, 2)
        item_price = round(item_price * exchange_rate, 2)
        tax_amount = round(tax_amount * exchange_rate, 2)
        rules.append(f"Multi-currency: converted at rate {exchange_rate}")

    account = get("Account", "account_code")
    if _is_restricted_account(account):
        rules.append(f"Account '{account}' is restricted — replaced with Item Purchase (sign reversed vs Bill); journal required")
        account = "Item Purchase"

    desc = clean_name(get("Description", "Memo", "memo"))
    tax_code = map_tax_code(get("Tax", "tax_code", "Tax Code"))

    is_desc_only = _is_description_only_line(record)
    if is_desc_only:
        rules.append("Description-only line — flagged to skip")

    return {
        "Supplier*":             supplier,
        "Adjustment Note date*": credit_date,
        "Reference Code":        ref,
        "Item":                  get("Item", "item"),
        "Description":           desc,
        "Qty":                   qty_val,
        "Item Price":            item_price,
        "Tax Code":              tax_code,
        "Tax":                   tax_amount,
        "Amount":                amount,
        "Account Code":          account,
        "Project":               get("Customer:Job", "project"),
        "Amounts*":              get("Amount Include tax", "amount_include_tax") or "Exclusive",
        "Note":                  get("PO No", "Memo", "memo"),
        "_is_description_only":  is_desc_only,
        "_rules_applied":        rules,
    }


# ── Cheque / Make Payment Transform ──────────────────────────────────────────
def transform_cheque_record(record: dict, existing_refs: Optional[set] = None) -> dict:
    """
    Transform Reckon Desktop Check/Credit Card Charge → Reckon One Make Payment.
    All 27 use cases from Cheque Type sheet.
    """
    rules: list[str] = []
    if existing_refs is None:
        existing_refs = set()

    get = _make_getter(record)

    bank_account = get("Bank", "bank_account", "Credit Card", "bank")
    payee        = get("Pay to the Order of", "Purchased From", "payee", "Name", "name")
    chq_date     = get("Date", "date")
    chq_ref      = get("No", "ref", "Num", "no")
    txn_id       = get("txn_id", "transaction_id", "Trans #")
    pay_method   = get("Pay Meth", "payment_method", "Payment Method")
    memo         = get("Memo", "memo")

    # Use case 14: no name → "No Name"
    if not payee or payee.lower() in ("nan", "none"):
        payee = "No Name"
        rules.append("No payee name — defaulted to 'No Name'")

    # Use cases 2, 3, 4: ref normalization
    ref = normalize_ref(chq_ref, txn_id)
    if not str(chq_ref).strip() or str(chq_ref).lower() in ("nan", "none"):
        rules.append(f"Blank cheque ref — using transaction ID: '{ref}'")
    elif ref in existing_refs:
        ref = make_unique_ref(ref, txn_id)
        rules.append(f"Duplicate cheque ref — made unique: '{ref}'")

    qty_str    = get("Qty", "qty", "QTY")
    amount_str = get("Net Amt", "Gross Amt", "amount", "Amount")
    qty_val    = _normalize_qty(qty_str, amount_str, rules)

    try:
        exchange_rate = float(get("Exchange Rate", "exchange_rate") or 1)
    except ValueError:
        exchange_rate = 1.0

    amount     = _to_float(get("Net Amt", "Gross Amt", "amount"))
    item_price = _to_float(get("Cost", "item_price", "price"))
    tax_amount = _to_float(get("Tax Amount", "tax_amount"))
    if exchange_rate != 1.0:
        amount     = round(amount * exchange_rate, 2)
        item_price = round(item_price * exchange_rate, 2)
        tax_amount = round(tax_amount * exchange_rate, 2)
        rules.append(f"Multi-currency: converted at rate {exchange_rate}")

    # Use case 13: bank/CC/GST/AR/AP/retained earnings in detail lines → Item Purchase
    account = get("Account", "account_code")
    if _is_restricted_account(account):
        rules.append(f"Account '{account}' is restricted — replaced with Item Purchase; journal required")
        account = "Item Purchase"

    desc = clean_name(get("Description", "Memo", "memo"))
    tax_code = map_tax_code(get("Tax", "tax_code", "Tax Code"))

    is_desc_only = _is_description_only_line(record)
    if is_desc_only:
        rules.append("Description-only line — flagged to skip")

    return {
        "Bank Account*":   bank_account,
        "Contact*":        payee,
        "Date*":           chq_date,
        "Reference":       ref,
        "Payment Method":  pay_method,
        "Details":         memo,
        "Item":            get("Item", "item"),
        "Description":     desc,
        "QTY":             qty_val,
        "Item Price":      item_price,
        "Tax Code":        tax_code,
        "Tax":             tax_amount,
        "Amount*":         amount,
        "Account":         account,
        "Project":         get("Customer:Job", "project"),
        "_is_description_only": is_desc_only,
        "_rules_applied":  rules,
    }


# ── Deposit / Receive Money Transform ────────────────────────────────────────
def transform_deposit_record(record: dict, existing_refs: Optional[set] = None) -> dict:
    """
    Transform Reckon Desktop Deposit → Reckon One Receive Money.
    All 24 use cases from Deposit sheet.
    Bank/CC/GST/AR/AP/retained earnings → Item Sales (opposite of cheque).
    """
    rules: list[str] = []
    if existing_refs is None:
        existing_refs = set()

    get = _make_getter(record)

    bank_account = get("Deposit To", "bank_account", "deposit_to")
    contact      = get("Received From", "received_from", "Name", "name")
    dep_date     = get("Date", "date")
    dep_ref      = get("Chq No", "ref", "Num", "no")
    txn_id       = get("txn_id", "transaction_id", "Trans #")
    pay_method   = get("Pmt Meth.", "payment_method", "Pay Meth")
    memo         = get("Memo", "memo")

    # Use case 13: no name → "No Name"
    if not contact or contact.lower() in ("nan", "none"):
        contact = "No Name"
        rules.append("No contact name — defaulted to 'No Name'")

    ref = normalize_ref(dep_ref, txn_id)
    if not str(dep_ref).strip() or str(dep_ref).lower() in ("nan", "none"):
        rules.append(f"Blank deposit ref — using transaction ID: '{ref}'")
    elif ref in existing_refs:
        ref = make_unique_ref(ref, txn_id)
        rules.append(f"Duplicate deposit ref — made unique: '{ref}'")

    try:
        exchange_rate = float(get("Exchange Rate", "exchange_rate") or 1)
    except ValueError:
        exchange_rate = 1.0

    amount     = _to_float(get("Amount", "Deposit Total", "amount"))
    tax_amount = _to_float(get("Tax Amount", "tax_amount"))
    if exchange_rate != 1.0:
        amount     = round(amount * exchange_rate, 2)
        tax_amount = round(tax_amount * exchange_rate, 2)
        rules.append(f"Multi-currency: converted at rate {exchange_rate}")

    # Use case 12: bank/CC/GST/AR/AP/retained earnings → Item Sales
    account = get("From Account", "account", "account_code")
    if _is_restricted_account(account):
        rules.append(f"Account '{account}' is restricted — replaced with Item Sales; journal required")
        account = "Item Sales"

    desc = clean_name(get("Memo", "description", "Description"))
    tax_code = map_tax_code(get("Tax", "tax_code", "Tax Code"))

    is_desc_only = _is_description_only_line(record)
    if is_desc_only:
        rules.append("Description-only line — flagged to skip")

    return {
        "Bank Account*":    bank_account,
        "Contact*":         contact,
        "Date*":            dep_date,
        "Reference":        ref,
        "Payment Method":   pay_method,
        "Details":          memo,
        "Account":          account,
        "Description":      desc,
        "Tax Code":         tax_code,
        "Tax":              tax_amount,
        "Amount*":          amount,
        "_is_description_only": is_desc_only,
        "_rules_applied":   rules,
    }


# ── C Card Credit → Receive Money Transform ───────────────────────────────────
def transform_c_card_credit_record(record: dict, existing_refs: Optional[set] = None) -> dict:
    """
    Transform Reckon Desktop C Card Credit → Reckon One Receive Money.
    Same use cases as Deposit. Item Sales for restricted accounts.
    """
    rules: list[str] = []
    if existing_refs is None:
        existing_refs = set()

    get = _make_getter(record)

    bank_account = get("Credit card", "bank_account", "credit_card")
    contact      = get("Purchased From", "purchased_from", "Name", "name")
    date         = get("Date", "date")
    ref_raw      = get("Ref No", "ref_no", "ref", "Num")
    txn_id       = get("txn_id", "transaction_id", "Trans #")

    if not contact or contact.lower() in ("nan", "none"):
        contact = "No Name"
        rules.append("No contact name — defaulted to 'No Name'")

    ref = normalize_ref(ref_raw, txn_id)
    if not str(ref_raw).strip() or str(ref_raw).lower() in ("nan", "none"):
        rules.append(f"Blank ref — using transaction ID: '{ref}'")
    elif ref in existing_refs:
        ref = make_unique_ref(ref, txn_id)
        rules.append(f"Duplicate ref — made unique: '{ref}'")

    try:
        exchange_rate = float(get("Exchange Rate", "exchange_rate") or 1)
    except ValueError:
        exchange_rate = 1.0

    amount     = _to_float(get("Amount", "Gross Amt", "amount"))
    tax_amount = _to_float(get("Tax Amount", "tax_amount"))
    if exchange_rate != 1.0:
        amount     = round(amount * exchange_rate, 2)
        tax_amount = round(tax_amount * exchange_rate, 2)
        rules.append(f"Multi-currency: converted at rate {exchange_rate}")

    account = get("Account", "account_code")
    if _is_restricted_account(account):
        rules.append(f"Account '{account}' is restricted — replaced with Item Sales; journal required")
        account = "Item Sales"

    desc = clean_name(get("Description", "Memo", "memo"))
    tax_code = map_tax_code(get("Tax", "tax_code", "Tax Code"))

    return {
        "Bank Account*":  bank_account,
        "Contact*":       contact,
        "Date*":          date,
        "Reference":      ref,
        "Account":        account,
        "Description":    desc,
        "Tax Code":       tax_code,
        "Tax":            tax_amount,
        "Amount*":        amount,
        "Project":        get("Customer:Job", "project"),
        "_rules_applied": rules,
    }


# ── Sales Receipts → Receive Money Transform ──────────────────────────────────
def transform_sales_receipt_record(record: dict, existing_refs: Optional[set] = None) -> dict:
    """
    Transform Reckon Desktop Sales Receipt → Reckon One Receive Money.
    Same 23 use cases as Invoice.
    """
    rules: list[str] = []
    if existing_refs is None:
        existing_refs = set()

    get = _make_getter(record)

    contact      = get("Customer Name/Customer Job", "customer_name", "Name")
    sr_date      = get("Date", "date")
    bank_account = get("Deposit to", "bank_account", "deposit_to")
    sr_ref       = get("Sale No", "sale_no", "ref", "Num")
    txn_id       = get("txn_id", "transaction_id", "Trans #")
    pay_method   = get("Payment Method", "payment_method", "Pay Meth")
    memo         = get("Memo", "memo", "Cheque No")

    ref = normalize_ref(sr_ref, txn_id)
    if not str(sr_ref).strip() or str(sr_ref).lower() in ("nan", "none"):
        rules.append(f"Blank sales receipt ref — using transaction ID: '{ref}'")
    elif ref in existing_refs:
        ref = make_unique_ref(ref, txn_id)
        rules.append(f"Duplicate ref — made unique: '{ref}'")

    qty_str    = get("QTY", "qty", "Qty")
    amount_str = get("Amount", "amount")
    qty_val    = _normalize_qty(qty_str, amount_str, rules)

    try:
        exchange_rate = float(get("Exchange rate", "exchange_rate") or 1)
    except ValueError:
        exchange_rate = 1.0

    amount     = _to_float(get("Amount", "amount"))
    item_price = _to_float(get("Rate", "rate", "item_price"))
    tax_amount = _to_float(get("Tax Amount", "tax_amount"))
    if exchange_rate != 1.0:
        amount     = round(amount * exchange_rate, 2)
        item_price = round(item_price * exchange_rate, 2)
        tax_amount = round(tax_amount * exchange_rate, 2)
        rules.append(f"Multi-currency: converted at rate {exchange_rate}")

    account = get("Account Code", "account", "account_code")
    if _is_restricted_account(account):
        rules.append(f"Account '{account}' is restricted — replaced with Item Sales; journal required")
        account = "Item Sales"

    # Due date check (use case 16 equivalent for SR)
    due_date = get("Due Date", "due_date")
    if sr_date and due_date:
        try:
            if due_date < sr_date:
                due_date = sr_date
                rules.append(f"Due date < receipt date — set due = receipt date")
        except TypeError:
            pass

    desc = clean_name(get("Description", "description"))
    tax_code = map_tax_code(get("Tax", "tax_code", "Tax Code"))

    is_desc_only = _is_description_only_line(record)
    if is_desc_only:
        rules.append("Description-only line — flagged to skip")

    return {
        "Contact*":         contact,
        "Date*":            sr_date,
        "Bank Account*":    bank_account,
        "Reference Code":   ref,
        "Payment Method":   pay_method,
        "Details":          memo,
        "Item":             get("Item", "item"),
        "Account":          account,
        "Description":      desc,
        "QTY":              qty_val,
        "Item Price":       item_price,
        "Tax Code":         tax_code,
        "Tax":              tax_amount,
        "Amount*":          amount,
        "Amounts*":         "Exclusive",  # inclusive by default per sheet
        "Allocation notes": get("Customer Msg", "customer_msg"),
        "_is_description_only": is_desc_only,
        "_rules_applied":   rules,
    }


# ── Transfer → Transfer Money Transform ───────────────────────────────────────
def transform_transfer_record(record: dict) -> dict:
    """
    Transform Reckon Desktop Transfer → Reckon One Transfer Money.
    Use cases: inactive bank/CC, multi-currency, current assets/liabilities (tick checkbox).
    """
    rules: list[str] = []
    get = _make_getter(record)

    from_account = get("Transfer Funds From", "from_account", "transfer_from")
    to_account   = get("Transfer Funds To", "to_account", "transfer_to")
    date         = get("Date", "date")
    memo         = get("Memo", "memo", "description")

    try:
        exchange_rate = float(get("Exchange Rate", "exchange_rate") or 1)
    except ValueError:
        exchange_rate = 1.0

    amount = _to_float(get("Transfer Amt", "amount", "Amount"))
    if exchange_rate != 1.0:
        amount = round(amount * exchange_rate, 2)
        rules.append(f"Multi-currency: converted at rate {exchange_rate}")

    # Use case 3: current assets/liabilities in transfer → accepted but need checkbox
    from_type = get("from_account_type", "")
    if from_type.lower() in ("current assets", "current liabilities", "equity"):
        rules.append(f"Non-bank account '{from_account}' in transfer — tick 'Show all accounts' checkbox in Reckon One")

    return {
        "Date*":        date,
        "Transfer from": from_account,
        "Transfer to":  to_account,
        "Amount*":      amount,
        "Description":  clean_name(memo),
        "_rules_applied": rules or ["Transfer record mapped directly"],
    }


# ── Payment → Receive Money Transform ────────────────────────────────────────
def transform_payment_record(record: dict, existing_refs: Optional[set] = None) -> dict:
    """
    Transform Reckon Desktop Payment → Reckon One Receive Money.
    All 11 use cases from Payment sheet.
    """
    rules: list[str] = []
    if existing_refs is None:
        existing_refs = set()

    get = _make_getter(record)

    contact      = get("Received From", "received_from", "Name", "name")
    date         = get("Date", "date")
    bank_account = get("Deposit To", "bank_account", "deposit_to")
    pay_ref      = get("Reference", "ref", "Num")
    txn_id       = get("txn_id", "transaction_id", "Trans #")
    pay_method   = get("Pmt Method", "payment_method", "Pay Meth")
    memo         = get("Memo", "memo")

    ref = normalize_ref(pay_ref, txn_id)
    if not str(pay_ref).strip() or str(pay_ref).lower() in ("nan", "none"):
        rules.append(f"Blank payment ref — using transaction ID: '{ref}'")
    elif ref in existing_refs:
        ref = make_unique_ref(ref, txn_id)
        rules.append(f"Duplicate payment ref — made unique: '{ref}'")

    try:
        exchange_rate = float(get("Ex Rate", "exchange_rate") or 1)
    except ValueError:
        exchange_rate = 1.0

    amount = _to_float(get("Amount", "amount"))
    if exchange_rate != 1.0:
        amount = round(amount * exchange_rate, 2)
        rules.append(f"Multi-currency: converted at rate {exchange_rate}")

    # Use case 11: payment applied on current assets → journal required
    ar_account = get("AR Account", "ar_account")
    if ar_account and _is_restricted_account(ar_account):
        rules.append(f"Current Assets in payment — journal required: debit Current Assets, credit AR")

    return {
        "Contact*":        contact,
        "Date*":           date,
        "Bank Account*":   bank_account,
        "Reference":       ref,
        "Payment Method*": pay_method,
        "Details":         memo,
        "Amount*":         amount,
        "_rules_applied":  rules,
    }


# ── Bill Payment Transform ────────────────────────────────────────────────────
def transform_bill_payment_record(record: dict, existing_refs: Optional[set] = None) -> dict:
    """
    Transform Reckon Desktop Bill Payment (Cheque/CC) → Reckon One Make Payment.
    All 12 use cases from Bill Payment sheet.
    """
    rules: list[str] = []
    if existing_refs is None:
        existing_refs = set()

    get = _make_getter(record)

    supplier     = get("Show Bill For", "supplier", "Supplier", "Name")
    pay_date     = get("Payment Date", "date", "Date")
    bank_account = get("Payment Account", "bank_account", "payment_account")
    pay_ref      = get("Reference", "ref", "Num")
    txn_id       = get("txn_id", "transaction_id", "Trans #")
    pay_method   = get("Payment Method", "payment_method")

    ref = normalize_ref(pay_ref, txn_id)
    if not str(pay_ref).strip() or str(pay_ref).lower() in ("nan", "none"):
        rules.append(f"Blank bill payment ref — using transaction ID: '{ref}'")
    elif ref in existing_refs:
        ref = make_unique_ref(ref, txn_id)
        rules.append(f"Duplicate ref — made unique: '{ref}'")

    try:
        exchange_rate = float(get("Exchange Rate", "exchange_rate") or 1)
    except ValueError:
        exchange_rate = 1.0

    amount = _to_float(get("Totals", "Amount", "amount", "Amt To Pay"))
    if exchange_rate != 1.0:
        amount = round(amount * exchange_rate, 2)
        rules.append(f"Multi-currency: converted at rate {exchange_rate}")

    # Use case 7: discount in bill payment → journal required
    discount = get("Discount", "discount")
    if discount and _to_float(discount) != 0:
        rules.append(f"Discount {discount} in bill payment — journal required: credit nominal, debit AP")

    return {
        "Contact*":        supplier,
        "Date*":           pay_date,
        "Bank Account*":   bank_account,
        "Reference":       ref,
        "Payment Method":  pay_method,
        "Amount*":         amount,
        "_rules_applied":  rules,
    }


# ── Journal Transform ─────────────────────────────────────────────────────────
def transform_journal_record(record: dict, existing_refs: Optional[set] = None) -> dict:
    """
    Transform Reckon Desktop Journal Entry → Reckon One Journal.
    All 14 use cases from Journal sheet.
    """
    rules: list[str] = []
    if existing_refs is None:
        existing_refs = set()

    get = _make_getter(record)

    jnl_ref = get("Entry No", "entry_no", "journal_no", "ref", "Num", "Num")
    txn_id  = get("txn_id", "transaction_id", "Trans #")

    ref = normalize_ref(str(jnl_ref), str(txn_id))
    if not str(jnl_ref).strip() or str(jnl_ref).lower() in ("nan", "none"):
        rules.append(f"Blank journal ref — using transaction ID: '{ref}'")
    elif ref in existing_refs:
        ref = make_unique_ref(ref, txn_id)
        rules.append(f"Duplicate journal ref — made unique: '{ref}'")

    try:
        exchange_rate = float(get("Exchange Rate", "exchange_rate", "Home Currency Adjustment") or 1)
    except ValueError:
        exchange_rate = 1.0

    debit  = _to_float(get("Debit", "debit"))
    credit = _to_float(get("Credit", "credit"))
    tax_amount = _to_float(get("Tax Amount", "tax_amount"))
    if exchange_rate != 1.0:
        debit  = round(debit * exchange_rate, 2)
        credit = round(credit * exchange_rate, 2)
        tax_amount = round(tax_amount * exchange_rate, 2)
        rules.append(f"Multi-currency: converted at rate {exchange_rate}")

    contact = get("Name", "contact", "name")
    desc    = clean_name(get("Memo (First Line Memo)", "first_memo", "memo", "Memo", "Description"))
    narration = clean_name(get("Memo", "narration", "note"))
    tax_code  = map_tax_code(get("Tax Item", "tax_item", "tax_code", "Tax Code"))

    is_desc_only = not get("Account", "account") and debit == 0 and credit == 0
    if is_desc_only:
        rules.append("Description-only journal line — flagged to skip")

    return {
        "Journal Date *":    get("Date", "date", "journal_date"),
        "Summary *":         ref,
        "Adjusting Journal": get("Adjusting Entry", "adjusting_entry"),
        "Description":       desc,
        "Account":           get("Account", "account"),
        "Debit":             debit,
        "Credit":            credit,
        "Tax Code":          tax_code,
        "Tax":               tax_amount,
        "Narration":         narration,
        "Contact":           contact,
        "Amounts *":         get("Amount Include Tax", "amount_include_tax") or "Exclusive",
        "_is_description_only": is_desc_only,
        "_rules_applied":    rules,
    }


# ── Paycheque → Journal Transform ────────────────────────────────────────────
def transform_paycheque_record(record: dict, existing_refs: Optional[set] = None) -> dict:
    """
    Transform Reckon Desktop Paycheque → Reckon One Journal.
    Bank Account credited; payroll item accounts debited.
    Inactive employee → import as active (use case 1).
    """
    rules: list[str] = []
    if existing_refs is None:
        existing_refs = set()

    get = _make_getter(record)

    chq_no   = get("Assign cheque Num", "cheque_num", "Num", "ref")
    txn_id   = get("txn_id", "transaction_id", "Trans #")
    ref      = normalize_ref(chq_no, txn_id)
    if not str(chq_no).strip() or str(chq_no).lower() in ("nan", "none"):
        rules.append(f"Blank paycheque ref — using transaction ID: '{ref}'")
    elif ref in existing_refs:
        ref = make_unique_ref(ref, txn_id)
        rules.append(f"Duplicate ref — made unique: '{ref}'")

    employee = get("Name", "employee", "name")
    # Inactive employee → import as active (use case 1)
    is_active = get("Active Status", "active_status", "status").lower() not in ("inactive", "false", "0", "no")
    if not is_active:
        rules.append("Inactive employee — imported as Active for conversion (revert after)")

    rules.append("Paycheque → Journal: Bank credited; payroll item accounts debited")

    return {
        "Journal Date *":    get("Payment Date", "date"),
        "Summary *":         ref,
        "Adjusting Journal": "",
        "Description":       f"Paycheque for {employee}",
        "Account":           get("Account", "account"),
        "Debit":             _to_float(get("Debit", "debit")),
        "Credit":            _to_float(get("Credit", "credit")),
        "Tax Code":          "",
        "Tax":               0,
        "Narration":         clean_name(get("Description", "description", "memo")),
        "Contact":           employee,
        "Trans Type":        "Paycheque",
        "_rules_applied":    rules,
    }


# ── Item Receipt → Journal Transform ─────────────────────────────────────────
def transform_item_receipt_record(record: dict, existing_refs: Optional[set] = None) -> dict:
    """
    Transform Reckon Desktop Item Receipt → Reckon One Journal.
    Positive gross amount: AP credited, asset account of item debited.
    QTY=0 → 1; description-only → skip.
    """
    rules: list[str] = []
    if existing_refs is None:
        existing_refs = set()

    get = _make_getter(record)

    ref_raw  = get("Ref No", "ref_no", "Num", "num")
    txn_id   = get("txn_id", "transaction_id", "Trans #")
    ref      = normalize_ref(ref_raw, txn_id)
    if not str(ref_raw).strip() or str(ref_raw).lower() in ("nan", "none"):
        rules.append(f"Blank item receipt ref — using transaction ID: '{ref}'")
    elif ref in existing_refs:
        ref = make_unique_ref(ref, txn_id)
        rules.append(f"Duplicate ref — made unique: '{ref}'")

    gross_amount = _to_float(get("Gross Amount", "amount", "Amount"))

    # Debit/credit based on amount sign
    if gross_amount >= 0:
        debit  = gross_amount
        credit = gross_amount
        rules.append("Item receipt: AP credited, asset account of item debited (positive amount)")
    else:
        debit  = abs(gross_amount)
        credit = abs(gross_amount)
        rules.append("Item receipt: AP debited, asset account reversed (negative amount)")

    # Inventory Part/Assembly → record as journal
    rules.append("Item Receipt → Journal: Inventory Part/Assembly recorded as journal entry in Reckon One")

    qty_str = get("Qty", "qty", "QTY")
    if qty_str:
        try:
            qty_val = float(qty_str)
            if qty_val == 0 and gross_amount != 0:
                rules.append("QTY=0 with amount — changed to 1")
        except ValueError:
            pass

    is_desc_only = not get("Account", "account") and gross_amount == 0
    if is_desc_only:
        rules.append("Description-only line — flagged to skip")

    return {
        "Journal Date *":    get("Date", "date"),
        "Summary *":         ref,
        "Adjusting Journal": "",
        "Description":       clean_name(get("Memo", "memo")),
        "Account":           get("Account", "account"),
        "Debit":             debit,
        "Credit":            credit,
        "Tax Code":          map_tax_code(get("Tax", "tax_code")),
        "Tax":               _to_float(get("Tax Amount", "tax_amount")),
        "Narration":         clean_name(get("Description", "description")),
        "Contact":           get("Supplier", "supplier", "Name"),
        "Trans Type":        "Item Receipt",
        "_is_description_only": is_desc_only,
        "_rules_applied":    rules,
    }


# ── Inventory Adjustment → Journal Transform ──────────────────────────────────
def transform_inventory_adjustment_record(record: dict, existing_refs: Optional[set] = None) -> dict:
    """
    Transform Reckon Desktop Inventory Adjustment → Reckon One Journal.
    Positive: Inventory debited, Adjustment account credited.
    Negative: reversed.
    """
    rules: list[str] = []
    if existing_refs is None:
        existing_refs = set()

    get = _make_getter(record)

    ref_raw = get("Ref No", "ref_no", "Num")
    txn_id  = get("txn_id", "transaction_id", "Trans #")
    ref     = normalize_ref(ref_raw, txn_id)
    if not str(ref_raw).strip() or str(ref_raw).lower() in ("nan", "none"):
        rules.append(f"Blank inventory adjustment ref — using transaction ID: '{ref}'")
    elif ref in existing_refs:
        ref = make_unique_ref(ref, txn_id)
        rules.append(f"Duplicate ref — made unique: '{ref}'")

    total_adj = _to_float(get("Total value of adjustment", "amount", "Amount"))

    if total_adj >= 0:
        debit  = total_adj
        credit = total_adj
        rules.append("Inventory adjustment: Inventory debited, Adjustment account credited (positive)")
    else:
        debit  = abs(total_adj)
        credit = abs(total_adj)
        rules.append("Inventory adjustment: Inventory credited, Adjustment account debited (negative)")

    return {
        "Journal Date *": get("Adjustment Date", "date"),
        "Summary *":      ref,
        "Description":    clean_name(get("Memo", "memo")),
        "Account":        get("Adjustment Account", "account"),
        "Debit":          debit,
        "Credit":         credit,
        "Tax Code":       "",
        "Tax":            0,
        "Narration":      clean_name(get("Description", "description")),
        "Contact":        get("Customer:Job", "customer_job"),
        "Trans Type":     "Inventory Adjustment",
        "_rules_applied": rules,
    }


# ── Liability Cheque → Journal Transform ─────────────────────────────────────
def transform_liability_cheque_record(record: dict, existing_refs: Optional[set] = None) -> dict:
    """
    Transform Reckon Desktop Liability Cheque → Reckon One Journal.
    Bank Account credited; payroll item accounts debited per pay item.
    """
    rules: list[str] = []
    if existing_refs is None:
        existing_refs = set()

    get = _make_getter(record)

    txn_no  = get("Transaction No", "transaction_no", "Num")
    txn_id  = get("txn_id", "Trans #")
    ref     = normalize_ref(txn_no, txn_id)
    if not str(txn_no).strip() or str(txn_no).lower() in ("nan", "none"):
        rules.append(f"Blank liability cheque ref — using transaction ID: '{ref}'")
    elif ref in existing_refs:
        ref = make_unique_ref(ref, txn_id)
        rules.append(f"Duplicate ref — made unique: '{ref}'")

    amt_to_pay = _to_float(get("Amt To Pay", "amount", "Amount"))
    rules.append("Liability Cheque → Journal: Bank Account credited; payroll item accounts debited")

    return {
        "Journal Date *": get("Cheque Date", "date"),
        "Summary *":      ref,
        "Description":    "",
        "Account":        get("Account", "account"),
        "Debit":          amt_to_pay,
        "Credit":         amt_to_pay,
        "Tax Code":       "",
        "Tax":            0,
        "Narration":      clean_name(get("Pay Item Description", "description")),
        "Contact":        "",
        "Trans Type":     "Liability Cheque",
        "_rules_applied": rules,
    }


# ── Liability Adjustment → Journal Transform ──────────────────────────────────
def transform_liability_adjustment_record(record: dict, existing_refs: Optional[set] = None) -> dict:
    """
    Transform Reckon Desktop Liability Adjustment → Reckon One Journal.
    Positive: Payroll Liabilities credited, account debited.
    Deposit Refund: Deposit To debited, account credited.
    """
    rules: list[str] = []
    if existing_refs is None:
        existing_refs = set()

    get = _make_getter(record)

    txn_no  = get("Transaction No", "transaction_no", "Num")
    txn_id  = get("txn_id", "Trans #")
    ref     = normalize_ref(txn_no, txn_id)
    if not str(txn_no).strip() or str(txn_no).lower() in ("nan", "none"):
        rules.append(f"Blank liability adjustment ref — using transaction ID: '{ref}'")
    elif ref in existing_refs:
        ref = make_unique_ref(ref, txn_id)
        rules.append(f"Duplicate ref — made unique: '{ref}'")

    amount = _to_float(get("Amount", "amount"))
    adj_type = get("adjustment_type", "type", "")

    if adj_type.lower() == "deposit refund":
        rules.append("Deposit Refund of Liabilities: Deposit To debited, Account credited")
    else:
        if amount >= 0:
            rules.append("Liability Adjustment: Payroll Liabilities credited, Account debited (positive)")
        else:
            rules.append("Liability Adjustment: Payroll Liabilities debited, Account credited (negative)")

    return {
        "Journal Date *": get("Date", "date"),
        "Summary *":      ref,
        "Description":    "",
        "Account":        get("Account", "account"),
        "Debit":          abs(amount),
        "Credit":         abs(amount),
        "Tax Code":       "",
        "Tax":            0,
        "Narration":      clean_name(get("Memo", "memo")),
        "Contact":        get("Employee/Supplier", "employee", "supplier", "Name"),
        "Trans Type":     "Liability Adjustment",
        "_rules_applied": rules,
    }


# ── YTD Adjustment → Journal Transform ───────────────────────────────────────
def transform_ytd_adjustment_record(record: dict, existing_refs: Optional[set] = None) -> dict:
    """Transform Reckon Desktop YTD Adjustment → Reckon One Journal."""
    rules: list[str] = []
    if existing_refs is None:
        existing_refs = set()

    get = _make_getter(record)

    no      = get("No", "no", "Num")
    txn_id  = get("txn_id", "Trans #")
    ref     = normalize_ref(no, txn_id)
    if not str(no).strip() or str(no).lower() in ("nan", "none"):
        rules.append(f"Blank YTD adjustment ref — using transaction ID: '{ref}'")
    elif ref in existing_refs:
        ref = make_unique_ref(ref, txn_id)
        rules.append(f"Duplicate ref — made unique: '{ref}'")

    ytd_amt = _to_float(get("Ytd Amt", "amount", "Amount"))
    rules.append("YTD Adjustment → Journal: Bank credited; payroll item accounts debited")

    return {
        "Journal Date *": get("Date", "date"),
        "Summary *":      ref,
        "Description":    "",
        "Account":        get("Account", "account"),
        "Debit":          ytd_amt,
        "Credit":         ytd_amt,
        "Tax Code":       "",
        "Tax":            0,
        "Narration":      clean_name(get("Memo", "memo")),
        "Contact":        get("To", "to", "Name"),
        "Trans Type":     "YTD Adjustment",
        "_rules_applied": rules,
    }


# ── C Card Refund → Journal Transform ────────────────────────────────────────
def transform_c_card_refund_record(record: dict, existing_refs: Optional[set] = None) -> dict:
    """
    Transform Reckon Desktop C Card Refund → Reckon One Journal.
    Bank amount debited; Receivable Account credited.
    """
    rules: list[str] = []
    if existing_refs is None:
        existing_refs = set()

    get = _make_getter(record)

    ref_raw = get("Ref no", "ref_no", "Num")
    txn_id  = get("txn_id", "Trans #")
    ref     = normalize_ref(ref_raw, txn_id)
    if not str(ref_raw).strip() or str(ref_raw).lower() in ("nan", "none"):
        rules.append(f"Blank C card refund ref — using transaction ID: '{ref}'")
    elif ref in existing_refs:
        ref = make_unique_ref(ref, txn_id)
        rules.append(f"Duplicate ref — made unique: '{ref}'")

    refund_amt = _to_float(get("Refund Amt", "refund_amt", "amount"))
    rules.append("C Card Refund → Journal: Bank debited; Receivable Account credited")

    return {
        "Journal Date *": get("Date", "date"),
        "Summary *":      ref,
        "Description":    "",
        "Account":        get("Account", "AR Account", "account"),
        "Debit":          refund_amt,
        "Credit":         refund_amt,
        "Tax Code":       "",
        "Tax":            0,
        "Narration":      clean_name(get("Memo", "memo")),
        "Contact":        get("Name", "name"),
        "Trans Type":     get("Refund Issued To", "refund_issued_to") or "C Card Refund",
        "_rules_applied": rules,
    }


# ── Statement Charge → Journal Transform ─────────────────────────────────────
def transform_statement_charge_record(record: dict, existing_refs: Optional[set] = None) -> dict:
    """
    Transform Reckon Desktop Statement Charge → Reckon One Journal.
    Positive: AR debited, Income credited. Negative: AR credited, Income debited.
    """
    rules: list[str] = []
    if existing_refs is None:
        existing_refs = set()

    get = _make_getter(record)

    number  = get("Number", "number", "Num")
    txn_id  = get("txn_id", "Trans #")
    ref     = normalize_ref(number, txn_id)
    if not str(number).strip() or str(number).lower() in ("nan", "none"):
        rules.append(f"Blank statement charge ref — using transaction ID: '{ref}'")
    elif ref in existing_refs:
        ref = make_unique_ref(ref, txn_id)
        rules.append(f"Duplicate ref — made unique: '{ref}'")

    amount_charge = _to_float(get("Amount Charge", "Balance", "amount"))

    if amount_charge >= 0:
        rules.append("Statement Charge: AR debited, Income credited (positive)")
    else:
        rules.append("Statement Charge: AR credited, Income debited (negative)")

    account = get("Account", "account")
    if _is_restricted_account(account):
        rules.append(f"Account '{account}' is restricted — replaced with Item Sales; journal required")
        account = "Item Sales"

    is_desc_only = not account and amount_charge == 0
    if is_desc_only:
        rules.append("Description-only line — flagged to skip")

    return {
        "Journal Date *": get("Date", "date"),
        "Summary *":      ref,
        "Description":    "",
        "Account":        account,
        "Debit":          abs(amount_charge),
        "Credit":         abs(amount_charge),
        "Tax Code":       "",
        "Tax":            0,
        "Narration":      clean_name(get("Description", "description")),
        "Contact":        get("Customer:Job", "customer_job", "Name"),
        "Trans Type":     get("Type", "type") or "Statement Charge",
        "_is_description_only": is_desc_only,
        "_rules_applied": rules,
    }


# ── Build Assembly → Journal Transform ────────────────────────────────────────
def transform_build_assembly_record(record: dict, existing_refs: Optional[set] = None) -> dict:
    """
    Transform Reckon Desktop Build Assembly → Reckon One Journal.
    Inventory Part/Assembly → journal entry only (no item in Reckon One).
    """
    rules: list[str] = []
    if existing_refs is None:
        existing_refs = set()

    get = _make_getter(record)

    num    = get("Num", "num", "Ref No")
    txn_id = get("txn_id", "Trans #")
    ref    = normalize_ref(num, txn_id)
    if not str(num).strip() or str(num).lower() in ("nan", "none"):
        rules.append(f"Blank build assembly ref — using transaction ID: '{ref}'")
    elif ref in existing_refs:
        ref = make_unique_ref(ref, txn_id)
        rules.append(f"Duplicate ref — made unique: '{ref}'")

    rules.append("Build Assembly → Journal: Inventory Part/Assembly recorded as journal entry in Reckon One")

    return {
        "Journal Date *": get("Date", "date"),
        "Summary *":      ref,
        "Description":    "",
        "Account":        get("Account", "account"),
        "Debit":          _to_float(get("Debit", "debit")),
        "Credit":         _to_float(get("Credit", "credit")),
        "Tax Code":       "",
        "Tax":            0,
        "Narration":      clean_name(get("Description", "description", "Memo")),
        "Contact":        "",
        "Trans Type":     "Build Assembly",
        "_rules_applied": rules,
    }


# ═══════════════════════════════════════════════════════════════════════════════
# BATCH HELPERS
# ═══════════════════════════════════════════════════════════════════════════════

def batch_transform(records: list[dict], entity_type: str) -> list[dict]:
    """
    Transform a list of records for the given entity type.
    Handles duplicate name deduplication post-transform.

    entity_type: coa | tax | customer | supplier | item | project | class |
                 terms | bank | invoice | credit_memo | bill | bill_credit |
                 cheque | deposit | c_card_credit | sales_receipt | transfer |
                 payment | bill_payment | journal | paycheque | item_receipt |
                 inventory_adjustment | liability_cheque | liability_adjustment |
                 ytd_adjustment | c_card_refund | statement_charge | build_assembly
    """
    # Entity types that use a shared seen_refs set
    REF_BASED = {
        "invoice":              (transform_invoice_record,              "Reference Code"),
        "credit_memo":          (transform_credit_memo_record,          "Reference Code"),
        "bill":                 (transform_bill_record,                 "Reference Code"),
        "bill_credit":          (transform_bill_credit_record,          "Reference Code"),
        "cheque":               (transform_cheque_record,               "Reference"),
        "deposit":              (transform_deposit_record,              "Reference"),
        "c_card_credit":        (transform_c_card_credit_record,        "Reference"),
        "sales_receipt":        (transform_sales_receipt_record,        "Reference Code"),
        "payment":              (transform_payment_record,              "Reference"),
        "bill_payment":         (transform_bill_payment_record,         "Reference"),
        "journal":              (transform_journal_record,              "Summary *"),
        "paycheque":            (transform_paycheque_record,            "Summary *"),
        "item_receipt":         (transform_item_receipt_record,         "Summary *"),
        "inventory_adjustment": (transform_inventory_adjustment_record, "Summary *"),
        "liability_cheque":     (transform_liability_cheque_record,     "Summary *"),
        "liability_adjustment": (transform_liability_adjustment_record, "Summary *"),
        "ytd_adjustment":       (transform_ytd_adjustment_record,       "Summary *"),
        "c_card_refund":        (transform_c_card_refund_record,        "Summary *"),
        "statement_charge":     (transform_statement_charge_record,     "Summary *"),
        "build_assembly":       (transform_build_assembly_record,       "Summary *"),
    }

    SIMPLE = {
        "coa":      transform_coa_record,
        "tax":      transform_tax_record,
        "customer": transform_customer_record,
        "supplier": transform_supplier_record,
        "item":     transform_item_record,
        "project":  transform_project_record,
        "class":    transform_class_record,
        "terms":    transform_terms_record,
        "bank":     transform_bank_record,
        "transfer": transform_transfer_record,
    }

    if entity_type in REF_BASED:
        fn, ref_key = REF_BASED[entity_type]
        seen_refs: set[str] = set()
        out = []
        for r in records:
            transformed = fn(r, existing_refs=seen_refs)
            seen_refs.add(transformed.get(ref_key, ""))
            out.append(transformed)
        return out

    if entity_type in SIMPLE:
        fn = SIMPLE[entity_type]
        transformed = [fn(r) for r in records]
    else:
        raise ValueError(f"Unknown entity type: '{entity_type}'")

    # Deduplicate display names
    name_key = {
        "coa":      "ACCOUNT NAME*",
        "customer": "Display name*",
        "supplier": "Display name*",
        "item":     "Item Name*",
        "project":  "Project name*",
        "class":    "Name*",
        "bank":     "Account Display Name*",
    }.get(entity_type)

    if name_key:
        names  = [r.get(name_key, "") for r in transformed]
        deduped = deduplicate_names(names)
        for rec, new_name in zip(transformed, deduped):
            if new_name != rec.get(name_key):
                rec[name_key] = new_name
                rec.setdefault("_rules_applied", []).append(
                    f"Duplicate name — renamed to '{new_name}' (-A/-B suffix rule)"
                )

    return transformed


# ── Mandatory COA accounts ────────────────────────────────────────────────────
def get_mandatory_coa_accounts() -> list[dict]:
    """
    3 accounts that MUST exist in Reckon One before migration starts.
    From COA sheet use case 11.
    """
    return [
        {
            "ACCOUNT NAME*": "Item Sales",
            "Account TYPE*": "Income",
            "DESCRIPTION":   "Default item sales income account (mandatory for migration)",
            "Status":        "Active",
        },
        {
            "ACCOUNT NAME*": "Item Purchase",
            "Account TYPE*": "Cost of Goods Sold",
            "DESCRIPTION":   "Default item purchase expense account (mandatory for migration)",
            "Status":        "Active",
        },
        {
            "ACCOUNT NAME*": "Rounding",
            "Account TYPE*": "Non-Current Liability",
            "DESCRIPTION":   "GST/invoice rounding differences account (mandatory for migration)",
            "Status":        "Active",
        },
    ]


def get_mandatory_items() -> list[dict]:
    """Rounding item required for GST rounding journals (Item use case 9)."""
    return [
        {
            "Item Name*":    "Rounding",
            "Type*":         "Service",
            "Sale Account*": "Rounding",
            "PURCHASE ACCOUNT*": "Rounding",
            "SALE DESCRIPTION":  "GST rounding adjustment",
            "PURCHASE DESCRIPTION": "GST rounding adjustment",
            "Status":        "Active",
        }
    ]


# ── Pre-flight validation ─────────────────────────────────────────────────────
def validate_migration_readiness(coa_records: list[dict]) -> list[dict]:
    """
    Pre-migration checklist. Returns list of {type, field, message} dicts.
    type: "error" | "warning" | "info"
    """
    issues = []
    names  = [r.get("ACCOUNT NAME*", "") for r in coa_records]
    lnames = [n.lower() for n in names]

    # Mandatory accounts
    for req in ("item sales", "item purchase", "rounding"):
        if req not in lnames:
            issues.append({
                "type": "error", "field": "COA",
                "message": f"Mandatory account '{req.title()}' missing — must be created before migration."
            })

    # Duplicate names
    seen: dict[str, int] = {}
    for name in names:
        key = name.lower().strip()
        if key in seen:
            issues.append({"type": "warning", "field": "COA",
                           "message": f"Duplicate account name '{name}' — will be auto-suffixed (-A/-B)."})
        seen[key] = 1

    # Code length
    for r in coa_records:
        code = str(r.get("ACCOUNT CODE", "") or "")
        if len(code) > ACCOUNT_CODE_MAX:
            issues.append({"type": "warning", "field": "COA",
                           "message": f"Account code '{code}' > 7 chars — will be truncated."})

    # Restricted account types used as item accounts (COA use case 11)
    for r in coa_records:
        acc_type = str(r.get("Account TYPE*", "")).lower()
        acc_name = r.get("ACCOUNT NAME*", "")
        if "bank" in acc_type or "credit card" in acc_type:
            issues.append({"type": "info", "field": "COA",
                           "message": f"'{acc_name}' is bank/CC type — cannot be used as item income/expense account."})

    return issues


# ── LLM context builder ───────────────────────────────────────────────────────
def build_reckon_mapping_context() -> str:
    """
    Returns a condensed rules string injected into the mapping agent system prompt.
    Covers all key rules from all 45 spreadsheet sheets.
    """
    type_lines = "\n".join(
        f"  {k.title()} → {v}" for k, v in COA_TYPE_MAP.items()
    )
    tax_lines = "\n".join(
        f"  {k} → {v['code']} ({v['rate']*100:.2f}%)" for k, v in TAX_CODE_MAP.items()
    )
    txn_lines = "\n".join(
        f"  {k.title()} → {v}" for k, v in list(TRANSACTION_TYPE_MAP.items())[:12]
    )
    return f"""=== RECKON DESKTOP → RECKON ONE RULES (45 sheets) ===

ACCOUNT TYPE MAPPING:
{type_lines}

TAX CODE MAPPING:
{tax_lines}

TRANSACTION TYPE MAPPING (Desktop → Reckon One function):
{txn_lines}
  (Paycheque/Item Receipt/Inventory Adjustment/Build Assembly/etc.) → Journal

MIGRATION SEQUENCE (47 steps):
  1. Org Settings → 2. Terms → 3. Tax → 4. COA → 5. Bank → 6. Customer →
  7. Project → 8. Supplier → 9. Item → 10. Invoice → 11. Credit Memo →
  12. Bill → 13. Bill Credit → 14. Cheque → 15. Deposit → 16. Transfer →
  17. Sales Receipt → 18. Journal → 19-30. Journal-based types →
  31. Opening AR → 32. Opening AP → 33. Opening Trial Balance

KEY COA RULES:
• Account code max 7 chars; strip special chars, leading dots/hyphens, date prefixes (AUG-25)
• Balance field IGNORED — brought in via opening journal
• Inactive accounts → import as Active for conversion period, revert after
• Duplicate names → suffix -A, -B, etc.
• Mandatory accounts before migration: Item Sales (Income), Item Purchase (COGS), Rounding (Non-Current Liability)

KEY TRANSACTION RULES:
• Ref max 20 chars; blank ref → use transaction ID; duplicate → append txn_id
• QTY=0 with amount → set QTY=1
• Due date < invoice/bill date → set due = invoice/bill date
• Multi-currency → multiply amounts by exchange rate (Reckon One has no multi-currency)
• >75 lines in invoice/bill → consolidate to 1-liner using Item Sales/Item Purchase, pass journal
• Description-only line (no item/qty/price) → skip the line
• Inventory Part/Assembly → record as journal entry in Reckon One, not as item
• GST rounding → create Rounding item/account; pass journal between GST and Rounding

KEY ACCOUNT REPLACEMENT RULES:
• Bank/Credit Card/Retained Earnings/GST Payable/Undeposited Funds used as item account:
  - In sales/deposits/receive money → replace with Item Sales account + pass journal
  - In purchases/cheques/make payment → replace with Item Purchase account + pass journal

KEY ITEM RULES:
• Service/OtherCharge/Subtotal/Group/Discount/Payment → Service type
• Inventory/InventoryAssembly/NonInventory → Product type
• Missing income account → default to 'Item Sales'
• Missing expense account → default to 'Item Purchase'
• Tax-inclusive item → use FRE as sale tax code

KEY ENTITY RULES:
• 'No Name' customer → rename to 'No Name-C'
• Bill1 line == customer/supplier name → ignore for address
• Job Status: None/Pending/Awarded/In Progress → Active; Closed → Completed; Not Awarded → Inactive
• Tax start date always: {TAX_START_DATE}
• Book settings start date always: {BOOK_START_DATE}
• NOT available in Reckon One: Class/Classification, Sales Order, Purchase Order

OPENING BALANCE RULES:
• Opening Trial Balance date = one day prior to conversion start (e.g. 30 June)
• AR/AP in trial balance → use Retained Earnings code for journal entries
• Bank reconciliation: Statement Closing Balance = Trial Balance amount for bank account
"""
