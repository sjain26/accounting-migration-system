# main.py — CLI entry point
import asyncio
from datetime  import datetime, timezone
from rich.console import Console
from rich.panel   import Panel
from rich.table   import Table

from graph   import build_graph
from memory  import save_run, get_memory_stats

console = Console()

SOURCE_ACCOUNTS = [
    {"code": "1001-100", "name": "Cash and bank balances",     "type": "Asset"},
    {"code": "1002-200", "name": "Sundry debtors",             "type": "Asset"},
    {"code": "2001-300", "name": "Sundry creditors",           "type": "Liability"},
    {"code": "5200-610", "name": "Deferred tax liability",     "type": "Liability"},
    {"code": "4001-500", "name": "Sales – domestic operations","type": "Revenue"},
    {"code": "7100-810", "name": "Misc. provisions – legacy",  "type": "Liability"},
]
TARGET_ACCOUNTS = [
    {"code": "0001-100010", "name": "Cash and bank",           "type": "Asset"},
    {"code": "0001-110010", "name": "Trade receivables",       "type": "Asset"},
    {"code": "0001-200010", "name": "Trade payables",          "type": "Liability"},
    {"code": "0001-210020", "name": "Deferred tax liability",  "type": "Liability"},
    {"code": "0001-400010", "name": "Revenue from operations", "type": "Revenue"},
]
JOURNAL_ENTRIES = [
    {"ref":"JV-001","date":"2024-04-01","desc":"Opening balance transfer",      "amount":48235000,"type":"OB"},
    {"ref":"JV-002","date":"2024-03-31","desc":"Depreciation accrual Q4 FY23",  "amount":1205000, "type":"Accrual"},
    {"ref":"JV-003","date":"2024-03-29","desc":"Advance – delivery in Apr",     "amount":21000000,"type":"Revenue"},
    {"ref":"JV-002","date":"2024-03-31","desc":"Depreciation accrual Q4 FY23",  "amount":1205000, "type":"Accrual"},
]
SOURCE_TB = {
    "0001-100010": ("Cash and bank",     12_45_82_310),
    "0001-110010": ("Trade receivables", 48_72_14_500),
    "0001-200010": ("Trade payables",    22_34_60_000),
    "0001-400010": ("Revenue",          184_52_30_000),
}
MIGRATED_TB = {
    "0001-100010": 12_45_82_310,
    "0001-110010": 48_72_14_500,
    "0001-200010": 22_34_60_000,
    "0001-400010":184_52_28_500,
}


def main():
    console.print(Panel.fit(
        "[bold purple]AI Accounting Migration System[/bold purple]\n"
        "LangGraph · Groq · Instructor · LlamaIndex · AutoLearn",
        border_style="purple"
    ))

    stats = get_memory_stats()
    console.print(f"Memory: [green]{stats['mappings']} mappings[/green] | "
                  f"[yellow]{stats['human_approved']} human-approved[/yellow] | "
                  f"[cyan]{stats['patterns']} patterns[/cyan]\n")

    graph      = build_graph()
    started_at = datetime.now(timezone.utc).isoformat()
    run_id     = f"cli-{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')}"

    initial_state = {
        "source_accounts": SOURCE_ACCOUNTS,
        "target_accounts": TARGET_ACCOUNTS,
        "journal_entries": JOURNAL_ENTRIES,
        "source_tb":       SOURCE_TB,
        "migrated_tb":     MIGRATED_TB,
        "cutoff_date":     "2024-03-31",
        "mappings":        [],
        "anomalies":       [],
        "recon_result":    {},
        "hitl_queue":      [],
        "errors":          [],
        "run_complete":    False,
        "progress_log":    [],
    }

    config = {"configurable": {"thread_id": run_id}}
    final  = graph.invoke(initial_state, config=config)

    # Print mappings
    t = Table(title="Account mappings", show_lines=False)
    t.add_column("Source",  style="dim",  width=12)
    t.add_column("Target",  style="cyan", width=14)
    t.add_column("Conf",    justify="right", width=6)
    t.add_column("Status",  width=10)
    for m in final["mappings"]:
        sc = "green" if m.get("status")=="approved" else "yellow" if m.get("status")=="review" else "red"
        t.add_row(m.get("source_code",""), m.get("target_code","UNMAPPED"),
                  str(m.get("confidence",0))+"%",
                  f"[{sc}]{m.get('status','?')}[/{sc}]")
    console.print(t)

    # Anomalies
    if final["anomalies"]:
        at = Table(title="Anomalies", show_lines=False)
        at.add_column("Ref", width=10)
        at.add_column("Severity", width=9)
        at.add_column("Finding", width=55)
        for a in final["anomalies"]:
            sc = "red" if a["severity"]=="high" else "yellow"
            at.add_row(a["ref"], f"[{sc}]{a['severity']}[/{sc}]", a["finding"])
        console.print(at)

    recon = final.get("recon_result", {})
    sc    = "green" if recon.get("overall_status")=="PASSED" else "yellow"
    console.print(Panel(
        f"[bold]TB Status:[/bold] [{sc}]{recon.get('overall_status','?')}[/{sc}]  "
        f"| Risk: {recon.get('risk_level','?')}\n\n"
        f"{recon.get('summary','')}\n\n" +
        "\n".join(f"  {i+1}. {s}" for i, s in enumerate(recon.get("next_steps", []))),
        title="Reconciliation", border_style="green"
    ))

    approved = sum(1 for m in final["mappings"] if m.get("status")=="approved")
    review   = sum(1 for m in final["mappings"] if m.get("status")=="review")
    errors   = sum(1 for m in final["mappings"] if m.get("status")=="error")

    save_run(run_id, "complete", len(final["mappings"]), approved, review,
             errors, len(final["anomalies"]), recon.get("overall_status","?"), started_at)

    console.print(f"\n[bold green]Done:[/bold green] {approved} approved · "
                  f"{review} review · {errors} errors · "
                  f"{len(final['anomalies'])} anomalies\n")


if __name__ == "__main__":
    main()
