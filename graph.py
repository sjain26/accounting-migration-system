# graph.py — LangGraph state machine
import asyncio
from typing       import TypedDict, Annotated
from langgraph.graph             import StateGraph, END
from langgraph.checkpoint.memory import MemorySaver
import operator

from agents import mapping_agent_async, anomaly_agent_async, reconcile_agent_async


class MigrationState(TypedDict):
    # Inputs
    source_accounts  : list[dict]
    target_accounts  : list[dict]
    journal_entries  : list[dict]
    source_tb        : dict
    migrated_tb      : dict
    cutoff_date      : str
    # Outputs (accumulated across nodes)
    mappings         : Annotated[list, operator.add]
    anomalies        : Annotated[list, operator.add]
    recon_result     : dict
    hitl_queue       : Annotated[list, operator.add]
    errors           : Annotated[list, operator.add]
    run_complete     : bool
    progress_log     : Annotated[list, operator.add]


def mapping_node(state: MigrationState) -> dict:
    results, hitl_q = asyncio.run(
        mapping_agent_async(state["source_accounts"], state["target_accounts"])
    )
    return {
        "mappings":    results,
        "hitl_queue":  hitl_q,
        "progress_log": [f"Mapping complete: {len(results)} accounts processed, "
                         f"{len(hitl_q)} queued for HITL"]
    }


def anomaly_node(state: MigrationState) -> dict:
    anomalies = asyncio.run(
        anomaly_agent_async(state["journal_entries"], state["cutoff_date"])
    )
    return {
        "anomalies":    anomalies,
        "progress_log": [f"Anomaly scan complete: {len(anomalies)} issues found"]
    }


def reconcile_node(state: MigrationState) -> dict:
    result = asyncio.run(
        reconcile_agent_async(state["source_tb"], state["migrated_tb"])
    )
    return {
        "recon_result": result,
        "progress_log": [f"Reconciliation: {result.get('overall_status', 'UNKNOWN')} "
                         f"| variance INR {result.get('net_variance', 0):,}"]
    }


def hitl_node(state: MigrationState) -> dict:
    # In Streamlit UI mode, HITL items are surfaced via the UI, not terminal
    # This node just passes through — Streamlit handles the review
    return {
        "progress_log": [f"HITL: {len(state['hitl_queue'])} items pending human review"]
    }


def report_node(state: MigrationState) -> dict:
    return {
        "run_complete": True,
        "progress_log": ["Pipeline complete — report ready"]
    }


def route_after_mapping(state: MigrationState) -> str:
    return "hitl" if state["hitl_queue"] else "anomaly"


def route_after_hitl(state: MigrationState) -> str:
    return "anomaly"


def build_graph():
    g = StateGraph(MigrationState)

    g.add_node("mapping",   mapping_node)
    g.add_node("anomaly",   anomaly_node)
    g.add_node("hitl",      hitl_node)
    g.add_node("reconcile", reconcile_node)
    g.add_node("report",    report_node)

    g.set_entry_point("mapping")
    g.add_conditional_edges("mapping", route_after_mapping,
                             {"hitl": "hitl", "anomaly": "anomaly"})
    g.add_edge("hitl",      "anomaly")
    g.add_edge("anomaly",   "reconcile")
    g.add_edge("reconcile", "report")
    g.add_edge("report",    END)

    return g.compile(checkpointer=MemorySaver())
