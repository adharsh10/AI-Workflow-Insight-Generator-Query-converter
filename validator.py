# validator.py
from __future__ import annotations
from typing import Dict, Any, List
import pandas as pd
import duckdb


def normalize_df(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize dtypes and index for consistent comparison."""
    if df is None:
        return pd.DataFrame()
    df2 = df.copy()
    try:
        df2 = df2.convert_dtypes()
    except Exception:
        pass
    df2 = df2.reset_index(drop=True)
    return df2


def frame_meta(df: pd.DataFrame) -> Dict[str, Any]:
    """Extract simple metadata snapshot of a DataFrame."""
    df = normalize_df(df)
    return {
        "rows": int(len(df)),
        "columns": list(df.columns),
        "dtypes": [str(t) for t in df.dtypes.tolist()],
    }


def compare_meta(a: Dict[str, Any], b: Dict[str, Any]) -> Dict[str, Any]:
    """Compare shape and schema-level metadata."""
    return {
        "rows_equal": a.get("rows") == b.get("rows"),
        "columns_equal": a.get("columns") == b.get("columns"),
        "dtypes_equal": a.get("dtypes") == b.get("dtypes"),
        "exact_match": (
            a.get("rows") == b.get("rows")
            and a.get("columns") == b.get("columns")
            and a.get("dtypes") == b.get("dtypes")
        ),
    }


def validate_node_sql_equivalence(node_id: str, py_snap: pd.DataFrame, sql_query: str) -> Dict[str, Any]:
    """
    Run a single-node validation by executing the node's SQL
    and comparing its normalized result to the pandas snapshot.
    """
    try:
        con = duckdb.connect()
        df_sql = con.execute(sql_query).df()
        con.close()
        df_sql = normalize_df(df_sql)
    except Exception as e:
        return {"node": node_id, "error": str(e), "valid": False}

    py_meta = frame_meta(py_snap)
    sql_meta = frame_meta(df_sql)
    cmp = compare_meta(py_meta, sql_meta)
    return {
        "node": node_id,
        "valid": cmp["exact_match"],
        "meta_py": py_meta,
        "meta_sql": sql_meta,
        "cmp": cmp,
    }


def validate_workflow_sql(nodes, edges, snapshots: Dict[str, Any], code_sql: str) -> Dict[str, Any]:
    """
    Validate workflow node-by-node by comparing pandas snapshot vs SQL run result.
    """
    results = {}
    try:
        con = duckdb.connect()
        con.execute(code_sql)
        for node_id, snap in snapshots.items():
            try:
                df_sql = con.execute(f"SELECT * FROM {node_id}").df()
                df_sql = normalize_df(df_sql)
                py_meta = snap.get("meta", {})
                sql_meta = frame_meta(df_sql)
                cmp = compare_meta(py_meta, sql_meta)
                results[node_id] = {"cmp": cmp, "valid": cmp["exact_match"]}
            except Exception:
                results[node_id] = {"valid": False, "cmp": {"rows_equal": False}}
        con.close()
    except Exception as e:
        results["error"] = str(e)
    return results
