# server.py
import json
from typing import List, Dict, Any, Optional, Tuple
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from fastapi.middleware.cors import CORSMiddleware
import pandas as pd
import duckdb
import hashlib
import io
import traceback
import os
import re
import tempfile
from pathlib import Path

# ---- Optional Spark import (for validation / run_code)
try:
    from pyspark.sql import SparkSession, DataFrame as SparkDataFrame  # type: ignore
    _SPARK_AVAILABLE = True
except Exception:
    _SPARK_AVAILABLE = False


app = FastAPI()
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ==========================
# Inputs / Models
# ==========================
class Edge(BaseModel):
    id: Optional[str] = None
    source: str
    target: str


class NodeData(BaseModel):
    label: Optional[str] = None
    type: str
    color: Optional[str] = None

    # flexible payload
    path: Optional[str] = None               # for CSV source or sink
    _fileText: Optional[str] = None          # uploaded CSV contents (browser)
    columns: Optional[str] = None            # select
    schema: Optional[List[Dict[str, Any]]] = None
    expr: Optional[str] = None               # filter / formula
    groupBy: Optional[str] = None            # summarize
    measures: Optional[List[Dict[str, Any]]] = None
    newCol: Optional[str] = None             # formula
    sortSpec: Optional[str] = None           # sort
    mode: Optional[str] = None               # sample
    n: Optional[Any] = None
    frac: Optional[Any] = None
    seed: Optional[Any] = None

    # join
    how: Optional[str] = None
    left_on: Optional[str] = None
    right_on: Optional[str] = None
    dedupeLeft: Optional[bool] = None
    dedupeRight: Optional[bool] = None
    dedupePick: Optional[str] = None
    dedupeOrderCol: Optional[str] = None


class Node(BaseModel):
    id: str
    data: NodeData


class RunRequest(BaseModel):
    nodes: List[Node]
    edges: List[Edge]
    lang: str  # "python" | "sql" | "spark"
    previewId: Optional[str] = None          # if set -> run only up to that node


class CodeRunRequest(BaseModel):
    """
    For "compiler mode": run user-edited code directly and return preview.
    """
    lang: str                                # "python" | "sql" | "spark"
    code: str
    nodes: List[Node]                        # needed for in-memory CSV -> temp files
    edges: List[Edge] = []                   # not strictly required but kept for symmetry
    previewId: Optional[str] = None          # not used yet, but kept for future


# ==========================
# Helpers
# ==========================
def topo_sort(nodes: List[Node], edges: List[Edge]) -> List[str]:
    incoming = {n.id: 0 for n in nodes}
    outs = {n.id: [] for n in nodes}
    for e in edges:
        incoming[e.target] = incoming.get(e.target, 0) + 1
        outs[e.source].append(e.target)
    q = [n.id for n in nodes if incoming.get(n.id, 0) == 0]
    order: List[str] = []
    while q:
        cur = q.pop(0)
        order.append(cur)
        for t in outs.get(cur, []):
            incoming[t] -= 1
            if incoming[t] == 0:
                q.append(t)
    if len(order) != len(nodes):  # cycle fallback = original order
        return [n.id for n in nodes]
    return order


def id_map(nodes: List[Node]) -> Dict[str, Node]:
    return {n.id: n for n in nodes}


def friendly_name(label: Optional[str], counts: Dict[str, int]) -> str:
    base = (label or "node").lower()
    base = "".join([c if c.isalnum() else "_" for c in base]).strip("_") or "node"
    counts[base] = counts.get(base, 0) + 1
    return base if counts[base] == 1 else f"{base}_{counts[base]}"


def subgraph_up_to(
    nodes: List[Node],
    edges: List[Edge],
    target_id: Optional[str],
) -> Tuple[List[Node], List[Edge]]:
    """
    Keep only nodes that are ancestors of target_id (including itself).
    If target_id is None -> return full graph.
    """
    if not target_id:
        return nodes, edges
    preds: Dict[str, List[str]] = {n.id: [] for n in nodes}
    for e in edges:
        preds[e.target].append(e.source)
    keep = set()
    stack = [target_id]
    while stack:
        cur = stack.pop()
        if cur in keep:
            continue
        keep.add(cur)
        stack.extend(preds.get(cur, []))
    n2 = [n for n in nodes if n.id in keep]
    e2 = [e for e in edges if e.source in keep and e.target in keep]
    return n2, e2


def df_signature(df: pd.DataFrame, limit: int = 200) -> Dict[str, Any]:
    cols = list(df.columns)
    dtypes = [str(t) for t in df.dtypes.tolist()]
    sample = df.head(limit)
    buf = io.StringIO()
    sample.to_csv(buf, index=False)
    digest = hashlib.md5(buf.getvalue().encode("utf-8")).hexdigest()
    return {"columns": cols, "dtypes": dtypes, "rows": int(len(df)), "sample_md5": digest}


def compare_signatures(a: Dict[str, Any], b: Dict[str, Any]) -> Tuple[bool, str]:
    if a["columns"] != b["columns"]:
        return False, f"Columns differ.\nA: {a['columns']}\nB: {b['columns']}"
    if a["rows"] != b["rows"]:
        return False, f"Row count differs. A={a['rows']} B={b['rows']}"
    if a["sample_md5"] != b["sample_md5"]:
        return False, "Sample hash differs (first rows content mismatch)."
    return True, "Match."


def quote_ident(col: str) -> str:
    if col is None:
        return col
    c = str(col)
    if c.replace("_", "").isalnum() and not c[0].isdigit():
        return c
    return '"' + c.replace('"', '""') + '"'


def _gather_source_csv_text(nodes: List[Node]) -> Dict[str, str]:
    """Return {original_path_or_name: file_text} for all source.csv that have in-memory text."""
    out: Dict[str, str] = {}
    for n in nodes:
        if n.data.type == "source.csv":
            path = (n.data.path or "").strip() or "uploaded.csv"
            if (
                n.data._fileText
                and isinstance(n.data._fileText, str)
                and n.data._fileText.strip()
            ):
                out[path] = n.data._fileText
    return out


def _materialize_temp_csvs(nodes: List[Node]) -> Dict[str, str]:
    """
    Create temp files on disk for any in-memory CSVs.
    Returns {original_path: temp_path}.
    """
    mapping: Dict[str, str] = {}
    src = _gather_source_csv_text(nodes)
    if not src:
        return mapping
    tmpdir = Path(tempfile.mkdtemp(prefix="etl_uploads_"))
    for orig, text in src.items():
        fname = Path(orig).name or "uploaded.csv"
        tpath = tmpdir / fname
        tpath.write_text(text, encoding="utf-8")
        mapping[orig] = str(tpath)
    return mapping


def _rewrite_paths_in_python(code: str, replacements: Dict[str, str]) -> str:
    """
    Replace pd.read_csv(r"...") occurrences that match original paths with temp paths.
    """
    for orig, new in replacements.items():
        pattern = re.compile(rf'pd\.read_csv\(\s*r?["\']{re.escape(orig)}["\']\s*\)')
        code = pattern.sub(f'pd.read_csv(r"{new}")', code)
    return code


def _rewrite_paths_in_sql(code: str, replacements: Dict[str, str]) -> str:
    """
    Replace read_csv_auto('...') occurrences with temp paths.
    """
    for orig, new in replacements.items():
        pattern = re.compile(
            rf"read_csv_auto\(\s*['\"]{re.escape(orig)}['\"]\s*,?\s*header\s*=\s*true\s*\)",
            re.IGNORECASE,
        )
        code = pattern.sub(f"read_csv_auto('{new}', header=true)", code)
        pattern2 = re.compile(
            rf"read_csv_auto\(\s*['\"]{re.escape(orig)}['\"]\s*\)",
            re.IGNORECASE,
        )
        code = pattern2.sub(f"read_csv_auto('{new}', header=true)", code)
    return code


def _rewrite_paths_in_spark(code: str, replacements: Dict[str, str]) -> str:
    """
    Replace spark.read.option('header', True).csv('...') occurrences with temp paths.
    """
    for orig, new in replacements.items():
        pattern = re.compile(
            rf"spark\.read\.option\(\s*['\"]header['\"],\s*True\s*\)\.csv\(\s*['\"]{re.escape(orig)}['\"]\s*\)"
        )
        code = pattern.sub(
            f"spark.read.option('header', True).csv('{new}')",
        )
    return code


# ==========================
# Codegen (Python / SQL / Spark)
# ==========================
def gen_python(nodes: List[Node], edges: List[Edge]) -> str:
    id2 = id_map(nodes)
    order = topo_sort(nodes, edges)
    parents: Dict[str, List[str]] = {n.id: [] for n in nodes}
    for e in edges:
        parents[e.target].append(e.source)
    counts: Dict[str, int] = {}
    names = {nid: friendly_name(id2[nid].data.label, counts) for nid in order}

    L: List[str] = []
    L.append("# Auto-generated by ETL Studio (pandas)")
    L.append("import pandas as pd")
    L.append("")

    for nid in order:
        n = id2[nid]
        v = names[nid]
        ps = [names[p] for p in parents[nid]]
        t = n.data.type

        if t == "source.csv":
            path = (n.data.path or "uploaded.csv")
            L += [
                f"# Source: {n.data.label or 'CSV'}",
                f'{v} = pd.read_csv(r"{path}")',
                "",
            ]

        elif t == "transform.select":
            cols_str = (n.data.columns or "*").strip()
            if cols_str in ("", "*"):
                L += [f"{v} = {ps[0]}.copy()", ""]
            else:
                cols = [c.strip() for c in cols_str.split(",") if c.strip()]
                L += [f"{v} = {ps[0]}[{json.dumps(cols)}].copy()", ""]

        elif t == "transform.filter":
            expr = (n.data.expr or "").replace("\\", "\\\\").replace('"', '\\"')
            L += [f'{v} = {ps[0]}.query("{expr}")', ""]

        elif t == "transform.summarize":
            by = [c.strip() for c in (n.data.groupBy or "").split(",") if c.strip()]
            measures = n.data.measures or []
            agg_map: Dict[str, List[str]] = {}
            rename: Dict[str, str] = {}
            for m in measures:
                col, op, alias = m.get("col"), m.get("op"), m.get("as")
                if not col or not op:
                    continue
                key = alias or f"{op}_{col}"
                agg_map.setdefault(col, []).append(op)
                rename[f"{op}_{col}"] = key
            agg_obj = ", ".join(
                [
                    f"'{c}': [{', '.join([repr(op) for op in ops])}]"
                    for c, ops in agg_map.items()
                ]
            )
            tmp = f"{v}_tmp"
            if by:
                L.append(
                    f"{tmp} = {ps[0]}.groupby({json.dumps(by)}).agg({{{agg_obj}}}).reset_index()"
                )
            else:
                L.append(f"{tmp} = {ps[0]}.agg({{{agg_obj}}})")
            L.append(f"{v} = {tmp}.copy()")
            L.append(
                f"{v}.columns = ['_'.join([str(c) for c in col]).strip('_') if isinstance(col, tuple) else col for col in {v}.columns]"
            )
            for k, alias in rename.items():
                L.append(f"{v}.rename(columns={{'{k}': '{alias}'}}, inplace=True)")
            L.append("")

        elif t == "transform.formula":
            new_col = n.data.newCol or "new_column"
            expr = (n.data.expr or "0").replace("\\", "\\\\").replace('"', '\\"')
            L += [
                f"{v} = {ps[0]}.copy()",
                f'{v}[{json.dumps(new_col)}] = {v}.eval("{expr}", engine="python")',
                "",
            ]

        elif t == "transform.sort":
            spec = (n.data.sortSpec or "").strip()
            if not spec:
                L += [f"{v} = {ps[0]}.copy()", ""]
            else:
                pieces = [p.strip() for p in spec.split(",") if p.strip()]
                cols = [p.split()[0] for p in pieces]
                ascending = [not p.lower().endswith("desc") for p in pieces]
                L += [
                    f"{v} = {ps[0]}.sort_values({json.dumps(cols)}, ascending={json.dumps(ascending)})",
                    "",
                ]

        elif t == "transform.sample":
            mode = n.data.mode or "rows"
            if mode == "fraction":
                f = float(n.data.frac or 0.1)
                L += [f"{v} = {ps[0]}.sample(frac={f}, random_state=None)", ""]
            else:
                N = int(n.data.n or 100)
                L += [f"{v} = {ps[0]}.sample(n={N}, random_state=None)", ""]

        elif t == "transform.join":
            how = (n.data.how or "inner")
            Lk = [c.strip() for c in (n.data.left_on or "id").split(",") if c.strip()]
            Rk = [c.strip() for c in (n.data.right_on or "id").split(",") if c.strip()]
            left_arg = json.dumps(Lk) if len(Lk) > 1 else json.dumps(Lk[0])
            right_arg = json.dumps(Rk) if len(Rk) > 1 else json.dumps(Rk[0])
            L += [
                f"{v} = {ps[0]}.merge({ps[1]}, how={json.dumps(how)}, left_on={left_arg}, right_on={right_arg})",
                "",
            ]

        elif t == "sink.csv":
            outp = n.data.path or "out.csv"
            L += [
                f'{ps[0]}.to_csv(r"{outp}", index=False)',
                f"# wrote: {outp}",
                "",
            ]

        else:
            L += [f"# TODO: {t}", ""]

    if order:
        counts2: Dict[str, int] = {}
        name2 = {nid: friendly_name(id2[nid].data.label, counts2) for nid in order}
        L.append(f"result = {name2[order[-1]]}")
        L.append("")
    return "\n".join(L)


def gen_sql(nodes: List[Node], edges: List[Edge]) -> str:
    id2 = id_map(nodes)
    order = topo_sort(nodes, edges)
    parents: Dict[str, List[str]] = {n.id: [] for n in nodes}
    for e in edges:
        parents[e.target].append(e.source)
    counts: Dict[str, int] = {}
    cte = {nid: friendly_name(id2[nid].data.label, counts) for nid in order}

    parts = ["-- Auto-generated by ETL Studio (SQL, DuckDB-safe)"]
    ctes: List[str] = []

    for nid in order:
        n = id2[nid]
        p = [cte[pid] for pid in parents[nid]]
        alias = cte[nid]
        t = n.data.type

        if t == "source.csv":
            src = f"SELECT * FROM read_csv_auto('{n.data.path or 'uploaded.csv'}', header=true)"
            ctes.append(f"{alias} AS ({src})")

        elif t == "transform.select":
            cols_str = (n.data.columns or "*").strip()
            select_list = (
                "*"
                if not cols_str or cols_str == "*"
                else ", ".join(
                    [quote_ident(c.strip()) for c in cols_str.split(",") if c.strip()]
                )
            )
            ctes.append(f"{alias} AS (SELECT {select_list} FROM {p[0]})")

        elif t == "transform.filter":
            raw = (n.data.expr or "1=1")
            ctes.append(f"{alias} AS (SELECT * FROM {p[0]} WHERE {raw})")

        elif t == "transform.summarize":
            by = [
                quote_ident(c.strip())
                for c in (n.data.groupBy or "").split(",")
                if c.strip()
            ]
            measures = n.data.measures or []
            exprs: List[str] = []
            for m in measures:
                col, op, alias_col = m.get("col"), m.get("op"), m.get("as")
                if not col or not op:
                    continue
                out_name = alias_col or f"{op}_{col}"
                exprs.append(
                    f"{op}({quote_ident(col)}) AS {quote_ident(out_name)}"
                )
            select_parts = (
                ", ".join([*by, *exprs]) if (by or exprs) else "*"
            )
            group = f" GROUP BY {', '.join(by)}" if by else ""
            ctes.append(f"{alias} AS (SELECT {select_parts} FROM {p[0]}{group})")

        elif t == "transform.formula":
            new_col = n.data.newCol or "new_column"
            expr = (n.data.expr or "0")
            ctes.append(
                f"{alias} AS (SELECT *, ({expr}) AS {quote_ident(new_col)} FROM {p[0]})"
            )

        elif t == "transform.sort":
            spec = (n.data.sortSpec or "").strip()
            if not spec:
                ctes.append(f"{alias} AS (SELECT * FROM {p[0]})")
            else:
                ob = ", ".join(
                    [
                        f"{quote_ident(s.split()[0])} {'DESC' if s.lower().endswith('desc') else 'ASC'}"
                        for s in [x.strip() for x in spec.split(",") if x.strip()]
                    ]
                )
                ctes.append(
                    f"{alias} AS (SELECT * FROM {p[0]} ORDER BY {ob})"
                )

        elif t == "transform.sample":
            mode = n.data.mode or "rows"
            if mode == "fraction":
                f = float(n.data.frac or 0.1)
                ctes.append(
                    f"{alias} AS (SELECT * FROM {p[0]} WHERE random() < {f})"
                )
            else:
                N = int(n.data.n or 100)
                ctes.append(
                    f"{alias} AS (SELECT * FROM {p[0]} USING SAMPLE {N} ROWS)"
                )

        elif t == "transform.join":
            how = (n.data.how or "INNER").upper()
            lk = [
                quote_ident(c.strip())
                for c in (n.data.left_on or "id").split(",")
                if c.strip()
            ]
            rk = [
                quote_ident(c.strip())
                for c in (n.data.right_on or "id").split(",")
                if c.strip()
            ]
            on_expr = " AND ".join(
                [
                    f"{lk[i]} = {rk[i if i < len(rk) else 0]}"
                    for i in range(len(lk))
                ]
            )
            ctes.append(
                f"""{alias} AS (
  SELECT *
  FROM {p[0]}
  {how} JOIN {p[1]}
    ON {on_expr}
)"""
            )

        elif t == "inspect.deepdive":
            ctes.append(f"{alias} AS (SELECT * FROM {p[0]})")

        elif t == "sink.csv":
            ctes.append(f"{alias} AS (SELECT * FROM {p[0]})")

        else:
            ctes.append(f"{alias} AS (SELECT * FROM {p[0]})")

    last = cte[order[-1]] if order else "final"
    parts.append("WITH\n  " + ",\n  ".join(ctes))
    parts.append(f"SELECT * FROM {last};")
    return "\n".join(parts)


def gen_spark(nodes: List[Node], edges: List[Edge]) -> str:
    id2 = id_map(nodes)
    order = topo_sort(nodes, edges)
    parents: Dict[str, List[str]] = {n.id: [] for n in nodes}
    for e in edges:
        parents[e.target].append(e.source)
    counts: Dict[str, int] = {}
    names = {nid: friendly_name(id2[nid].data.label, counts) for nid in order}

    L: List[str] = []
    L.append("# Auto-generated by ETL Studio (PySpark)")
    L.append("from pyspark.sql import SparkSession, functions as F")
    L.append("spark = SparkSession.builder.appName('etl-studio').getOrCreate()")
    L.append("")

    for nid in order:
        n = id2[nid]
        v = names[nid]
        ps = [names[p] for p in parents[nid]]
        t = n.data.type

        if t == "source.csv":
            path = (n.data.path or "uploaded.csv")
            L.append(
                f"{v} = spark.read.option('header', True).csv({json.dumps(path)})"
            )

        elif t == "transform.select":
            cols_str = (n.data.columns or "*").strip()
            if cols_str in ("", "*"):
                L.append(f"{v} = {ps[0]}")
            else:
                cols = [c.strip() for c in cols_str.split(",") if c.strip()]
                L.append(
                    f"{v} = {ps[0]}.select({', '.join([repr(c) for c in cols])})"
                )

        elif t == "transform.filter":
            expr = (n.data.expr or "1=1")
            L.append(f"{v} = {ps[0]}.filter({json.dumps(expr)})")

        elif t == "transform.summarize":
            by = [
                c.strip()
                for c in (n.data.groupBy or "").split(",")
                if c.strip()
            ]
            measures = n.data.measures or []
            aggs: List[str] = []
            for m in measures:
                col, op, alias_col = m.get("col"), m.get("op"), m.get("as")
                if not col or not op:
                    continue
                out = alias_col or f"{op}_{col}"
                aggs.append(f"F.{op}('{col}').alias('{out}')")
            if by:
                L.append(
                    f"{v} = {ps[0]}.groupBy({', '.join([repr(c) for c in by])}).agg({', '.join(aggs)})"
                )
            else:
                L.append(f"{v} = {ps[0]}.agg({', '.join(aggs)})")

        elif t == "transform.formula":
            new_col = n.data.newCol or "new_column"
            expr = (n.data.expr or "0")
            L.append(
                f"{v} = {ps[0]}.withColumn('{new_col}', F.expr({json.dumps(expr)}))"
            )

        elif t == "transform.sort":
            spec = (n.data.sortSpec or "").strip()
            if not spec:
                L.append(f"{v} = {ps[0]}")
            else:
                orders: List[str] = []
                for p in [s.strip() for s in spec.split(",") if s.strip()]:
                    c = p.split()[0]
                    desc = p.lower().endswith("desc")
                    orders.append(f"F.col('{c}').{'desc' if desc else 'asc'}()")
                L.append(f"{v} = {ps[0]}.orderBy({', '.join(orders)})")

        elif t == "transform.sample":
            mode = n.data.mode or "rows"
            if mode == "fraction":
                f = float(n.data.frac or 0.1)
                L.append(f"{v} = {ps[0]}.sample({f})")
            else:
                N = int(n.data.n or 100)
                L.append(f"__cnt = {ps[0]}.count()")
                L.append(f"{v} = {ps[0]}.limit(min({N}, __cnt))")

        elif t == "transform.join":
            how = (n.data.how or "inner")
            lk = [
                c.strip()
                for c in (n.data.left_on or "id").split(",")
                if c.strip()
            ]
            rk = [
                c.strip()
                for c in (n.data.right_on or "id").split(",")
                if c.strip()
            ]
            cond = " & ".join(
                [
                    f"{ps[0]}.{lk[i]} == {ps[1]}.{rk[i if i < len(rk) else 0]}"
                    for i in range(len(lk))
                ]
            )
            L.append(
                f"{v} = {ps[0]}.join({ps[1]}, on=({cond}), how='{how}')"
            )

        elif t == "sink.csv":
            outp = n.data.path or "out_spark.csv"
            L.append(
                f"{ps[0]}.coalesce(1).write.mode('overwrite').option('header', True).csv({json.dumps(outp)})"
            )

        else:
            L.append(f"# TODO: {t}")
        L.append("")

    if order:
        L.append(f"result = {names[order[-1]]}")
    return "\n".join(L)


# ==========================
# Execution (pandas truth)
# ==========================
def exec_workflow_pandas(
    nodes: List[Node],
    edges: List[Edge],
    preview_id: Optional[str],
) -> Tuple[pd.DataFrame, Dict[str, str]]:
    """
    Execute (only up to preview_id if provided).
    Returns (final_df, node_errors).
    - This is the "ground truth" engine.
    - Used for node preview + final output + validation.
    """
    n2, e2 = subgraph_up_to(nodes, edges, preview_id)
    order = topo_sort(n2, e2)
    id2 = id_map(n2)
    parents: Dict[str, List[str]] = {n.id: [] for n in n2}
    for e in e2:
        parents[e.target].append(e.source)

    frames: Dict[str, pd.DataFrame] = {}
    node_errors: Dict[str, str] = {}

    for nid in order:
        n = id2[nid]
        t = n.data.type
        try:
            ps = [frames.get(p, pd.DataFrame()) for p in parents[nid]]

            if t == "source.csv":
                file_text = (
                    n.data._fileText if isinstance(n.data._fileText, str) else None
                )
                file_path = (n.data.path or "").strip() or None
                if file_text and file_text.strip():
                    df = pd.read_csv(io.StringIO(file_text))
                elif file_path and os.path.exists(file_path):
                    df = pd.read_csv(file_path)
                else:
                    raise FileNotFoundError(
                        f"CSV Source '{n.data.label or nid}' has no uploaded data or readable path."
                    )
                frames[nid] = df

            elif t == "transform.select":
                cols_str = (n.data.columns or "*").strip()
                df = (
                    ps[0].copy()
                    if cols_str in ("", "*")
                    else ps[0][
                        [c.strip() for c in cols_str.split(",") if c.strip()]
                    ].copy()
                )
                schema = n.data.schema or []
                if schema:
                    for s in schema:
                        name = (s.get("name") or "").strip()
                        dt = (s.get("dtype") or "string").strip().lower()
                        if not name or name not in df.columns:
                            continue
                        if dt == "integer":
                            df[name] = pd.to_numeric(
                                df[name], errors="coerce"
                            ).astype("Int64")
                        elif dt == "float":
                            df[name] = pd.to_numeric(df[name], errors="coerce")
                        elif dt == "boolean":
                            df[name] = df[name].astype("boolean")
                        elif dt == "date":
                            df[name] = pd.to_datetime(
                                df[name], errors="coerce"
                            ).dt.date
                        elif dt == "datetime":
                            df[name] = pd.to_datetime(df[name], errors="coerce")
                        else:
                            df[name] = df[name].astype("string")
                frames[nid] = df

            elif t == "transform.filter":
                expr = (n.data.expr or "True")
                try:
                    df = ps[0].query(expr)
                except Exception:
                    df = ps[0].copy()
                    node_errors[nid] = (
                        f"Filter expression failed; returned input unchanged. expr='{expr}'"
                    )
                frames[nid] = df

            elif t == "transform.summarize":
                by = [
                    c.strip()
                    for c in (n.data.groupBy or "").split(",")
                    if c.strip()
                ]
                measures = n.data.measures or []
                agg_map: Dict[str, List[str]] = {}
                rename: Dict[str, str] = {}
                for m in measures:
                    col, op, alias = m.get("col"), m.get("op"), m.get("as")
                    if not col or not op:
                        continue
                    key = alias or f"{op}_{col}"
                    agg_map.setdefault(col, []).append(op)
                    rename[f"{op}_{col}"] = key
                if by:
                    tmp = ps[0].groupby(by).agg(agg_map).reset_index()
                else:
                    tmp = ps[0].agg(agg_map)
                df = tmp.copy()
                df.columns = [
                    "_".join([str(c) for c in col]).strip("_")
                    if isinstance(col, tuple)
                    else col
                    for col in df.columns
                ]
                df.rename(columns=rename, inplace=True)
                frames[nid] = df

            elif t == "transform.formula":
                new_col = n.data.newCol or "new_column"
                expr = (n.data.expr or "0")
                df = ps[0].copy()
                try:
                    df[new_col] = df.eval(expr, engine="python")
                except Exception:
                    df[new_col] = None
                    node_errors[nid] = (
                        f"Formula evaluation failed; filled '{new_col}' "
                        f"with nulls. expr='{expr}'"
                    )
                frames[nid] = df

            elif t == "transform.sort":
                spec = (n.data.sortSpec or "").strip()
                if not spec:
                    df = ps[0].copy()
                else:
                    pieces = [p.strip() for p in spec.split(",") if p.strip()]
                    cols = [p.split()[0] for p in pieces]
                    ascending = [not p.lower().endswith("desc") for p in pieces]
                    df = ps[0].sort_values(cols, ascending=ascending)
                frames[nid] = df

            elif t == "transform.sample":
                mode = n.data.mode or "rows"
                if mode == "fraction":
                    f = float(n.data.frac or 0.1)
                    df = ps[0].sample(frac=f, random_state=None)
                else:
                    N = int(n.data.n or 100)
                    df = ps[0].sample(
                        n=min(N, len(ps[0])),
                        random_state=None,
                    )
                frames[nid] = df

            elif t == "transform.join":
                how = n.data.how or "inner"
                Lk = [
                    c.strip()
                    for c in (n.data.left_on or "id").split(",")
                    if c.strip()
                ]
                Rk = [
                    c.strip()
                    for c in (n.data.right_on or "id").split(",")
                    if c.strip()
                ]
                df = ps[0].merge(
                    ps[1],
                    how=how,
                    left_on=Lk if len(Lk) > 1 else Lk[0],
                    right_on=Rk if len(Rk) > 1 else Rk[0],
                )
                frames[nid] = df

            elif t == "sink.csv":
                outp = n.data.path or "out.csv"
                ps[0].to_csv(outp, index=False)
                frames[nid] = ps[0]

            else:
                frames[nid] = ps[0] if ps else pd.DataFrame()

        except Exception as e:
            node_errors[nid] = f"{type(e).__name__}: {e}"
            frames[nid] = pd.DataFrame()

    final_df = frames[order[-1]] if order else pd.DataFrame()
    return final_df, node_errors


# ==========================
# Endpoints
# ==========================
@app.post("/run_workflow")
def run_workflow(req: RunRequest):
    """
    Main endpoint:
    - If previewId is None => final output of full workflow
    - If previewId is set  => output of subgraph up to that node
    Also returns codegen for python/sql/spark.
    """
    try:
        df, node_errors = exec_workflow_pandas(
            req.nodes,
            req.edges,
            req.previewId,
        )

        code_py = gen_python(req.nodes, req.edges)
        code_sql = gen_sql(req.nodes, req.edges)
        code_spark = gen_spark(req.nodes, req.edges)

        preview = json.loads(df.head(200).to_json(orient="records"))

        return {
            "preview": preview,
            "rows": int(len(df)),
            "columns": list(df.columns),
            "node_errors": node_errors,
            "code_py": code_py,
            "code_sql": code_sql,
            "code_spark": code_spark,
        }
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/generate_code")
def generate_code(req: RunRequest):
    """
    Pure codegen (if you ever want to call it separately).
    """
    try:
        return {
            "python": gen_python(req.nodes, req.edges),
            "sql": gen_sql(req.nodes, req.edges),
            "spark": gen_spark(req.nodes, req.edges),
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/validate_code")
def validate_code(req: RunRequest):
    """
    Compare backend pandas result vs the generated code’s result.
    Uses temp files for in-memory CSV sources.
    - python: exec generated code, take 'result' DataFrame
    - sql: run in duckdb
    - spark: exec generated PySpark code, convert result.toPandas()
    """
    try:
        # Truth = pandas engine
        truth_df, _errs = exec_workflow_pandas(
            req.nodes, req.edges, req.previewId
        )
        sig_truth = df_signature(truth_df)

        replacements = _materialize_temp_csvs(req.nodes)
        lang = (req.lang or "python").lower()

        # ---- SQL
        if lang == "sql":
            sql = gen_sql(req.nodes, req.edges)
            sql = _rewrite_paths_in_sql(sql, replacements)
            con = duckdb.connect()
            out = con.execute(sql).fetchdf()
            con.close()
            sig_other = df_signature(out)
            ok, why = compare_signatures(sig_truth, sig_other)
            return {"lang": "sql", "valid": ok, "reason": why}

        # ---- Python (pandas)
        elif lang == "python":
            code = gen_python(req.nodes, req.edges)
            code = _rewrite_paths_in_python(code, replacements)
            ns: Dict[str, Any] = {"pd": pd}
            exec(compile(code, "<etl-python>", "exec"), ns, ns)
            if "result" not in ns or not isinstance(ns["result"], pd.DataFrame):
                return {
                    "lang": "python",
                    "valid": False,
                    "reason": "No DataFrame named 'result' produced.",
                }
            sig_other = df_signature(ns["result"])
            ok, why = compare_signatures(sig_truth, sig_other)
            return {"lang": "python", "valid": ok, "reason": why}

        # ---- Spark
        elif lang == "spark":
            if not _SPARK_AVAILABLE:
                return {
                    "lang": "spark",
                    "valid": False,
                    "reason": "PySpark runtime not available; skipping validation.",
                }
            code = gen_spark(req.nodes, req.edges)
            code = _rewrite_paths_in_spark(code, replacements)
            ns: Dict[str, Any] = {}
            # this will import SparkSession inside the generated code
            exec(compile(code, "<etl-spark>", "exec"), ns, ns)
            result_obj = ns.get("result", None)
            if result_obj is None:
                return {
                    "lang": "spark",
                    "valid": False,
                    "reason": "No variable 'result' found in Spark code.",
                }
            try:
                import pandas as _pd  # local alias just to be explicit
                spark_df = result_obj  # type: ignore
                if not hasattr(spark_df, "toPandas"):
                    return {
                        "lang": "spark",
                        "valid": False,
                        "reason": "result is not a Spark DataFrame.",
                    }
                pdf = spark_df.toPandas()
            except Exception as e:
                return {
                    "lang": "spark",
                    "valid": False,
                    "reason": f"Spark execution failed: {e}",
                }
            sig_other = df_signature(pdf)
            ok, why = compare_signatures(sig_truth, sig_other)
            return {"lang": "spark", "valid": ok, "reason": why}

        else:
            return {
                "lang": lang,
                "valid": False,
                "reason": f"Unsupported language: {lang}",
            }

    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/run_code")
def run_code(req: CodeRunRequest):
    """
    "Compiler mode" endpoint:
    - Takes user-edited code for python/sql/spark
    - Executes it
    - Returns preview (rows/columns) so the CodeMirror panel can show the result.
    Uses the same temp CSV mechanism for in-browser uploads.
    """
    try:
        replacements = _materialize_temp_csvs(req.nodes)
        lang = (req.lang or "python").lower()
        code = req.code

        # ---------------- Python ----------------
        if lang == "python":
            code = _rewrite_paths_in_python(code, replacements)
            ns: Dict[str, Any] = {"pd": pd}
            exec(compile(code, "<user-python>", "exec"), ns, ns)
            obj = ns.get("result", None)
            if not isinstance(obj, pd.DataFrame):
                raise RuntimeError(
                    "Python code must define a pandas DataFrame variable named 'result'."
                )
            df = obj

        # ---------------- SQL (DuckDB) ----------------
        elif lang == "sql":
            code = _rewrite_paths_in_sql(code, replacements)
            con = duckdb.connect()
            df = con.execute(code).fetchdf()
            con.close()

        # ---------------- Spark ----------------
        elif lang == "spark":
            if not _SPARK_AVAILABLE:
                raise RuntimeError(
                    "PySpark runtime not available; cannot execute Spark code."
                )
            code = _rewrite_paths_in_spark(code, replacements)
            ns: Dict[str, Any] = {}
            exec(compile(code, "<user-spark>", "exec"), ns, ns)
            obj = ns.get("result", None)
            if obj is None or not hasattr(obj, "toPandas"):
                raise RuntimeError(
                    "Spark code must define a Spark DataFrame named 'result'."
                )
            df = obj.toPandas()  # type: ignore

        else:
            raise RuntimeError(f"Unsupported language: {lang}")

        sig = df_signature(df)
        preview = json.loads(df.head(200).to_json(orient="records"))
        return {
            "preview": preview,
            "rows": sig["rows"],
            "columns": sig["columns"],
        }

    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))
