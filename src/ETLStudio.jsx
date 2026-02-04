// ETLStudio.jsx
import React, { useCallback, useEffect, useMemo, useRef, useState } from "react";
import ReactFlow, {
  Background,
  Controls,
  addEdge,
  applyEdgeChanges,
  applyNodeChanges,
} from "reactflow";
import "reactflow/dist/style.css";
import {
  Play, Square, Trash2, Download, Upload, Code,
  AlignHorizontalDistributeCenter, AlertTriangle, Eye
} from "lucide-react";
import { nanoid } from "nanoid";
import Papa from "papaparse";
import AICopilotPanel from "./components/AICopilotPanel";

/* ----------------------------------------------------------
   Tiny UI bits
---------------------------------------------------------- */
const Button = ({ className = "", children, ...props }) => (
  <button
    className={`px-3 py-2 rounded-2xl shadow-sm border border-gray-200 hover:shadow transition ${className}`}
    {...props}
  >
    {children}
  </button>
);
const SelectBox = ({ value, onChange, children }) => (
  <select
    value={value}
    onChange={(e) => onChange?.(e.target.value)}
    className="px-2 py-2 rounded-xl border border-gray-200 bg-white"
  >
    {children}
  </select>
);
const TextInput = ({ value, onChange, placeholder }) => (
  <input
    value={value}
    onChange={(e) => onChange?.(e.target.value)}
    placeholder={placeholder}
    className="px-3 py-2 rounded-xl border border-gray-200 w-full"
  />
);
const Label = ({ children }) => (
  <label className="text-xs font-medium text-gray-600">{children}</label>
);

/* ----------------------------------------------------------
   Node templates
---------------------------------------------------------- */
const NODE_TEMPLATES = [
  { type: "source.csv", label: "CSV Source", color: "#22c55e", data: { path: "data.csv", _fileName: "", _fileObj: null, _fileText: "" } },
  { type: "transform.select", label: "Select Columns", color: "#06b6d4", data: { columns: "*", schema: [] } },
  { type: "transform.filter", label: "Filter Rows", color: "#0ea5e9", data: { expr: "sales > 0" } },
  {
    type: "transform.join",
    label: "Join",
    color: "#a855f7",
    data: {
      how: "inner",
      left_on: "id",
      right_on: "id",
      dedupeLeft: false,
      dedupeRight: false,
      dedupePick: "first",
      dedupeOrderCol: ""
    }
  },
  {
    type: "transform.summarize",
    label: "Summarize / Group By",
    color: "#f59e0b",
    data: {
      groupBy: "category, class",
      measures: [{ col: "units", op: "sum", as: "units" }],
    },
  },
  // NEW: Formula
  {
    type: "transform.formula",
    label: "Formula",
    color: "#0ea5e9",
    data: {
      newCol: "new_column",
      expr: "units * 2",
    },
  },
  // NEW: Sort
  {
    type: "transform.sort",
    label: "Sort",
    color: "#22c55e",
    data: {
      sortSpec: "units desc",
    },
  },
  // NEW: Sample
  {
    type: "transform.sample",
    label: "Sample",
    color: "#10b981",
    data: {
      mode: "rows",   // rows | fraction
      n: 100,
      frac: 0.1,
      seed: "",
    },
  },
  { type: "inspect.deepdive", label: "Deep Dive", color: "#10b981", data: {} },
  { type: "sink.csv", label: "CSV Sink", color: "#ef4444", data: { path: "out.csv" } },
];

/* ----------------------------------------------------------
   Helpers
---------------------------------------------------------- */
const newNode = (tpl, position = { x: 140, y: 140 }) => ({
  id: nanoid(8),
  type: "default",
  position,
  sourcePosition: "right",
  targetPosition: "left",
  data: { ...tpl.data, label: tpl.label, type: tpl.type, color: tpl.color },
  style: { borderRadius: 16, border: `1px solid #e5e7eb`, padding: 8, background: "#fff" },
});
const NodeCard = ({ data }) => (
  <div className="min-w-[180px]">
    <div className="flex items-center gap-2">
      <div className="w-2 h-2 rounded-full" style={{ background: data.color }} />
      <div className="font-semibold text-sm">{data.label}</div>
    </div>
    <div className="text-[11px] text-gray-500 mt-1">{data.type}</div>
  </div>
);
const CodePane = ({ code, lang }) => (
  <div className="w-full h-full bg-[#0a0a0a] text-gray-100 rounded-2xl p-3 font-mono text-xs overflow-auto">
    <div className="flex items-center justify-between mb-2">
      <div className="flex items-center gap-2 text-gray-400"><Code size={14} /> Live code ({lang})</div>
      <Button className="text-gray-200 bg-[#111] border-gray-700" onClick={() => navigator.clipboard.writeText(code)}>Copy</Button>
    </div>
    <pre className="whitespace-pre"><code>{code}</code></pre>
  </div>
);

/* ----------------------------------------------------------
   Graph utils
---------------------------------------------------------- */
function topoSort(nodes, edges) {
  const incoming = new Map(nodes.map((n) => [n.id, 0]));
  const outs = new Map(nodes.map((n) => [n.id, []]));
  edges.forEach((e) => {
    incoming.set(e.target, (incoming.get(e.target) || 0) + 1);
    outs.get(e.source)?.push(e.target);
  });
  const q = nodes.filter((n) => (incoming.get(n.id) || 0) === 0).map((n) => n.id);
  const order = [];
  while (q.length) {
    const id = q.shift();
    order.push(id);
    for (const t of outs.get(id) || []) {
      incoming.set(t, incoming.get(t) - 1);
      if (incoming.get(t) === 0) q.push(t);
    }
  }
  return order.length === nodes.length ? order : nodes.map((n) => n.id);
}
function ancestorsOf(targetId, nodes, edges) {
  const pred = new Map(nodes.map(n => [n.id, []]));
  edges.forEach(e => pred.get(e.target).push(e.source));
  const seen = new Set();
  const stack = [targetId];
  while (stack.length) {
    const cur = stack.pop();
    if (seen.has(cur)) continue;
    seen.add(cur);
    for (const p of pred.get(cur) || []) stack.push(p);
  }
  return seen;
}
function subgraphUpTo(targetId, nodes, edges) {
  if (!targetId) return { nodes, edges };
  const keep = ancestorsOf(targetId, nodes, edges);
  const n2 = nodes.filter(n => keep.has(n.id));
  const e2 = edges.filter(e => keep.has(e.source) && keep.has(e.target));
  return { nodes: n2, edges: e2 };
}

/* ----------------------------------------------------------
   Codegen helpers
---------------------------------------------------------- */
function slugifyLabel(s) {
  return (s || "")
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "_")
    .replace(/^_+|_+$/g, "")
    .slice(0, 24) || "node";
}
function friendlyVarNames(order, id2node) {
  const names = {}, counts = {};
  order.forEach((id) => {
    const n = id2node[id];
    const base = slugifyLabel(n.data?.label) || "node";
    counts[base] = (counts[base] || 0) + 1;
    names[id] = counts[base] === 1 ? base : `${base}_${counts[base]}`;
  });
  return names;
}
const IDENT = /^[A-Za-z_][A-Za-z0-9_]*$/;
function quoteIdent(col) {
  if (col == null) return col;
  const c = String(col);
  return IDENT.test(c) ? c : `"${c.replace(/"/g, '""')}"`;
}
function softenNumericComparisons(expr) {
  if (!expr) return "";
  let out = expr.replace(/`([^`]+)`/g, (_, name) => quoteIdent(name));
  out = out.replace(
    /\b([A-Za-z_][A-Za-z0-9_]*|"(?:[^"]|"")+")\s*(<=|>=|=|<>|<|>)\s*([0-9]+(?:\.[0-9]+)?)\b/g,
    (m, col, op, num) => `TRY_CAST(${quoteIdent(col)} AS DOUBLE) ${op} ${num}`
  );
  return out;
}
const escRe = (s) => s.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");

/** Rewrite bare column mentions into helper or quoted identifiers. */
function rewriteExprWithColumns(expr, columns, forSql = false) {
  if (!expr || !Array.isArray(columns) || columns.length === 0) return expr || "";
  let out = expr;
  const hasHelpers = /\b(?:n|s|b)\s*\(/.test(out);
  const keys = [...columns].sort((a, b) => String(b).length - String(a).length);
  for (const key of keys) {
    const re = new RegExp(`(^|[^A-Za-z0-9_])(${escRe(String(key))})(?=$|[^A-Za-z0-9_])`, "g");
    out = out.replace(re, (_, pre, name) => {
      if (forSql) return `${pre}${quoteIdent(name)}`;
      if (hasHelpers) return `${pre}${name}`;
      return `${pre}n(${JSON.stringify(name)})`;
    });
  }
  return out;
}

/* ----------------------------------------------------------
   Python / SQL code generators
---------------------------------------------------------- */
function genPython(nodes, edges) {
  const id2node = Object.fromEntries(nodes.map((n) => [n.id, n]));
  const parents = Object.fromEntries(nodes.map((n) => [n.id, []]));
  edges.forEach((e) => parents[e.target].push(e.source));
  const order = topoSort(nodes, edges);
  const vname = friendlyVarNames(order, id2node);

  const L = (s = "") => s;
  const lines = [L("# Auto-generated by ETL Studio (pandas)"), L("import pandas as pd"), L("")];

  for (const id of order) {
    const n = id2node[id];
    const v = vname[id];
    const pVars = parents[id].map((pid) => vname[pid]);

    switch (n.data.type) {
      case "source.csv": {
        lines.push(L(`# Source: ${n.data.label || "CSV"}`));
        lines.push(L(`${v} = pd.read_csv(r"${n.data.path}")`));
        lines.push(L(""));
        break;
      }
      case "transform.select": {
        lines.push(L(`# Select columns`));
        if (pVars.length !== 1) { lines.push(L(`# WARN: select expects 1 input`)); lines.push(""); break; }
        const colsStr = (n.data.columns || "*").trim();
        const cols = colsStr === "*" ? null : colsStr.split(",").map((s) => s.trim()).filter(Boolean);
        if (!cols) lines.push(L(`${v} = ${pVars[0]}.copy()`));
        else lines.push(L(`${v} = ${pVars[0]}[${JSON.stringify(cols)}].copy()`));
        lines.push("");
        break;
      }
      case "transform.filter": {
        lines.push(L(`# Filter rows`));
        if (pVars.length !== 1) { lines.push(L(`# WARN: filter expects 1 input`)); lines.push(""); break; }
        const expr = (n.data.expr || "").replace(/\\"/g, '\\"');
        lines.push(L(`${v} = ${pVars[0]}.query("${expr}")`));
        lines.push("");
        break;
      }
      case "transform.summarize": {
        lines.push(L(`# Summarize / Group By`));
        if (pVars.length !== 1) { lines.push(L(`# WARN: summarize expects 1 input`)); lines.push(""); break; }
        const by = (n.data.groupBy || "").split(",").map(s=>s.trim()).filter(Boolean);
        const measures = Array.isArray(n.data.measures) ? n.data.measures : [];
        const aggMap = {};
        const renameMap = {};
        measures.forEach(({ col, op, as }) => {
          if (!col || !op) return;
          const key = as || `${op}_${col}`;
          if (!aggMap[col]) aggMap[col] = [];
          aggMap[col].push(op);
          renameMap[`${op}_${col}`] = key;
        });
        const aggObj = Object.entries(aggMap).map(([col,fns]) => `'${col}':[${fns.map(f=>`'${f}'`).join(", ")}]`).join(", ");
        const tmp = `${v}_tmp`;
        if (by.length) lines.push(L(`${tmp} = ${pVars[0]}.groupby(${JSON.stringify(by)}).agg({${aggObj}}).reset_index()`));
        else lines.push(L(`${tmp} = ${pVars[0]}.agg({${aggObj}})`));
        lines.push(L(`${v} = ${tmp}.copy()`));
        lines.push(L(`${v}.columns = ['_'.join([str(c) for c in col]).strip('_') if isinstance(col, tuple) else col for col in ${v}.columns]`));
        Object.entries(renameMap).forEach(([k, alias]) => lines.push(L(`${v}.rename(columns={'${k}': '${alias}'}, inplace=True)`)));
        lines.push("");
        break;
      }
      case "transform.formula": {
        lines.push(L(`# Formula`));
        if (pVars.length !== 1) { lines.push(L(`# WARN: formula expects 1 input`)); lines.push(""); break; }
        const newCol = n.data.newCol || "new_column";
        const expr = (n.data.expr || "0").replace(/\\/g, "\\\\").replace(/"/g, '\\"');
        lines.push(L(`${v} = ${pVars[0]}.copy()`));
        lines.push(L(`${v}["${newCol}"] = ${v}.eval("${expr}", engine="python")`));
        lines.push("");
        break;
      }
      case "transform.sort": {
        lines.push(L(`# Sort`));
        if (pVars.length !== 1) { lines.push(L(`# WARN: sort expects 1 input`)); lines.push(""); break; }
        const spec = (n.data.sortSpec || "").trim();
        if (!spec) { lines.push(L(`${v} = ${pVars[0]}.copy()`)); lines.push(""); break; }
        const pieces = spec.split(",").map(s=>s.trim()).filter(Boolean);
        const cols = pieces.map(p => p.split(/\s+/)[0]);
        const ascending = pieces.map(p => !/desc$/i.test(p));
        lines.push(L(`${v} = ${pVars[0]}.sort_values(${JSON.stringify(cols)}, ascending=${JSON.stringify(ascending)})`));
        lines.push("");
        break;
      }
      case "transform.sample": {
        lines.push(L(`# Sample`));
        if (pVars.length !== 1) { lines.push(L(`# WARN: sample expects 1 input`)); lines.push(""); break; }
        const mode = n.data.mode || "rows";
        if (mode === "fraction") {
          const frac = Number(n.data.frac || 0.1);
          lines.push(L(`${v} = ${pVars[0]}.sample(frac=${isFinite(frac)?frac:0.1}, random_state=None)`));
        } else {
          const nrows = Math.max(0, parseInt(n.data.n || 100, 10) || 0);
          lines.push(L(`${v} = ${pVars[0]}.sample(n=${nrows}, random_state=None)`));
        }
        lines.push("");
        break;
      }
      case "transform.join": {
        lines.push(L(`# Join dataframes`));
        if (pVars.length !== 2) { lines.push(L(`# WARN: join expects 2 inputs`)); lines.push(""); break; }
        const how = n.data.how || "inner";
        const leftKeys = (n.data.left_on || "id").split(",").map(s=>s.trim()).filter(Boolean);
        const rightKeys = (n.data.right_on || "id").split(",").map(s=>s.trim()).filter(Boolean);
        const leftArg = leftKeys.length > 1 ? `[${leftKeys.map(c=>`"${c}"`).join(", ")}]` : `"${leftKeys[0]}"`;
        const rightArg= rightKeys.length> 1 ? `[${rightKeys.map(c=>`"${c}"`).join(", ")}]` : `"${rightKeys[0]}"`;
        lines.push(L(`${v} = ${pVars[0]}.merge(${pVars[1]}, how="${how}", left_on=${leftArg}, right_on=${rightArg})`));
        lines.push("");
        break;
      }
      case "sink.csv": {
        lines.push(L(`# Write to CSV`));
        if (pVars.length !== 1) { lines.push(L(`# WARN: sink expects 1 input`)); lines.push(""); break; }
        lines.push(L(`${pVars[0]}.to_csv(r"${n.data.path}", index=False)`));
        lines.push(L(`# wrote: ${n.data.path}`));
        lines.push("");
        break;
      }
      default:
        lines.push(L(`# TODO: ${n.data.type}`), L(""));
    }
  }
  if (order.length) {
    const last = friendlyVarNames(order, id2node)[order[order.length - 1]];
    lines.push(L(`# Final result`));
    lines.push(L(`result = ${last}`));
    lines.push(L(""));
  }
  return lines.join("\n");
}

/* ---- Readable CTE names for SQL ---- */
function friendlyCteNames(order, id2node) {
  const counts = {};
  const names = {};
  for (const id of order) {
    const base = slugifyLabel(id2node[id]?.data?.label || "node");
    counts[base] = (counts[base] || 0) + 1;
    names[id] = counts[base] === 1 ? base : `${base}_${counts[base]}`;
  }
  return names;
}

function genSQL(nodes, edges, duckdbFlavor = true, finalizeId = null, sourceOverrides = {}) {
  const id2node = Object.fromEntries(nodes.map((n) => [n.id, n]));
  const parents = Object.fromEntries(nodes.map((n) => [n.id, []]));
  edges.forEach((e) => parents[e.target].push(e.source));
  const order = topoSort(nodes, edges);

  const cteName = friendlyCteNames(order, id2node);
  const ctes = [];

  for (const id of order) {
    const n = id2node[id];
    const p = parents[id].map(pid => cteName[pid]);
    const alias = cteName[id];

    switch (n.data.type) {
      case "source.csv": {
        const override = sourceOverrides[id];
        if (override) { ctes.push(`${alias} AS (${override})`); break; }
        const src = duckdbFlavor
          ? `SELECT * FROM read_csv_auto('${n.data.path}', header=true)`
          : `/* TODO */ SELECT * FROM '${n.data.path}'`;
        ctes.push(`${alias} AS (${src})`);
        break;
      }
      case "transform.select": {
        const colsStr = (n.data.columns || "*").trim();
        const selectList = (!colsStr || colsStr === "*")
          ? "*"
          : colsStr.split(",").map(s=>s.trim()).filter(Boolean).map(c=>quoteIdent(c)).join(", ");
        ctes.push(`${alias} AS (SELECT ${selectList} FROM ${p[0] || "MISSING"})`);
        break;
      }
      case "transform.filter": {
        const raw = n.data.expr || "1=1";
        const safe = softenNumericComparisons(raw);
        ctes.push(`${alias} AS (SELECT * FROM ${p[0] || "MISSING"} WHERE ${safe})`);
        break;
      }
      case "transform.summarize": {
        const by = (n.data.groupBy || "").split(",").map(s=>s.trim()).filter(Boolean).map(quoteIdent);
        const measures = Array.isArray(n.data.measures) ? n.data.measures : [];
        const exprs = measures
          .filter(m=>m.col && m.op)
          .map(({ col, op, as }) => `${op}(${quoteIdent(col)}) as ${quoteIdent(as || `${op}_${col}`)}`);
        const selectParts = [...by, ...exprs].join(", ");
        const group = by.length ? ` GROUP BY ${by.join(", ")}` : "";
        ctes.push(`${alias} AS (SELECT ${selectParts} FROM ${p[0] || "MISSING"}${group})`);
        break;
      }
      case "transform.formula": {
        const newCol = n.data.newCol || "new_column";
        let expr = String(n.data.expr || "0");
        // Heuristic quoting: wrap tokens that look like identifiers (esp. with spaces) in quotes unless already quoted/numeric.
        expr = expr.replace(/(?<!")([A-Za-z0-9_][A-Za-z0-9 _]*[A-Za-z0-9_])(?!")/g, (m) => {
          if (/^[0-9]+(\.[0-9]+)?$/.test(m)) return m;
          if (/\b(CASE|WHEN|THEN|END|AND|OR|NOT|NULL|TRUE|FALSE|LIKE|IN)\b/i.test(m)) return m;
          return quoteIdent(m.trim());
        });
        ctes.push(`${alias} AS (SELECT *, (${expr}) AS ${quoteIdent(newCol)} FROM ${p[0] || "MISSING"})`);
        break;
      }
      case "transform.sort": {
        const spec = (n.data.sortSpec || "").trim();
        const orderBy = !spec ? "" :
          " ORDER BY " + spec.split(",").map(s => s.trim()).filter(Boolean).map(part => {
            const [col] = part.split(/\s+/);
            const dir = /desc$/i.test(part) ? "DESC" : "ASC";
            return `${quoteIdent(col)} ${dir}`;
          }).join(", ");
        ctes.push(`${alias} AS (SELECT * FROM ${p[0] || "MISSING"}${orderBy})`);
        break;
      }
      case "transform.sample": {
        const mode = n.data.mode || "rows";
        if (mode === "fraction") {
          const frac = Number(n.data.frac || 0.1);
          ctes.push(`${alias} AS (SELECT * FROM ${p[0] || "MISSING"} WHERE random() < ${isFinite(frac)?frac:0.1})`);
        } else {
          const nrows = Math.max(0, parseInt(n.data.n || 100, 10) || 0);
          ctes.push(`${alias} AS (SELECT * FROM ${p[0] || "MISSING"} USING SAMPLE ${nrows} ROWS)`);
        }
        break;
      }
      case "transform.join": {
        const how = (n.data.how || "INNER").toUpperCase();
        const leftKeys  = (n.data.left_on  || "id").split(",").map(s=>s.trim()).filter(Boolean).map(quoteIdent);
        const rightKeys = (n.data.right_on || "id").split(",").map(s=>s.trim()).filter(Boolean).map(quoteIdent);

        const pick    = (n.data.dedupePick || "first").toLowerCase();
        const ordCol  = (n.data.dedupeOrderCol || "").trim();
        const rnOrder = ordCol ? `${quoteIdent(ordCol)} ${pick === "first" ? "ASC" : "DESC"}` : "1";

        const leftPart = n.data.dedupeLeft
          ? `(SELECT * FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY ${leftKeys.join(", ")} ORDER BY ${rnOrder}) rn FROM ${p[0] || "MISSING"}) L WHERE rn=1)`
          : `${p[0] || "MISSING"}`;

        const rightPart = n.data.dedupeRight
          ? `(SELECT * FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY ${rightKeys.join(", ")} ORDER BY ${rnOrder}) rn FROM ${p[1] || "MISSING"}) R WHERE rn=1)`
          : `${p[1] || "MISSING"}`;

        const onExpr = leftKeys.map((lk,i)=>`${lk} = ${rightKeys[i] || rightKeys[0]}`).join(" AND ");
        ctes.push(
          `${alias} AS (
            SELECT *
            FROM ${leftPart}
            ${how} JOIN ${rightPart}
              ON ${onExpr}
          )`
        );
        break;
      }
      case "inspect.deepdive": {
        ctes.push(`${alias} AS (SELECT * FROM ${p[0] || "MISSING"})`);
        break;
      }
      case "sink.csv": {
        ctes.push(`${alias} AS (SELECT * FROM ${p[0] || "MISSING"})`);
        break;
      }
      default:
        ctes.push(`${alias} AS (SELECT /* TODO */ * FROM ${p[0] || "MISSING"})`);
    }
  }

  const lastId = finalizeId || order[order.length - 1];
  const last = cteName[lastId] || "final";
  return [
    "-- Auto-generated by ETL Studio (SQL, DuckDB-safe)",
    `WITH\n  ${ctes.join(",\n  ")}`,
    `SELECT * FROM ${last};`,
  ].join("\n");
}

/* ----------------------------------------------------------
   Validation
---------------------------------------------------------- */
function validateWorkflow(nodes, edges) {
  const incoming = Object.fromEntries(nodes.map(n => [n.id, 0]));
  edges.forEach(e => { incoming[e.target] = (incoming[e.target] || 0) + 1; });

  const errs = [];
  for (const n of nodes) {
    const inputs = incoming[n.id] || 0;
    const t = n.data?.type;

    if (t === "source.csv") {
      if (!n.data?._fileObj && !n.data?._fileText) errs.push(`CSV Source "${n.data?.label}" needs an uploaded file (use Upload).`);
      if (inputs !== 0) errs.push(`CSV Source "${n.data?.label}" should not have inputs.`);
    }
    if (t === "transform.select"     && inputs !== 1) errs.push(`Select "${n.data?.label}" must have exactly 1 input.`);
    if (t === "transform.filter"     && inputs !== 1) errs.push(`Filter "${n.data?.label}" must have exactly 1 input.`);
    if (t === "transform.summarize"  && inputs !== 1) errs.push(`Summarize "${n.data?.label}" must have exactly 1 input.`);
    if (t === "transform.formula"    && inputs !== 1) errs.push(`Formula "${n.data?.label}" must have exactly 1 input.`);
    if (t === "transform.sort"       && inputs !== 1) errs.push(`Sort "${n.data?.label}" must have exactly 1 input.`);
    if (t === "transform.sample"     && inputs !== 1) errs.push(`Sample "${n.data?.label}" must have exactly 1 input.`);
    if (t === "transform.join"       && inputs !== 2) errs.push(`Join "${n.data?.label}" must have exactly 2 inputs.`);
    if (t === "sink.csv"             && inputs !== 1) errs.push(`CSV Sink "${n.data?.label}" must have exactly 1 input.`);
  }
  if (!nodes.some(n => n.data?.type?.startsWith("source.")))
    errs.push("Add at least one Source node.");

  return errs;
}

/* ==========================================================
   MAIN COMPONENT
========================================================== */
export default function ETLStudio() {
  const [nodes, setNodes] = useState([]);
  const [edges, setEdges] = useState([]);

  const [lang, setLang] = useState("python");
  const [selectedNodeId, setSelectedNodeId] = useState(null);
  const [runTargetId, setRunTargetId] = useState(null);

  const [execEngine, setExecEngine] = useState("js");
  const [isRunning, setIsRunning] = useState(false);
  const [runError, setRunError] = useState("");
  const [runRows, setRunRows] = useState([]);
  const [rowCount, setRowCount] = useState(0);

  // Copilot context (fed from the last Run/Preview)
  const [aiSampleRows, setAiSampleRows] = useState([]);
  const [aiSchema, setAiSchema] = useState(null);

  // --- Pane widths (resizable) ---
  const [canvasW, setCanvasW] = useState(0.5);
  const [inspectorW, setInspectorW] = useState(0.25);
  const [resultsW, setResultsW] = useState(0.25);
  const draggingRef = useRef(null);

  /* ---------- Persist (restore + autosave) ---------- */
  useEffect(() => {
    const saved = localStorage.getItem("etl_studio_state_v21");
    if (saved) {
      try {
        const obj = JSON.parse(saved);
        setNodes(obj.nodes || []);
        setEdges(obj.edges || []);
        setLang(obj.lang || "python");
        setExecEngine(obj.execEngine || "js");
        if (obj.layout && typeof obj.layout.canvasW === "number") {
          setCanvasW(obj.layout.canvasW);
          setInspectorW(obj.layout.inspectorW);
          setResultsW(obj.layout.resultsW);
        }
      } catch {}
    }
  }, []);
  useEffect(() => {
    const state = {
      nodes, edges, lang, execEngine,
      layout: { canvasW, inspectorW, resultsW }
    };
    localStorage.setItem("etl_studio_state_v21", JSON.stringify(state));
  }, [nodes, edges, lang, execEngine, canvasW, inspectorW, resultsW]);
  useEffect(() => {
    const t = setInterval(() => {
      const state = {
        nodes, edges, lang, execEngine,
        layout: { canvasW, inspectorW, resultsW }
      };
      localStorage.setItem("etl_studio_state_v21", JSON.stringify(state));
    }, 8000);
    return () => clearInterval(t);
  }, [nodes, edges, lang, execEngine, canvasW, inspectorW, resultsW]);

  /* ---------- Flow handlers ---------- */
  const onNodesChange = useCallback((changes) => setNodes((nds) => applyNodeChanges(changes, nds)), []);
  const onEdgesChange = useCallback((changes) => setEdges((eds) => applyEdgeChanges(changes, eds)), []);
  const onConnect = useCallback((params) =>
    setEdges((eds) => addEdge({ ...params, type: "smoothstep" }, eds)), []);

  /* ---------- Live code ---------- */
  const code = useMemo(() => (lang === "python" ? genPython(nodes, edges) : genSQL(nodes, edges)), [nodes, edges, lang]);

  /* ---------- Selection helpers ---------- */
  const selected = useMemo(() => nodes.find((n) => n.id === selectedNodeId) || null, [nodes, selectedNodeId]);
  const updateSelected = useCallback(
    (patch) => setNodes((nds) => nds.map((n) => (n.id === selectedNodeId ? { ...n, data: { ...n.data, ...patch } } : n))),
    [selectedNodeId]
  );

  /* ---------- Palette ---------- */
  const addNodeFromTpl = (tpl) => {
    const xMax = nodes.length ? Math.max(...nodes.map(n => n.position.x)) : 80;
    const y = 120 + (nodes.length % 4) * 110;
    setNodes((nds) => [...nds, newNode(tpl, { x: xMax + 240, y })]);
  };
  const removeSelected = () => {
    if (!selectedNodeId) return;
    setNodes((nds) => nds.filter((n) => n.id !== selectedNodeId));
    setEdges((eds) => eds.filter((e) => e.source !== selectedNodeId && e.target !== selectedNodeId));
    setSelectedNodeId(null);
    if (runTargetId === selectedNodeId) setRunTargetId(null);
  };
  const layoutHorizontal = () => {
    const order = topoSort(nodes, edges);
    const x0 = 80, y0 = 180, dx = 240;
    setNodes((nds) => nds.map((n) => ({
      ...n,
      position: { x: x0 + dx * order.indexOf(n.id), y: y0 },
      sourcePosition: "right",
      targetPosition: "left",
    })));
  };
  const downloadJSON = () => {
    const blob = new Blob([JSON.stringify({ nodes, edges, lang, execEngine }, null, 2)], { type: "application/json" });
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url; a.download = `etl_studio_${Date.now()}.json`; a.click();
    URL.revokeObjectURL(url);
  };
  const uploadJSON = (file) => {
    const reader = new FileReader();
    reader.onload = () => {
      try {
        const obj = JSON.parse(reader.result);
        setNodes((obj.nodes || []).map(n => ({
          ...n,
          sourcePosition: n.sourcePosition || "right",
          targetPosition: n.targetPosition || "left",
        })));
        setEdges(obj.edges || []);
        setLang(obj.lang || "python");
        setExecEngine(obj.execEngine || "js");
      } catch (e) { alert("Invalid file"); }
    };
    reader.readAsText(file);
  };

  /* ---------- Copilot: prompt -> workflow (stub) ---------- */
  const applyWorkflowFromPrompt = (prompt) => {
    const p = (prompt || "").toLowerCase();
    const wantFilter = /\b(filter|where)\b/.test(p);
    const wantGroup = /\b(group|summar|aggregate|sum|avg|average|count)\b/.test(p);

    const tplByType = (type) => NODE_TEMPLATES.find((t) => t.type === type);

    // Reuse an existing CSV source if present (so uploaded file isn't lost)
    const existingSource = nodes.find((n) => n.data?.type === "source.csv");
    const sourceNode = existingSource || newNode(tplByType("source.csv"), { x: 80, y: 180 });

    const newNodes = [];
    const newEdges = [];

    // If we are reusing the existing source, keep it in the canvas and don't duplicate.
    const baseX = sourceNode.position?.x ?? 80;
    const baseY = sourceNode.position?.y ?? 180;
    if (!existingSource) newNodes.push(sourceNode);

    let prevId = sourceNode.id;
    let idx = 1;

    const firstStringCol = aiSchema?.columns?.find((c) => c.type === "string")?.name || aiSchema?.columns?.[0]?.name;
    const firstNumCol = aiSchema?.columns?.find((c) => c.type === "number")?.name || aiSchema?.columns?.[0]?.name;

    if (wantFilter) {
      const f = newNode(tplByType("transform.filter"), { x: baseX + 240 * idx, y: baseY });
      // Gentle default using known columns if available
      if (firstNumCol) f.data.expr = `${firstNumCol} > 0`;
      newNodes.push(f);
      newEdges.push({ id: nanoid(8), source: prevId, target: f.id, type: "smoothstep" });
      prevId = f.id;
      idx += 1;
    }

    if (wantGroup) {
      const s = newNode(tplByType("transform.summarize"), { x: baseX + 240 * idx, y: baseY });
      if (firstStringCol) s.data.groupBy = firstStringCol;
      if (firstNumCol) s.data.measures = [{ col: firstNumCol, op: "sum", as: firstNumCol }];
      newNodes.push(s);
      newEdges.push({ id: nanoid(8), source: prevId, target: s.id, type: "smoothstep" });
      prevId = s.id;
      idx += 1;
    }

    const sink = newNode(tplByType("sink.csv"), { x: baseX + 240 * idx, y: baseY });
    newNodes.push(sink);
    newEdges.push({ id: nanoid(8), source: prevId, target: sink.id, type: "smoothstep" });

    // Apply: merge into canvas (keep existing nodes if source reused)
    setNodes((prev) => {
      const filtered = existingSource ? prev : prev;
      // If source was reused, we only add new nodes (excluding source)
      return [...filtered, ...newNodes.filter((n) => !existingSource || n.id !== sourceNode.id)];
    });
    setEdges((prev) => [...prev, ...newEdges]);

    // Select the last node for convenience
    setSelectedNodeId(sink.id);
  };

  /* ==========================================================
     EXECUTION ENGINE (JS)
  ========================================================== */
  async function executeJS(targetId = null) {
    const { nodes: n2, edges: e2 } = subgraphUpTo(targetId || null, nodes, edges);

    const problems = validateWorkflow(n2, e2);
    if (problems.length) throw new Error(problems[0]);

    const parents = Object.fromEntries(n2.map(n => [n.id, []]));
    e2.forEach(e => parents[e.target].push(e.source));
    const order = topoSort(n2, e2);

    const out = {};
    const getRows = (id) => out[id] || [];

    const toNumberAny = (v) => {
      if (v == null) return NaN;
      const s = String(v)
        .trim()
        .replace(/[₹$£€¥₩]/g, "")
        .replace(/,/g, "")
        .replace(/\s+/g, "")
        .replace(/%$/, "");
      const num = Number(s);
      return Number.isFinite(num) ? num : NaN;
    };

    for (const id of order) {
      const n = n2.find(x => x.id === id);
      const t = n.data.type;

      if (t === "source.csv") {
        let text;
        if (typeof n.data._fileText === "string") {
          text = n.data._fileText;
        } else if (n.data._fileObj && typeof n.data._fileObj.text === "function") {
          text = await n.data._fileObj.text();
        } else {
          throw new Error(`CSV Source "${n.data.label}" has no loaded file content. Please upload again.`);
        }
        const parsed = Papa.parse(text, { header: true, dynamicTyping: false, skipEmptyLines: true });
        if (parsed.errors?.length) throw new Error(`CSV parse error: ${parsed.errors[0].message}`);
        out[id] = parsed.data;
        continue;
      }

      if (t === "transform.select") {
        const src = getRows(parents[id][0]);

        const colsStr = (n.data.columns || "*").trim();
        let rows = src;
        if (colsStr !== "*") {
          const cols = colsStr.split(",").map(s => s.trim()).filter(Boolean);
          rows = src.map(row => {
            const r = {};
            for (const c of cols) r[c] = row[c];
            return r;
          });
        }

        const schema = Array.isArray(n.data.schema) ? n.data.schema : [];
        if (schema.length) {
          const typeMap = Object.fromEntries(
            schema.filter(s => s.name && s.dtype).map(s => [String(s.name).trim(), s.dtype])
          );
          const castValue = (v, dtype) => {
            if (v == null || v === "") return v;
            switch (dtype) {
              case "integer": { const num = toNumberAny(v); return Number.isFinite(num) ? Math.trunc(num) : null; }
              case "float":   { const num = toNumberAny(v); return Number.isFinite(num) ? num : null; }
              case "boolean": {
                if (typeof v === "boolean") return v;
                const s = String(v).trim().toLowerCase();
                if (["true","t","1","yes","y"].includes(s)) return true;
                if (["false","f","0","no","n"].includes(s)) return false;
                return null;
              }
              case "date":    { const d = new Date(v); return isNaN(d.getTime()) ? null : d.toISOString().slice(0,10); }
              case "datetime":{ const d = new Date(v); return isNaN(d.getTime()) ? null : d.toISOString(); }
              case "string":
              default:        return typeof v === "string" ? v.trim() : String(v);
            }
          };
          rows = rows.map(r => {
            const rr = { ...r };
            for (const [col, dt] of Object.entries(typeMap)) {
              const key = col in rr ? col : Object.keys(rr).find(k => k.trim() === col);
              if (key) rr[key] = castValue(rr[key], dt);
            }
            return rr;
          });
        }
        out[id] = rows;
        continue;
      }

      if (t === "transform.filter") {
        const src = getRows(parents[id][0]);
        const expr = (n.data.expr || "").trim();
        if (!expr) { out[id] = src; continue; }

        const fn = new Function("row", `with (row) { return (${expr}); }`);
        const filtered = [];
        for (const r of src) {
          const evalRow = {};
          for (const [k, v] of Object.entries(r)) {
            if (typeof v === "number") evalRow[k] = v;
            else if (typeof v === "string") {
              const nnum = toNumberAny(v);
              evalRow[k] = Number.isFinite(nnum) ? nnum : v;
            } else evalRow[k] = v;
          }
          try { if (fn(evalRow)) filtered.push(r); } catch {}
        }
        out[id] = filtered;
        continue;
      }

      if (t === "transform.summarize") {
        const src = getRows(parents[id][0]);
        const by = (n.data.groupBy || "").split(",").map(s=>s.trim()).filter(Boolean);

        const measures = Array.isArray(n.data.measures) ? n.data.measures.filter(m=>m.col && m.op) : [];
        const gmap = new Map();
        for (const row of src) {
          const key = JSON.stringify(by.map(c => row[c]));
          if (!gmap.has(key)) gmap.set(key, []);
          gmap.get(key).push(row);
        }
        const res = [];
        for (const [k, rows] of gmap.entries()) {
          const obj = {};
          by.forEach((c, i) => (obj[c] = JSON.parse(k)[i]));
          for (const m of measures) {
            const { col, op, as } = m;
            const name = as || `${op}_${col}`;
            const vals = rows.map(r => r[col]).filter(v => v != null);
            let v = null;
            switch (op) {
              case "sum":  v = vals.map(Number).filter(Number.isFinite).reduce((a,x)=>a+x,0); break;
              case "avg":
              case "mean": { const nums = vals.map(Number).filter(Number.isFinite); v = nums.length ? nums.reduce((a,x)=>a+x,0)/nums.length : null; break; }
              case "min":  v = vals.length ? Math.min(...vals.map(Number)) : null; break;
              case "max":  v = vals.length ? Math.max(...vals.map(Number)) : null; break;
              case "count": v = rows.length; break;
              case "first": v = vals.length ? vals[0] : null; break;
              case "last":  v = vals.length ? vals[vals.length-1] : null; break;
              default: v = null;
            }
            obj[name] = v;
          }
          res.push(obj);
        }
        out[id] = res;
        continue;
      }

      // NEW: FORMULA
      if (t === "transform.formula") {
        const src = getRows(parents[id][0]) || [];
        const newCol = (n.data.newCol || "new_column").trim() || "new_column";
        let expr = String(n.data.expr || "0");

        const columns = src.length ? Object.keys(src[0]) : [];
        expr = rewriteExprWithColumns(expr, columns, false);

        const buildContext = (row) => {
          const n = (k) => {
            const v = row[k];
            const num = typeof v === "number" ? v : (Number(String(v).replace(/[, ]/g, "")));
            return Number.isFinite(num) ? num : 0;
          };
          const s = (k) => (row[k] == null ? "" : String(row[k]));
          const b = (k) => {
            const v = row[k];
            if (typeof v === "boolean") return v;
            const sv = String(v).trim().toLowerCase();
            if (["true","t","1","yes","y"].includes(sv)) return true;
            if (["false","f","0","no","n"].includes(sv)) return false;
            return Boolean(v);
          };
          const col = (k) => row[k];
          return { n, s, b, col, Math };
        };
        const fn = new Function("ctx", `with (ctx) { return (${expr}); }`);

        const outRows = [];
        for (const r of src) {
          let val = null;
          try { val = fn(buildContext(r)); } catch { val = null; }
          outRows.push({ ...r, [newCol]: val });
        }
        out[id] = outRows;
        continue;
      }

      // NEW: SORT
      if (t === "transform.sort") {
        const src = getRows(parents[id][0]) || [];
        const spec = (n.data.sortSpec || "").trim();
        if (!spec) { out[id] = [...src]; continue; }
        const pieces = spec.split(",").map(s=>s.trim()).filter(Boolean).map(p => {
          const [c] = p.split(/\s+/);
          return { col: c, asc: !/desc$/i.test(p) };
        });
        const rows = [...src].sort((a,b) => {
          for (const { col, asc } of pieces) {
            const av = a[col], bv = b[col];
            if (av == null && bv == null) continue;
            if (av == null) return asc ? -1 : 1;
            if (bv == null) return asc ? 1 : -1;
            if (av < bv) return asc ? -1 : 1;
            if (av > bv) return asc ? 1 : -1;
          }
          return 0;
        });
        out[id] = rows;
        continue;
      }

      // NEW: SAMPLE
      if (t === "transform.sample") {
        const src = getRows(parents[id][0]) || [];
        const mode = n.data.mode || "rows";
        let rows = [];
        if (mode === "fraction") {
          const f = Number(n.data.frac || 0.1);
          const p = isFinite(f) ? Math.max(0, Math.min(1, f)) : 0.1;
          rows = src.filter(() => Math.random() < p);
        } else {
          const N = Math.max(0, parseInt(n.data.n || 100, 10) || 0);
          rows = [...src].sort(() => Math.random() - 0.5).slice(0, N);
        }
        out[id] = rows;
        continue;
      }

      // JOIN (kept)
      if (t === "transform.join") {
        const left = getRows(parents[id][0]);
        const right = getRows(parents[id][1]);
        const how = (n.data.how || "inner").toLowerCase();

        const leftKeys  = (n.data.left_on  || "id").split(",").map(s=>s.trim()).filter(Boolean);
        const rightKeys = (n.data.right_on || "id").split(",").map(s=>s.trim()).filter(Boolean);

        const norm = (v) => (typeof v === "string" ? v.trim() : v);
        const keyFn = (row, keys) => JSON.stringify(keys.map(k => norm(row[k])));

        const dedupeLeft  = !!n.data.dedupeLeft;
        const dedupeRight = !!n.data.dedupeRight;
        const dedupePick  = (n.data.dedupePick || "first");
        const orderBy     = (n.data.dedupeOrderCol || "").trim();

        const dedupeSide = (rows, keys) => {
          if (!rows?.length || !keys.length) return rows || [];
          let arr = [...rows];
          if (orderBy) {
            arr.sort((a,b) => {
              const va = a[orderBy], vb = b[orderBy];
              if (va === vb) return 0;
              return (va > vb ? 1 : -1) * (dedupePick === "first" ? 1 : -1);
            });
          }
          const seen = new Set(), outRows = [];
          for (const r of arr) {
            const k = keyFn(r, keys);
            if (!seen.has(k)) { seen.add(k); outRows.push(r); }
          }
          return outRows;
        };

        const L = dedupeLeft  ? dedupeSide(left,  leftKeys)  : left;
        const R = dedupeRight ? dedupeSide(right, rightKeys) : right;

        const rIndex = new Map();
        for (const r of R) {
          const k = keyFn(r, rightKeys);
          if (!rIndex.has(k)) rIndex.set(k, []);
          rIndex.get(k).push(r);
        }

        const res = [];
        const rKeysAll = new Set(rIndex.keys());
        const lKeysSeen = new Set();

        for (const l of L) {
          const lk = keyFn(l, leftKeys);
          lKeysSeen.add(lk);
          const matches = rIndex.get(lk) || [];
          if (matches.length) {
            for (const m of matches) res.push({ ...l, ...m });
          } else if (how === "left" || how === "outer") {
            res.push({ ...l });
          }
        }
        if (how === "right" || how === "outer") {
          for (const rk of rKeysAll) {
            if (!lKeysSeen.has(rk)) {
              const rrows = rIndex.get(rk) || [];
              if (rrows.length) res.push({ ...rrows[0] });
            }
          }
        }
        out[id] = res;
        continue;
      }

      if (t === "inspect.deepdive") {
        out[id] = getRows(parents[id][0]);
        continue;
      }

      if (t === "sink.csv") {
        const src = getRows(parents[id][0]);
        const path = (n.data.path || "out.csv").trim();
        if (!Array.isArray(src) || src.length === 0) { out[id] = []; continue; }

        const headers = Object.keys(src[0]);
        const csvLines = [headers.join(",")];
        for (const r of src) {
          const row = headers.map((h) => {
            let v = r[h];
            if (v == null) return "";
            v = String(v).replace(/"/g, '""');
            if (v.includes(",") || v.includes("\n") || v.includes('"')) v = `"${v}"`;
            return v;
          });
          csvLines.push(row.join(","));
        }
        const csvContent = csvLines.join("\n");
        const blob = new Blob([csvContent], { type: "text/csv" });
        const url = URL.createObjectURL(blob);
        const a = document.createElement("a");
        a.href = url; a.download = path; a.click();
        URL.revokeObjectURL(url);
        out[id] = src;
        continue;
      }

      throw new Error(`Unsupported node type: ${t}`);
    }

    const finalId = targetId || order[order.length - 1];
    return out[finalId] || [];
  }

  /* ---------- Run dispatcher ---------- */
  const inferSchemaFromRows = (rows) => {
    if (!Array.isArray(rows) || rows.length === 0) return null;
    const first = rows[0] || {};
    const cols = Object.keys(first).map((name) => {
      // crude type inference from sample
      let type = "string";
      for (let i = 0; i < Math.min(rows.length, 50); i++) {
        const v = rows[i]?.[name];
        if (v === null || v === undefined || v === "") continue;
        const num = Number(v);
        if (!Number.isNaN(num) && Number.isFinite(num)) {
          type = "number";
        }
        break;
      }
      return { name, type };
    });
    return { columns: cols };
  };

  const runWorkflow = async (targetId = null) => {
    setRunError(""); setRunRows([]); setRowCount(0); setIsRunning(true);
    try {
      if (execEngine !== "js") {
        throw new Error("Use JavaScript (no deps) engine for now.");
      }
      const rows = await executeJS(targetId);
      setRunRows(rows.slice(0, 200));
      setRowCount(rows.length);

      const sample = rows.slice(0, 200);
      setAiSampleRows(sample);
      setAiSchema(inferSchemaFromRows(sample));
    } catch (e) {
      setRunError(String(e?.message || e));
    } finally {
      setIsRunning(false);
    }
  };

  const previewSelectedNode = () => {
    if (!selectedNodeId) return;
    setRunTargetId(selectedNodeId);
    runWorkflow(selectedNodeId);
  };
  const clearPreviewTarget = () => setRunTargetId(null);

  /* ---------- Resizers ---------- */
  const onStartDrag = (which, e) => {
    e.preventDefault();
    draggingRef.current = {
      which,
      startX: e.clientX,
      startWidths: { canvasW, inspectorW, resultsW },
    };
    window.addEventListener("mousemove", onDrag);
    window.addEventListener("mouseup", onStopDrag);
  };
  const onDrag = (e) => {
    const d = draggingRef.current;
    if (!d) return;
    const dx = e.clientX - d.startX;
    const totalPx = document.body.clientWidth;
    const frac = dx / Math.max(totalPx, 1);

    if (d.which === "left") {
      let newCanvas = d.startWidths.canvasW + frac;
      let newInspector = d.startWidths.inspectorW - frac;
      const min = 0.15, max = 0.75;
      newCanvas = Math.min(max, Math.max(min, newCanvas));
      newInspector = Math.min(0.6, Math.max(0.15, newInspector));
      const right = 1 - (newCanvas + newInspector);
      setCanvasW(newCanvas);
      setInspectorW(newInspector);
      setResultsW(Math.max(0.15, right));
    } else if (d.which === "right") {
      let newInspector = d.startWidths.inspectorW + frac;
      let newResults = d.startWidths.resultsW - frac;
      newInspector = Math.min(0.6, Math.max(0.15, newInspector));
      newResults = Math.min(0.6, Math.max(0.15, newResults));
      const left = 1 - (newInspector + newResults);
      setInspectorW(newInspector);
      setResultsW(newResults);
      setCanvasW(Math.max(0.15, left));
    }
  };
  const onStopDrag = () => {
    window.removeEventListener("mousemove", onDrag);
    window.removeEventListener("mouseup", onStopDrag);
    draggingRef.current = null;
  };

  /* ==========================================================
     UI
  ========================================================== */
  return (
    <div className="w-full h-screen flex flex-col bg-gray-50">
      {/* Toolbar */}
      <div className="flex items-center gap-2 p-2 border-b bg-white">
        {NODE_TEMPLATES.map((tpl) => (
          <Button
            key={tpl.type}
            className="text-sm"
            style={{ borderColor: tpl.color, color: tpl.color }}
            onClick={() => addNodeFromTpl(tpl)}
          >
            + {tpl.label}
          </Button>
        ))}
        <div className="flex-1" />
        <SelectBox value={lang} onChange={setLang}>
          <option value="python">Python (pandas)</option>
          <option value="sql">SQL (DuckDB)</option>
        </SelectBox>
        <SelectBox value={execEngine} onChange={setExecEngine}>
          <option value="js">JavaScript (no deps)</option>
          <option value="duckdb">SQL (DuckDB WASM)</option>
        </SelectBox>
        <Button onClick={layoutHorizontal}><AlignHorizontalDistributeCenter size={16} /></Button>
        <Button onClick={downloadJSON}><Download size={16} /></Button>
        <label className="px-3 py-2 rounded-2xl shadow-sm border border-gray-200 bg-gray-100 cursor-pointer">
          <Upload size={16} />
          <input type="file" hidden onChange={(e) => uploadJSON(e.target.files[0])} />
        </label>
      </div>

      {/* Main area: Copilot | Canvas | Inspector | Results/Code */}
      <div className="flex-1 overflow-hidden" style={{ display: "flex", width: "100%" }}>
        <AICopilotPanel
          width={400}
          sampleRows={aiSampleRows}
          schema={aiSchema}
          onApplyWorkflow={applyWorkflowFromPrompt}
        />

        {/* Center: Canvas | Inspector | Results/Code (RESIZABLE) */}
        <div className="flex-1 relative overflow-hidden" style={{ display: "flex", width: "100%" }}>
          {/* Canvas */}
          <div style={{ width: `${canvasW * 100}%`, minWidth: 200 }} className="relative">
          <ReactFlow
            nodes={nodes.map((n) => ({
              ...n,
              sourcePosition: n.sourcePosition || "right",
              targetPosition: n.targetPosition || "left",
              data: { ...n.data, label: <NodeCard data={n.data} /> },
            }))}
            edges={edges}
            onNodesChange={onNodesChange}
            onEdgesChange={onEdgesChange}
            onConnect={onConnect}
            fitView
            onNodeClick={(_, node) => setSelectedNodeId(node.id)}
          >
            {/* Removed MiniMap */}
            <Controls />
            {/* Non-dotted background */}
            <Background variant="lines" gap={24} lineWidth={1} />
          </ReactFlow>

          {/* Left resizer */}
          <div
            onMouseDown={(e) => onStartDrag("left", e)}
            className="absolute top-0 right-0 h-full w-2 cursor-col-resize bg-gray-200/60 hover:bg-gray-300"
            title="Drag to resize"
          />
        </div>

          {/* Inspector */}
          <div style={{ width: `${inspectorW * 100}%`, minWidth: 240 }} className="flex flex-col border-l bg-white overflow-hidden relative">
          {selected ? (
            <div className="flex flex-col h-full">
              <div className="flex items-center justify-between px-3 py-2 border-b">
                <div className="font-medium text-sm">{selected.data.label}</div>
                <Button className="text-red-600 border-red-200" onClick={removeSelected}>
                  <Trash2 size={14} />
                </Button>
              </div>
              <div className="flex-1 overflow-y-auto p-3">
                <NodeInspectorImpl data={selected.data} onChange={updateSelected} />
              </div>
              <div className="border-t p-2 flex items-center gap-2">
                <Button className="flex-1 text-blue-600 border-blue-200" onClick={previewSelectedNode} disabled={isRunning}>
                  <Eye size={14} /> Preview this node
                </Button>
                <Button onClick={clearPreviewTarget}>Clear</Button>
              </div>
            </div>
          ) : (
            <div className="p-4 text-sm text-gray-500">Select a node to edit its settings.</div>
          )}

          {/* Right resizer */}
          <div
            onMouseDown={(e) => onStartDrag("right", e)}
            className="absolute top-0 right-0 h-full w-2 cursor-col-resize bg-gray-200/60 hover:bg-gray-300"
            title="Drag to resize"
          />
        </div>

          {/* Results + Code */}
          <div style={{ width: `${resultsW * 100}%`, minWidth: 260 }} className="flex flex-col border-l bg-[#fafafa]">
          <div className="flex items-center justify-between px-3 py-2 border-b bg-white">
            <div className="flex items-center gap-2">
              {isRunning ? (
                <Button className="bg-red-100 text-red-700 border-red-300" onClick={() => setIsRunning(false)}>
                  <Square size={14} /> Stop
                </Button>
              ) : (
                <Button className="bg-green-100 text-green-700 border-green-300" onClick={() => runWorkflow(runTargetId)}>
                  <Play size={14} /> Run
                </Button>
              )}
              <div className="text-sm text-gray-500">
                {runTargetId ? <>Preview up to <code>{selected?.data?.label}</code></> : "Run entire flow"}
              </div>
            </div>
            <div className="text-xs text-gray-400">{rowCount ? `Rows: ${rowCount}` : "Rows: 0"}</div>
          </div>

          {runError ? (
            <div className="text-red-600 text-sm p-3 bg-red-50 flex items-start gap-2">
              <AlertTriangle size={16} /> <span>{runError}</span>
            </div>
          ) : (
            <ResultTable rows={runRows} />
          )}

          <div className="h-[45%] border-t overflow-hidden">
            <CodePane code={code} lang={lang === "python" ? "py" : "sql"} />
          </div>
          </div>
        </div>
      </div>
    </div>
  );
}

/* ==========================================================
   Node Inspector
========================================================== */
function NodeInspectorImpl({ data, onChange }) {
  if (!data) return null;

  if (data.type === "source.csv")
    return (
      <div className="flex flex-col gap-2">
        <Label>CSV File</Label>
        <input
          type="file"
          accept=".csv"
          onChange={(e) => {
            const f = e.target.files[0];
            if (!f) return;
            const reader = new FileReader();
            reader.onload = () =>
              onChange({ _fileObj: f, _fileText: reader.result, _fileName: f.name, path: f.name });
            reader.readAsText(f);
          }}
        />
        {data._fileName && <div className="text-xs text-gray-500">Loaded: <b>{data._fileName}</b></div>}
      </div>
    );

  if (data.type === "transform.select")
    return (
      <>
        <Label>Columns (comma-separated or *)</Label>
        <TextInput value={data.columns} onChange={(v) => onChange({ columns: v })} />
        <div className="text-[11px] text-gray-500 mb-2">
          Example: <code>category,sales,units</code>. Then define types below.
        </div>

        <Label>Data Types</Label>
        <table className="min-w-full text-xs border rounded-lg overflow-hidden">
          <thead className="bg-gray-50">
            <tr>
              <th className="px-2 py-1 text-left border-b">Column</th>
              <th className="px-2 py-1 text-left border-b">Type</th>
              <th className="px-2 py-1 border-b"></th>
            </tr>
          </thead>
          <tbody>
            {(data.schema || []).map((row, idx) => (
              <tr key={idx} className={idx % 2 ? "bg-white" : "bg-gray-50/40"}>
                <td className="px-2 py-1 border-b">
                  <input
                    className="w-full border rounded-md px-1 py-[2px]"
                    value={row.name}
                    onChange={(e) => {
                      const schema = [...(data.schema || [])];
                      schema[idx] = { ...schema[idx], name: e.target.value };
                      onChange({ schema });
                    }}
                  />
                </td>
                <td className="px-2 py-1 border-b">
                  <select
                    className="w-full border rounded-md px-1 py-[2px]"
                    value={row.dtype}
                    onChange={(e) => {
                      const schema = [...(data.schema || [])];
                      schema[idx] = { ...schema[idx], dtype: e.target.value };
                      onChange({ schema });
                    }}
                  >
                    <option value="string">string</option>
                    <option value="integer">integer</option>
                    <option value="float">float</option>
                    <option value="boolean">boolean</option>
                    <option value="date">date</option>
                    <option value="datetime">datetime</option>
                  </select>
                </td>
                <td className="px-2 py-1 border-b text-right">
                  <Button
                    className="text-red-700 border-red-200"
                    onClick={() => {
                      const schema = (data.schema || []).filter((_, i) => i !== idx);
                      onChange({ schema });
                    }}
                  >
                    Remove
                  </Button>
                </td>
              </tr>
            ))}
            <tr>
              <td colSpan={3} className="px-2 py-2">
                <Button
                  onClick={() =>
                    onChange({
                      schema: [...(data.schema || []), { name: "", dtype: "string" }],
                    })
                  }
                >
                  + Add type rule
                </Button>
              </td>
            </tr>
          </tbody>
        </table>
      </>
    );

  if (data.type === "transform.filter")
    return (
      <div>
        <Label>Expression (e.g. sales &gt; 10000)</Label>
        <TextInput value={data.expr} onChange={(v) => onChange({ expr: v })} />
        <div className="text-[11px] text-gray-500 mt-1">
          Evaluated with <code>with(row){"{}"}</code>. Use column names directly.
        </div>
      </div>
    );

  if (data.type === "transform.summarize")
    return (
      <div className="flex flex-col gap-3">
        <div>
          <Label>Group by columns</Label>
          <TextInput value={data.groupBy} onChange={(v) => onChange({ groupBy: v })} />
          <div className="text-[11px] text-gray-500">Comma-separated (e.g. <code>category, class</code>)</div>
        </div>

        <div>
          <Label>Measures</Label>
          <div className="text-xs text-gray-500 mb-1">Pick a column and an operation; we’ll generate the expression.</div>
          <table className="min-w-full text-xs border rounded-lg overflow-hidden">
            <thead className="bg-gray-50">
              <tr>
                <th className="px-2 py-1 text-left border-b">Column</th>
                <th className="px-2 py-1 text-left border-b">Operation</th>
                <th className="px-2 py-1 text-left border-b">Alias</th>
                <th className="px-2 py-1 border-b"></th>
              </tr>
            </thead>
            <tbody>
              {(data.measures || []).map((m, idx) => (
                <tr key={idx} className={idx % 2 ? "bg-white" : "bg-gray-50/40"}>
                  <td className="px-2 py-1 border-b">
                    <input className="w-full border rounded-md px-1 py-[2px]"
                      value={m.col || ""} onChange={(e)=> {
                        const measures = [...(data.measures || [])];
                        measures[idx] = { ...measures[idx], col: e.target.value };
                        onChange({ measures });
                      }}/>
                  </td>
                  <td className="px-2 py-1 border-b">
                    <select className="w-full border rounded-md px-1 py-[2px]"
                      value={m.op || "sum"}
                      onChange={(e)=>{
                        const measures = [...(data.measures || [])];
                        measures[idx] = { ...measures[idx], op: e.target.value };
                        onChange({ measures });
                      }}>
                      <option value="sum">sum</option>
                      <option value="avg">avg</option>
                      <option value="min">min</option>
                      <option value="max">max</option>
                      <option value="count">count</option>
                      <option value="first">first</option>
                      <option value="last">last</option>
                    </select>
                  </td>
                  <td className="px-2 py-1 border-b">
                    <input className="w-full border rounded-md px-1 py-[2px]"
                      value={m.as || ""} onChange={(e)=>{
                        const measures = [...(data.measures || [])];
                        measures[idx] = { ...measures[idx], as: e.target.value };
                        onChange({ measures });
                      }}/>
                  </td>
                  <td className="px-2 py-1 border-b text-right">
                    <Button className="text-red-700 border-red-200"
                      onClick={()=>{
                        const measures = (data.measures || []).filter((_,i)=>i!==idx);
                        onChange({ measures });
                      }}>Remove</Button>
                  </td>
                </tr>
              ))}
              <tr>
                <td colSpan={4} className="px-2 py-2">
                  <Button onClick={() => onChange({ measures: [...(data.measures || []), { col: "", op: "sum", as: "" }] })}>
                    + Add measure
                  </Button>
                </td>
              </tr>
            </tbody>
          </table>

          <div className="mt-2">
            <Label>Generated expression</Label>
            <div className="text-[11px] text-gray-600 border rounded-md p-2 bg-gray-50">
              {(data.measures || [])
                .filter(m=>m.col && m.op)
                .map(({col,op,as}) => `${op}(${col}) as ${as || `${op}_${col}`}`)
                .join(", ") || "—"}
            </div>
          </div>
        </div>
      </div>
    );

  if (data.type === "transform.formula")
    return (
      <div className="flex flex-col gap-3">
        <div>
          <Label>New column name</Label>
          <TextInput value={data.newCol} onChange={(v) => onChange({ newCol: v })} />
        </div>
        <div>
          <Label>Expression</Label>
          <TextInput value={data.expr} onChange={(v) => onChange({ expr: v })} />
          <div className="text-[11px] text-gray-500 mt-1">
            JS engine: row-wise using helpers <code>n()</code>, <code>s()</code>, <code>b()</code>.<br />
            Example: <code>n("gross margin") / n(units)</code> or simply <code>gross margin/units</code> (auto-wrapped).
          </div>
        </div>
      </div>
    );

  if (data.type === "transform.sort")
    return (
      <div className="flex flex-col gap-2">
        <Label>Sort by</Label>
        <TextInput value={data.sortSpec} onChange={(v) => onChange({ sortSpec: v })} />
        <div className="text-[11px] text-gray-500">
          Comma-separated. Example: <code>category asc, units desc</code>
        </div>
      </div>
    );

  if (data.type === "transform.sample")
    return (
      <div className="flex flex-col gap-3">
        <div>
          <Label>Mode</Label>
          <SelectBox value={data.mode} onChange={(v)=>onChange({ mode: v })}>
            <option value="rows">rows</option>
            <option value="fraction">fraction</option>
          </SelectBox>
        </div>
        {data.mode === "fraction" ? (
          <div>
            <Label>Fraction (0-1)</Label>
            <TextInput value={String(data.frac)} onChange={(v)=>onChange({ frac: v })} />
          </div>
        ) : (
          <div>
            <Label>Number of rows</Label>
            <TextInput value={String(data.n)} onChange={(v)=>onChange({ n: v })} />
          </div>
        )}
      </div>
    );

  if (data.type === "transform.join")
    return (
      <div className="flex flex-col gap-3">
        <div>
          <Label>Join Type</Label>
          <SelectBox value={data.how} onChange={(v) => onChange({ how: v })}>
            <option value="inner">inner</option>
            <option value="left">left</option>
            <option value="right">right</option>
            <option value="outer">outer</option>
          </SelectBox>
        </div>
        <div>
          <Label>Left Key (comma-separated)</Label>
          <TextInput value={data.left_on} onChange={(v) => onChange({ left_on: v })} />
        </div>
        <div>
          <Label>Right Key (comma-separated)</Label>
          <TextInput value={data.right_on} onChange={(v) => onChange({ right_on: v })} />
        </div>

        <div className="border-t pt-2">
          <Label>De-duplicate before join</Label>
          <div className="flex items-center gap-3 mt-1">
            <label className="text-sm flex items-center gap-2">
              <input type="checkbox" checked={!!data.dedupeLeft}
                onChange={(e)=>onChange({ dedupeLeft: e.target.checked })}/>
              Dedupe left
            </label>
            <label className="text-sm flex items-center gap-2">
              <input type="checkbox" checked={!!data.dedupeRight}
                onChange={(e)=>onChange({ dedupeRight: e.target.checked })}/>
              Dedupe right
            </label>
          </div>
          <div className="mt-2 grid grid-cols-2 gap-2">
            <div>
              <Label>Pick row</Label>
              <SelectBox value={data.dedupePick || "first"}
                onChange={(v)=>onChange({ dedupePick: v })}>
                <option value="first">first</option>
                <option value="last">last</option>
              </SelectBox>
            </div>
            <div>
              <Label>Order by (optional)</Label>
              <TextInput value={data.dedupeOrderCol || ""}
                onChange={(v)=>onChange({ dedupeOrderCol: v })}
                placeholder="e.g. updated_at" />
            </div>
          </div>
        </div>
      </div>
    );

  if (data.type === "sink.csv")
    return (
      <div className="flex flex-col gap-2">
        <Label>Output path</Label>
        <TextInput value={data.path} onChange={(v) => onChange({ path: v })} />
      </div>
    );

  return <div className="text-gray-400 text-sm">No configurable fields.</div>;
}

/* ==========================================================
   Result table (simple, non-virtualized)
========================================================== */
function ResultTable({ rows }) {
  if (!rows?.length) return <div className="text-xs text-gray-400 p-2">No rows.</div>;
  const cols = Object.keys(rows[0]);
  return (
    <div className="overflow-auto text-xs h-[55%]">
      <table className="min-w-full border">
        <thead className="bg-gray-100 sticky top-0">
          <tr>{cols.map((c) => (<th key={c} className="px-2 py-1 border-b text-left">{c}</th>))}</tr>
        </thead>
        <tbody>
          {rows.map((r, i) => (
            <tr key={i} className={i % 2 ? "bg-gray-50" : "bg-white"}>
              {cols.map((c) => (<td key={c} className="px-2 py-1 border-b">{String(r[c] ?? "")}</td>))}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}
