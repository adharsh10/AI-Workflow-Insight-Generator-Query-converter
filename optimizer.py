# optimizer.py
from __future__ import annotations
from typing import Dict, List, Tuple, Any, Set

Node = Dict[str, Any]
Edge = Dict[str, Any]

def _topo(nodes: List[Node], edges: List[Edge]) -> List[str]:
    inc = {n["id"]: 0 for n in nodes}
    outs = {n["id"]: [] for n in nodes}
    for e in edges:
        inc[e["target"]] = inc.get(e["target"], 0) + 1
        outs[e["source"]].append(e["target"])
    q = [nid for nid in inc if inc[nid] == 0]
    out = []
    while q:
        nid = q.pop(0)
        out.append(nid)
        for t in outs.get(nid, []):
            inc[t] -= 1
            if inc[t] == 0:
                q.append(t)
    return out if len(out) == len(nodes) else [n["id"] for n in nodes]

def _parents(edges: List[Edge]) -> Dict[str, List[str]]:
    p = {}
    for e in edges:
        p.setdefault(e["target"], []).append(e["source"])
        p.setdefault(e["source"], p.get(e["source"], []))
    return p

def _children(edges: List[Edge]) -> Dict[str, List[str]]:
    c = {}
    for e in edges:
        c.setdefault(e["source"], []).append(e["target"])
        c.setdefault(e["target"], c.get(e["target"], []))
    return c

def _remove_node(nodes: Dict[str, Node], edges: List[Edge], nid: str) -> None:
    ins = [e for e in edges if e["target"] == nid]
    outs = [e for e in edges if e["source"] == nid]
    if len(ins) == 1 and len(outs) == 1:
        src = ins[0]["source"]
        dst = outs[0]["target"]
        edges[:] = [e for e in edges if e["source"] != nid and e["target"] != nid]
        edges.append({"source": src, "target": dst})
        nodes.pop(nid, None)

def prune_dead(nodes: List[Node], edges: List[Edge], target_id: str | None) -> Tuple[List[Node], List[Edge]]:
    if not target_id:
        return nodes, edges
    preds = {}
    for e in edges:
        preds.setdefault(e["target"], []).append(e["source"])
    keep: Set[str] = set()
    stack = [target_id]
    while stack:
        cur = stack.pop()
        if cur in keep:
            continue
        keep.add(cur)
        for p in preds.get(cur, []):
            stack.append(p)
    n2 = [n for n in nodes if n["id"] in keep]
    e2 = [e for e in edges if e["source"] in keep and e["target"] in keep]
    return n2, e2

def combine_redundant(nodes: List[Node], edges: List[Edge]) -> Tuple[List[Node], List[Edge]]:
    id2 = {n["id"]: n for n in nodes}
    order = _topo(nodes, edges)
    parents = _parents(edges)
    children = _children(edges)

    for nid in order:
        n = id2.get(nid)
        if not n:
            continue
        t = n.get("data", {}).get("type")

        if t == "transform.select" and len(parents.get(nid, [])) == 1:
            pin = parents[nid][0]
            pnode = id2.get(pin)
            if not pnode or pnode.get("data", {}).get("type") != "transform.select":
                continue
            cols1 = (pnode["data"].get("columns") or "*").strip()
            cols2 = (n["data"].get("columns") or "*").strip()
            if cols1 == "*" and cols2 != "*":
                _remove_node(id2, edges, pin)
            elif cols1 != "*" and cols2 == "*":
                _remove_node(id2, edges, nid)
            elif cols1 != "*" and cols2 != "*":
                set1 = [c.strip() for c in cols1.split(",") if c.strip()]
                set2 = [c.strip() for c in cols2.split(",") if c.strip()]
                composed = [c for c in set1 if c in set2]
                pnode["data"]["columns"] = ",".join(composed) if composed else "*"
                _remove_node(id2, edges, nid)

        if t == "transform.filter" and len(parents.get(nid, [])) == 1:
            pin = parents[nid][0]
            pnode = id2.get(pin)
            if not pnode or pnode.get("data", {}).get("type") != "transform.filter":
                continue
            e1 = (pnode["data"].get("expr") or "").strip()
            e2 = (n["data"].get("expr") or "").strip()
            expr = e2 if not e1 else (e1 if not e2 else f"({e1}) AND ({e2})")
            pnode["data"]["expr"] = expr
            _remove_node(id2, edges, nid)

        if t == "transform.select":
            cols = (n["data"].get("columns") or "*").strip()
            if cols in ("", "*") and len(parents.get(nid, [])) == 1 and len(children.get(nid, [])) == 1:
                _remove_node(id2, edges, nid)

    keep_ids = set(id2.keys())
    nodes_out = [id2[i] for i in order if i in keep_ids]
    uniq, seen = [], set()
    for e in edges:
        key = (e["source"], e["target"])
        if key not in seen and e["source"] in keep_ids and e["target"] in keep_ids:
            uniq.append(e)
            seen.add(key)
    return nodes_out, uniq

def optimize(nodes: List[Node], edges: List[Edge], target_id: str | None = None) -> Tuple[List[Node], List[Edge]]:
    n1, e1 = prune_dead(nodes, edges, target_id)
    n2, e2 = combine_redundant(n1, e1)
    return n2, e2
