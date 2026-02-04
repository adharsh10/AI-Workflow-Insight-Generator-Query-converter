import React, { useMemo, useState } from "react";
import { Sparkles, Wand2, BarChart3, Send, RefreshCw } from "lucide-react";

function computeQuickInsights(sampleRows) {
  if (!Array.isArray(sampleRows) || sampleRows.length === 0) {
    return { text: "No data loaded yet. Run the workflow (or Preview a node) first.", tables: [] };
  }

  const cols = Object.keys(sampleRows[0] || {});
  const nullCounts = {};
  for (const c of cols) nullCounts[c] = 0;

  const n = sampleRows.length;
  for (let i = 0; i < n; i++) {
    const row = sampleRows[i] || {};
    for (const c of cols) {
      const v = row[c];
      if (v === null || v === undefined || v === "") nullCounts[c] += 1;
    }
  }

  // Detect numeric columns from sample
  const numericCols = cols.filter((c) => {
    let hits = 0;
    const m = Math.min(n, 50);
    for (let i = 0; i < m; i++) {
      const v = sampleRows[i]?.[c];
      if (v === null || v === undefined || v === "") continue;
      if (!Number.isNaN(Number(v))) hits += 1;
    }
    return hits >= Math.min(5, Math.floor(m / 2));
  });

  const numRows = [];
  for (const c of numericCols) {
    const vals = sampleRows
      .map((r) => Number(r?.[c]))
      .filter((v) => Number.isFinite(v));
    if (!vals.length) continue;
    vals.sort((a, b) => a - b);
    const sum = vals.reduce((a, b) => a + b, 0);
    const mean = sum / vals.length;
    const median = vals[Math.floor(vals.length * 0.5)];
    numRows.push([c, vals.length, mean.toFixed(2), median.toFixed(2), vals[0], vals[vals.length - 1]]);
  }

  const textLines = [
    `Rows (sample): ${n}`,
    `Columns: ${cols.length}`,
    "",
    "Null/empty counts (sample):",
    ...cols.map((c) => `- ${c}: ${nullCounts[c]}`),
  ];

  const tables = numRows.length
    ? [
        {
          title: "Numeric summary (sample)",
          columns: ["column", "n", "mean", "median", "min", "max"],
          rows: numRows,
        },
      ]
    : [];

  return { text: textLines.join("\n"), tables };
}

export default function AICopilotPanel({
  width = 400,
  sampleRows,
  schema,
  onApplyWorkflow,
}) {
  const [insPrompt, setInsPrompt] = useState("");
  const [insOutput, setInsOutput] = useState("");
  const [insTables, setInsTables] = useState([]);
  const [insLoading, setInsLoading] = useState(false);

  const [chatMode, setChatMode] = useState("workflow"); // workflow | insights
  const [chatInput, setChatInput] = useState("");
  const [messages, setMessages] = useState(() => [
    {
      id: "m0",
      role: "assistant",
      content:
        "I'm your Copilot. Describe the workflow you want, or ask questions about your data.\n\nTip: Run/Preview once to load data context for insights.",
      actions: [],
    },
  ]);

  const canQuickInsights = Array.isArray(sampleRows) && sampleRows.length > 0;

  const schemaHint = useMemo(() => {
    const cols = schema?.columns || schema || [];
    if (!Array.isArray(cols) || cols.length === 0) return "";
    const names = cols
      .map((c) => (typeof c === "string" ? c : c?.name))
      .filter(Boolean)
      .slice(0, 12);
    return names.length ? `Columns: ${names.join(", ")}${cols.length > 12 ? ", ..." : ""}` : "";
  }, [schema]);

  function pushMessage(role, content, actions = []) {
    setMessages((prev) => [...prev, { id: `m${prev.length + 1}`, role, content, actions }]);
  }

  function runQuickInsights() {
    const out = computeQuickInsights(sampleRows || []);
    setInsOutput(out.text);
    setInsTables(out.tables || []);
  }

  async function runAIInsightsStub() {
    setInsLoading(true);
    try {
      // Stub for now: echo prompt + quick stats
      const out = computeQuickInsights(sampleRows || []);
      const txt =
        "LLM insights (stub - backend not connected yet).\n\n" +
        (insPrompt?.trim() ? `Your question: ${insPrompt.trim()}\n\n` : "") +
        out.text;
      setInsOutput(txt);
      setInsTables(out.tables || []);
    } finally {
      setInsLoading(false);
    }
  }

  async function onSendChat() {
    const prompt = chatInput.trim();
    if (!prompt) return;
    setChatInput("");
    pushMessage("user", prompt);

    if (chatMode === "workflow") {
      pushMessage("assistant", "Got it. I can generate a workflow for that (stub for now).", [
        { label: "Apply workflow to canvas", kind: "apply_workflow", payload: { prompt } },
      ]);
    } else {
      pushMessage(
        "assistant",
        "I can help with data questions too. For now, use the Insights panel above (Quick insights works locally; AI insights is stubbed)."
      );
    }
  }

  return (
    <div
      className="h-full border-r bg-white flex flex-col"
      style={{ width, minWidth: 320, maxWidth: 520 }}
    >
      {/* Header */}
      <div className="px-3 py-2 border-b flex items-center gap-2">
        <Sparkles size={16} className="text-purple-600" />
        <div className="font-semibold text-sm">Copilot</div>
        <div className="flex-1" />
        <div className="text-xs text-gray-400">stubbed</div>
      </div>

      {/* Insights (top-left) */}
      <div className="p-3 border-b">
        <div className="flex items-center gap-2 mb-2">
          <BarChart3 size={16} className="text-amber-600" />
          <div className="font-medium text-sm">Generate Insights</div>
          <div className="flex-1" />
          <button
            className="px-2 py-1 text-xs rounded-lg border border-gray-200 hover:bg-gray-50 flex items-center gap-1"
            onClick={() => {
              setInsPrompt("");
              setInsOutput("");
              setInsTables([]);
            }}
            title="Clear"
          >
            <RefreshCw size={12} />
            Clear
          </button>
        </div>

        {schemaHint ? <div className="text-xs text-gray-500 mb-2">{schemaHint}</div> : null}

        <textarea
          value={insPrompt}
          onChange={(e) => setInsPrompt(e.target.value)}
          placeholder="Ask for insights (e.g., top segments, outliers, trends) ..."
          rows={3}
          className="w-full px-3 py-2 rounded-xl border border-gray-200"
        />

        <div className="mt-2 flex gap-2">
          <button
            className="flex-1 px-3 py-2 rounded-xl border border-gray-200 hover:bg-gray-50 text-sm"
            onClick={runQuickInsights}
            disabled={!canQuickInsights}
            title={!canQuickInsights ? "Run/Preview once to load data" : "Compute summary stats"}
          >
            Quick insights
          </button>
          <button
            className="flex-1 px-3 py-2 rounded-xl border border-gray-200 bg-gray-900 text-white hover:bg-gray-800 text-sm flex items-center justify-center gap-2"
            onClick={runAIInsightsStub}
            disabled={insLoading || (!canQuickInsights && !insPrompt.trim())}
          >
            <Wand2 size={14} />
            {insLoading ? "Thinking..." : "AI insights"}
          </button>
        </div>

        <div className="mt-3 border rounded-xl bg-[#fafafa] max-h-52 overflow-auto p-2">
          <pre className="text-xs whitespace-pre-wrap m-0">{insOutput || "No insights yet."}</pre>

          {insTables?.map((t, idx) => (
            <div key={idx} className="mt-3">
              <div className="text-xs font-semibold mb-1">{t.title}</div>
              <div className="overflow-x-auto">
                <table className="w-full text-xs border-collapse">
                  <thead>
                    <tr>
                      {t.columns.map((c) => (
                        <th key={c} className="text-left border-b border-gray-200 p-1">
                          {c}
                        </th>
                      ))}
                    </tr>
                  </thead>
                  <tbody>
                    {t.rows.map((r, ridx) => (
                      <tr key={ridx}>
                        {r.map((cell, cidx) => (
                          <td key={cidx} className="border-b border-gray-100 p-1">
                            {String(cell)}
                          </td>
                        ))}
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </div>
          ))}
        </div>
      </div>

      {/* Chat */}
      <div className="flex-1 flex flex-col min-h-0">
        <div className="px-3 py-2 border-b flex items-center gap-2">
          <div className="text-xs text-gray-500">Mode</div>
          <select
            value={chatMode}
            onChange={(e) => setChatMode(e.target.value)}
            className="px-2 py-1 rounded-lg border border-gray-200 text-sm"
          >
            <option value="workflow">Workflow</option>
            <option value="insights">Insights</option>
          </select>
          <div className="flex-1" />
          <div className="text-xs text-gray-400">Chat</div>
        </div>

        <div className="flex-1 overflow-auto p-3 space-y-2">
          {messages.map((m) => (
            <div key={m.id} className={m.role === "user" ? "text-right" : "text-left"}>
              <div
                className={
                  "inline-block max-w-[92%] px-3 py-2 rounded-2xl text-sm " +
                  (m.role === "user"
                    ? "bg-blue-600 text-white"
                    : "bg-gray-100 text-gray-900 border border-gray-200")
                }
              >
                <div className="whitespace-pre-wrap">{m.content}</div>

                {Array.isArray(m.actions) && m.actions.length > 0 ? (
                  <div className="mt-2 flex flex-wrap gap-2 justify-end">
                    {m.actions.map((a, i) => (
                      <button
                        key={i}
                        className="px-2 py-1 rounded-lg text-xs border border-gray-300 hover:bg-white bg-white/80"
                        onClick={() => {
                          if (a.kind === "apply_workflow") {
                            onApplyWorkflow?.(a.payload?.prompt || "");
                          }
                        }}
                      >
                        {a.label}
                      </button>
                    ))}
                  </div>
                ) : null}
              </div>
            </div>
          ))}
        </div>

        <div className="p-3 border-t flex gap-2">
          <textarea
            value={chatInput}
            onChange={(e) => setChatInput(e.target.value)}
            placeholder={chatMode === "workflow" ? "Describe a workflow..." : "Ask a data question..."}
            rows={2}
            className="flex-1 px-3 py-2 rounded-xl border border-gray-200 resize-none"
            onKeyDown={(e) => {
              if ((e.ctrlKey || e.metaKey) && e.key === "Enter") onSendChat();
            }}
          />
          <button
            className="px-3 py-2 rounded-xl border border-gray-200 bg-gray-900 text-white hover:bg-gray-800 flex items-center gap-2"
            onClick={onSendChat}
            title="Send (Ctrl/Cmd+Enter)"
          >
            <Send size={14} />
            Send
          </button>
        </div>
      </div>
    </div>
  );
}
