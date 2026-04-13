"use client";

import { useEffect, useMemo, useState } from "react";
import ReactECharts from "echarts-for-react";
import {
  createAgentConversationDetailed,
  deleteAgentConversationDetailed,
  getAgentConversationsDetailed,
  getAgentMessagesDetailed,
  postAgentMessageDetailed,
} from "../../lib/api";
import { useScope } from "../../components/layout/ScopeProvider";
import type { AgentChartSpec, AgentConversation, AgentMessage } from "../../lib/types";

function renderChartSpec(spec: AgentChartSpec) {
  if (spec.type === "table") {
    return (
      <div className="overflow-x-auto rounded-lg border border-ink/10 bg-white">
        <table className="min-w-full text-left text-xs">
          <thead className="bg-slate-50 text-slate">
            <tr>
              {(spec.columns || []).map((column) => (
                <th key={column} className="px-3 py-2 font-medium">
                  {column}
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            {(spec.rows || []).map((row, idx) => (
              <tr key={idx} className="border-t border-ink/10">
                {row.map((cell, jdx) => (
                  <td key={`${idx}-${jdx}`} className="px-3 py-2 text-ink">
                    {cell == null ? "-" : String(cell)}
                  </td>
                ))}
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    );
  }

  const option = {
    tooltip: { trigger: "axis" },
    grid: { left: 36, right: 20, top: 28, bottom: 40 },
    legend: { top: 0 },
    xAxis: {
      type: "category",
      data: spec.x || [],
      axisLabel: { color: "#526079", fontSize: 11 },
    },
    yAxis: {
      type: "value",
      axisLabel: { color: "#526079", formatter: "{value}%" },
      splitLine: { lineStyle: { color: "#E3E8EF" } },
    },
    series: (spec.series || []).map((series) => ({
      name: series.name,
      type: spec.type,
      data: series.data,
      smooth: spec.type === "line",
      barMaxWidth: spec.type === "bar" ? 26 : undefined,
    })),
  };
  return <ReactECharts option={option} style={{ width: "100%", height: 280 }} notMerge lazyUpdate />;
}

export default function AgentPage() {
  const { scope } = useScope();
  const [conversations, setConversations] = useState<AgentConversation[]>([]);
  const [activeConversationId, setActiveConversationId] = useState<string | null>(null);
  const [messages, setMessages] = useState<AgentMessage[]>([]);
  const [input, setInput] = useState("");
  const [loadingConversations, setLoadingConversations] = useState(false);
  const [loadingMessages, setLoadingMessages] = useState(false);
  const [sending, setSending] = useState(false);
  const [error, setError] = useState<string | null>(null);

  async function loadConversations(selectLatest = false) {
    setLoadingConversations(true);
    setError(null);
    const result = await getAgentConversationsDetailed();
    setLoadingConversations(false);
    if (!result.ok) {
      setError((result.data as { detail?: string } | null)?.detail || result.error || "Failed to load conversations.");
      return;
    }
    const items = Array.isArray((result.data as { items?: unknown[] } | null)?.items)
      ? ((result.data as { items: AgentConversation[] }).items || [])
      : [];
    setConversations(items);
    if (selectLatest && items.length > 0) {
      setActiveConversationId(items[0].id);
    }
  }

  async function loadMessages(conversationId: string) {
    setLoadingMessages(true);
    setError(null);
    const result = await getAgentMessagesDetailed(conversationId);
    setLoadingMessages(false);
    if (!result.ok) {
      setError((result.data as { detail?: string } | null)?.detail || result.error || "Failed to load messages.");
      return;
    }
    const items = Array.isArray((result.data as { items?: unknown[] } | null)?.items)
      ? ((result.data as { items: AgentMessage[] }).items || [])
      : [];
    setMessages(items);
  }

  useEffect(() => {
    loadConversations(true);
  }, []);

  useEffect(() => {
    if (!activeConversationId) {
      setMessages([]);
      return;
    }
    loadMessages(activeConversationId);
  }, [activeConversationId]);

  async function handleNewConversation() {
    const result = await createAgentConversationDetailed({});
    if (!result.ok) {
      setError((result.data as { detail?: string } | null)?.detail || result.error || "Failed to create conversation.");
      return;
    }
    const conversation = ((result.data as { conversation?: AgentConversation } | null)?.conversation || null) as
      | AgentConversation
      | null;
    if (!conversation) return;
    setConversations((prev) => [conversation, ...prev]);
    setActiveConversationId(conversation.id);
    setMessages([]);
  }

  async function handleDeleteConversation(id: string) {
    const result = await deleteAgentConversationDetailed(id);
    if (!result.ok) {
      setError((result.data as { detail?: string } | null)?.detail || result.error || "Failed to delete conversation.");
      return;
    }
    const next = conversations.filter((conv) => conv.id !== id);
    setConversations(next);
    if (activeConversationId === id) {
      setActiveConversationId(next[0]?.id || null);
    }
  }

  async function handleSend() {
    if (!activeConversationId || !input.trim() || sending) return;
    setSending(true);
    setError(null);
    const userText = input.trim();
    setInput("");

    const optimisticUserMessage: AgentMessage = {
      id: `temp-user-${Date.now()}`,
      conversation_id: activeConversationId,
      role: "user",
      content: userText,
      chart_spec: null,
      meta: null,
      created_at: new Date().toISOString(),
    };
    setMessages((prev) => [...prev, optimisticUserMessage]);

    const result = await postAgentMessageDetailed(activeConversationId, {
      message: userText,
      context: {
        taxonomy_view: scope.taxonomyView,
        sector: scope.sector,
        subsector: scope.subsector,
        category: scope.category,
        years: scope.years,
        gender: scope.gender,
        nse: scope.nse,
        age_min: scope.ageMin,
        age_max: scope.ageMax,
        brands: scope.brands,
      },
    });
    setSending(false);

    if (!result.ok) {
      setError((result.data as { detail?: string } | null)?.detail || result.error || "Failed to send message.");
      setMessages((prev) => prev.filter((msg) => msg.id !== optimisticUserMessage.id));
      return;
    }

    const assistant = ((result.data as { assistant?: AgentMessage } | null)?.assistant || null) as AgentMessage | null;
    if (assistant) {
      setMessages((prev) => [...prev.filter((msg) => msg.id !== optimisticUserMessage.id), optimisticUserMessage, assistant]);
    }
    loadConversations(false);
  }

  const activeConversation = useMemo(
    () => conversations.find((conv) => conv.id === activeConversationId) || null,
    [conversations, activeConversationId]
  );

  return (
    <div className="grid min-h-[calc(100vh-140px)] grid-cols-12 gap-4">
      <aside className="col-span-12 rounded-2xl border border-ink/10 bg-white p-3 shadow-sm lg:col-span-4 xl:col-span-3">
        <div className="mb-3 flex items-center justify-between gap-2">
          <h1 className="text-sm font-semibold text-ink">Agent</h1>
          <button
            type="button"
            onClick={handleNewConversation}
            className="rounded-lg border border-ink/10 bg-white px-2.5 py-1.5 text-xs font-medium text-ink hover:bg-slate-50"
          >
            New chat
          </button>
        </div>
        <div className="space-y-2 overflow-y-auto pr-1" style={{ maxHeight: "calc(100vh - 230px)" }}>
          {loadingConversations ? <p className="text-xs text-slate">Loading conversations...</p> : null}
          {!loadingConversations && !conversations.length ? (
            <p className="rounded-lg border border-dashed border-ink/20 px-3 py-4 text-xs text-slate">
              No conversations yet.
            </p>
          ) : null}
          {conversations.map((conversation) => {
            const active = conversation.id === activeConversationId;
            return (
              <div
                key={conversation.id}
                className={`rounded-xl border p-2 ${active ? "border-emerald-300 bg-emerald-50/50" : "border-ink/10 bg-white"}`}
              >
                <button
                  type="button"
                  onClick={() => setActiveConversationId(conversation.id)}
                  className="w-full text-left"
                >
                  <p className="truncate text-sm font-medium text-ink">{conversation.title || "New chat"}</p>
                  <p className="mt-0.5 text-[11px] text-slate">
                    {new Date(conversation.last_message_at).toLocaleString()}
                  </p>
                </button>
                <div className="mt-2 flex justify-end">
                  <button
                    type="button"
                    onClick={() => handleDeleteConversation(conversation.id)}
                    className="text-[11px] text-rose-600 hover:text-rose-700"
                  >
                    Delete
                  </button>
                </div>
              </div>
            );
          })}
        </div>
      </aside>

      <section className="col-span-12 rounded-2xl border border-ink/10 bg-white shadow-sm lg:col-span-8 xl:col-span-9">
        <div className="border-b border-ink/10 px-4 py-3">
          <h2 className="text-sm font-semibold text-ink">{activeConversation?.title || "Chat"}</h2>
          <p className="mt-1 text-xs text-slate">
            Ask about benchmark data. The assistant answers only from BBS datasets and your access scope.
          </p>
        </div>
        <div className="space-y-3 overflow-y-auto px-4 py-4" style={{ maxHeight: "calc(100vh - 330px)" }}>
          {!activeConversationId ? (
            <div className="rounded-xl border border-dashed border-ink/20 p-4 text-sm text-slate">
              Create a new conversation to start.
            </div>
          ) : null}
          {loadingMessages ? <p className="text-xs text-slate">Loading messages...</p> : null}
          {activeConversationId && !loadingMessages && !messages.length ? (
            <div className="rounded-xl border border-dashed border-ink/20 p-4 text-sm text-slate">
              Try: "Compare NPS trend for my current macrosector" or "Show a bar chart of Brand Awareness by segment".
            </div>
          ) : null}
          {messages.map((msg) => {
            const isUser = msg.role === "user";
            return (
              <div key={msg.id} className={`flex ${isUser ? "justify-end" : "justify-start"}`}>
                <div
                  className={`max-w-[88%] rounded-2xl px-4 py-3 text-sm ${
                    isUser ? "bg-ink text-white" : "border border-ink/10 bg-slate-50 text-ink"
                  }`}
                >
                  <p className="whitespace-pre-wrap">{msg.content}</p>
                  {!isUser && msg.chart_spec ? <div className="mt-3">{renderChartSpec(msg.chart_spec)}</div> : null}
                </div>
              </div>
            );
          })}
          {error ? <p className="text-xs text-rose-600">{error}</p> : null}
        </div>
        <div className="border-t border-ink/10 p-3">
          <div className="flex gap-2">
            <textarea
              value={input}
              onChange={(event) => setInput(event.target.value)}
              placeholder="Ask about Journey, Network, Trends, benchmarks, sectors..."
              rows={2}
              className="min-h-[52px] flex-1 resize-y rounded-xl border border-ink/10 px-3 py-2 text-sm outline-none ring-emerald-200 focus:ring"
              disabled={!activeConversationId || sending}
            />
            <button
              type="button"
              onClick={handleSend}
              disabled={!activeConversationId || !input.trim() || sending}
              className="rounded-xl bg-ink px-4 py-2 text-sm font-medium text-white disabled:opacity-60"
            >
              {sending ? "Sending..." : "Send"}
            </button>
          </div>
        </div>
      </section>
    </div>
  );
}
