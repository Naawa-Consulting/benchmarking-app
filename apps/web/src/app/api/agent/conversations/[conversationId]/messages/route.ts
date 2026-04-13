import { NextRequest, NextResponse } from "next/server";
import {
  buildConversationTitle,
  ensureConversationOwner,
  generateAgentResponse,
  requireAgentAccess,
  type AgentContext,
} from "../../../_lib/agent";
import { supabaseAdminPostgrest } from "../../../../_lib/supabase-admin";

export const dynamic = "force-dynamic";

async function loadConversationMessages(conversationId: string) {
  return supabaseAdminPostgrest(
    `agent_messages?select=id,conversation_id,role,content,chart_spec,meta,created_at&conversation_id=eq.${encodeURIComponent(
      conversationId
    )}&order=created_at.asc&limit=500`
  );
}

export async function GET(
  request: NextRequest,
  context: { params: { conversationId: string } }
) {
  const { denied, authz } = await requireAgentAccess(request);
  if (denied) return denied;

  const conversationId = context.params.conversationId;
  const ownership = await ensureConversationOwner(conversationId, authz);
  if (!ownership.ok) {
    return NextResponse.json({ detail: ownership.detail }, { status: ownership.status });
  }

  const result = await loadConversationMessages(conversationId);
  if (!result.response.ok) {
    return NextResponse.json({ detail: "Failed to load messages.", error: result.data }, { status: 500 });
  }
  return NextResponse.json({ items: Array.isArray(result.data) ? result.data : [] });
}

export async function POST(
  request: NextRequest,
  context: { params: { conversationId: string } }
) {
  const { denied, authz } = await requireAgentAccess(request);
  if (denied) return denied;

  const conversationId = context.params.conversationId;
  const ownership = await ensureConversationOwner(conversationId, authz);
  if (!ownership.ok) {
    return NextResponse.json({ detail: ownership.detail }, { status: ownership.status });
  }

  const payload = (await request.json().catch(() => ({}))) as { message?: string; context?: AgentContext };
  const message = typeof payload.message === "string" ? payload.message.trim() : "";
  if (!message) {
    return NextResponse.json({ detail: "Message is required." }, { status: 400 });
  }

  const now = new Date().toISOString();
  const insertUser = await supabaseAdminPostgrest("agent_messages", {
    method: "POST",
    headers: { Prefer: "return=representation" },
    body: [
      {
        conversation_id: conversationId,
        role: "user",
        content: message,
        chart_spec: null,
        meta: { source: "ui" },
        created_at: now,
      },
    ],
  });
  if (!insertUser.response.ok) {
    return NextResponse.json({ detail: "Failed to save user message.", error: insertUser.data }, { status: 500 });
  }

  const historyResult = await loadConversationMessages(conversationId);
  const historyRows = Array.isArray(historyResult.data)
    ? (historyResult.data as Array<{ role?: string; content?: string }>)
    : [];
  const history = historyRows
    .filter((row) => (row.role === "user" || row.role === "assistant") && typeof row.content === "string")
    .map((row) => ({ role: row.role as "user" | "assistant", content: row.content as string }));

  let generated: Awaited<ReturnType<typeof generateAgentResponse>>;
  try {
    generated = await generateAgentResponse({
      request,
      authz,
      userMessage: message,
      context: payload.context || null,
      history,
    });
  } catch (error) {
    const detail = error instanceof Error ? error.message : "Agent processing failed.";
    return NextResponse.json({ detail }, { status: 502 });
  }

  const assistantInsert = await supabaseAdminPostgrest("agent_messages", {
    method: "POST",
    headers: { Prefer: "return=representation" },
    body: [
      {
        conversation_id: conversationId,
        role: "assistant",
        content: generated.text,
        chart_spec: generated.chart_spec,
        meta: generated.meta,
        created_at: new Date().toISOString(),
      },
    ],
  });
  if (!assistantInsert.response.ok) {
    return NextResponse.json({ detail: "Failed to save assistant response.", error: assistantInsert.data }, { status: 500 });
  }

  const currentTitle = String((ownership.row as { title?: string }).title || "New chat");
  const patchConversationBody: Record<string, unknown> = {
    updated_at: new Date().toISOString(),
    last_message_at: new Date().toISOString(),
  };
  if (!currentTitle || currentTitle === "New chat") {
    patchConversationBody.title = buildConversationTitle(message);
  }
  await supabaseAdminPostgrest(`agent_conversations?id=eq.${encodeURIComponent(conversationId)}`, {
    method: "PATCH",
    body: patchConversationBody,
    headers: { Prefer: "return=minimal" },
  });

  return NextResponse.json({
    ok: true,
    assistant: Array.isArray(assistantInsert.data) ? assistantInsert.data[0] : null,
  });
}
