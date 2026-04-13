import { NextRequest, NextResponse } from "next/server";
import { buildConversationTitle, createConversationForUser, requireAgentAccess, resolveEffectiveAgentUserId } from "../_lib/agent";
import { supabaseAdminPostgrest } from "../../_lib/supabase-admin";

export const dynamic = "force-dynamic";

export async function GET(request: NextRequest) {
  const { denied, authz } = await requireAgentAccess(request);
  if (denied) return denied;
  const effectiveUserId = await resolveEffectiveAgentUserId(authz);

  const result = await supabaseAdminPostgrest(
    `agent_conversations?select=id,user_id,title,created_at,updated_at,last_message_at,archived&user_id=eq.${encodeURIComponent(
      effectiveUserId
    )}&archived=is.false&order=last_message_at.desc&limit=200`
  );
  if (!result.response.ok) {
    return NextResponse.json({ detail: "Failed to load conversations.", error: result.data }, { status: 500 });
  }
  return NextResponse.json({ items: Array.isArray(result.data) ? result.data : [] });
}

export async function POST(request: NextRequest) {
  const { denied, authz } = await requireAgentAccess(request);
  if (denied) return denied;
  const effectiveUserId = await resolveEffectiveAgentUserId(authz);

  const payload = (await request.json().catch(() => ({}))) as { title?: string };
  const title = buildConversationTitle(payload.title || "");
  const insert = await createConversationForUser(effectiveUserId, title);
  if (!insert.response.ok) {
    return NextResponse.json({ detail: "Failed to create conversation.", error: insert.data }, { status: 500 });
  }
  const row = Array.isArray(insert.data) ? insert.data[0] : null;
  return NextResponse.json({ ok: true, conversation: row });
}
