import { NextRequest, NextResponse } from "next/server";
import { ensureConversationOwner, requireAgentAccess } from "../../_lib/agent";
import { supabaseAdminPostgrest } from "../../../_lib/supabase-admin";

export const dynamic = "force-dynamic";

export async function DELETE(
  request: NextRequest,
  context: { params: { conversationId: string } }
) {
  const { denied, authz } = await requireAgentAccess(request);
  if (denied) return denied;

  const conversationId = context.params.conversationId;
  const ownership = await ensureConversationOwner(conversationId, authz);
  if (!ownership.ok) {
    return NextResponse.json({ detail: ownership.detail, error: (ownership as { error?: unknown }).error }, { status: ownership.status });
  }

  const update = await supabaseAdminPostgrest(`agent_conversations?id=eq.${encodeURIComponent(conversationId)}`, {
    method: "PATCH",
    body: { archived: true, updated_at: new Date().toISOString() },
    headers: { Prefer: "return=representation" },
  });
  if (!update.response.ok) {
    return NextResponse.json({ detail: "Failed to archive conversation.", error: update.data }, { status: 500 });
  }
  return NextResponse.json({ ok: true });
}
