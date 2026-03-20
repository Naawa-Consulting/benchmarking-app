import { createClient } from "../../utils/supabase/client";

export function createSupabaseBrowserClient() {
  return createClient();
}
