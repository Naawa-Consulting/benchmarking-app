"use client";

import { useEffect, useMemo, useState } from "react";
import {
  createAdminUserDetailed,
  deleteAdminUserDetailed,
  getAdminUserAccessDetailed,
  getAdminUsersDetailed,
  patchAdminUserAccessDetailed,
  patchAdminUserRoleDetailed,
} from "../../lib/api";
import type { AdminUserItem, BbsRole, UserAccessPayload } from "../../lib/types";

type AuthzMe = {
  role: BbsRole;
  is_admin_module_allowed?: boolean;
};

const ROLE_OPTIONS: BbsRole[] = ["owner", "admin", "analyst", "viewer"];
const MARKET_SCOPE_TYPES = ["market_sector", "market_subsector", "market_category"] as const;
const MARKET_SCOPE_LABELS: Record<(typeof MARKET_SCOPE_TYPES)[number], string> = {
  market_sector: "Macrosector",
  market_subsector: "Segmento",
  market_category: "Categoría Comercial",
};

function roleLabel(role: BbsRole) {
  if (role === "admin") return "administrator";
  return role;
}

function toggleValue(values: string[], value: string) {
  if (values.includes(value)) return values.filter((item) => item !== value);
  return [...values, value].sort((a, b) => a.localeCompare(b));
}

export default function AdminUsersPage() {
  const [authz, setAuthz] = useState<AuthzMe | null>(null);
  const [users, setUsers] = useState<AdminUserItem[]>([]);
  const [selectedUserId, setSelectedUserId] = useState<string>("");
  const [access, setAccess] = useState<UserAccessPayload | null>(null);
  const [loadingUsers, setLoadingUsers] = useState(false);
  const [loadingAccess, setLoadingAccess] = useState(false);
  const [saving, setSaving] = useState(false);
  const [message, setMessage] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);

  const [newEmail, setNewEmail] = useState("");
  const [newRole, setNewRole] = useState<BbsRole>("viewer");

  const selectedUser = useMemo(
    () => users.find((item) => item.id === selectedUserId) || null,
    [selectedUserId, users]
  );

  useEffect(() => {
    let active = true;
    fetch("/api/auth/me", { cache: "no-store" })
      .then((response) => response.json())
      .then((data: AuthzMe) => {
        if (!active) return;
        setAuthz(data);
      })
      .catch(() => {
        if (!active) return;
        setAuthz({ role: "viewer" });
      });
    return () => {
      active = false;
    };
  }, []);

  async function loadUsers() {
    setLoadingUsers(true);
    setError(null);
    const result = await getAdminUsersDetailed();
    if (!result.ok || !result.data || typeof result.data !== "object") {
      setError("No se pudieron cargar los usuarios.");
      setLoadingUsers(false);
      return;
    }
    const data = result.data as { items?: AdminUserItem[] };
    const items = Array.isArray(data.items) ? data.items : [];
    setUsers(items);
    setSelectedUserId((prev) => (prev && items.some((item) => item.id === prev) ? prev : items[0]?.id || ""));
    setLoadingUsers(false);
  }

  async function loadAccess(userId: string) {
    if (!userId) {
      setAccess(null);
      return;
    }
    setLoadingAccess(true);
    setError(null);
    const result = await getAdminUserAccessDetailed(userId);
    if (!result.ok || !result.data || typeof result.data !== "object") {
      setError("No se pudieron cargar los accesos del usuario.");
      setLoadingAccess(false);
      return;
    }
    setAccess(result.data as UserAccessPayload);
    setLoadingAccess(false);
  }

  useEffect(() => {
    if (!authz?.is_admin_module_allowed) return;
    void loadUsers();
  }, [authz?.is_admin_module_allowed]);

  useEffect(() => {
    if (!selectedUserId) {
      setAccess(null);
      return;
    }
    void loadAccess(selectedUserId);
  }, [selectedUserId]);

  if (!authz) {
    return <main className="py-8 text-sm text-slate">Loading...</main>;
  }

  if (!authz.is_admin_module_allowed) {
    return (
      <main className="py-8">
        <div className="main-surface rounded-3xl p-6">
          <h1 className="text-xl font-semibold">Admin</h1>
          <p className="mt-2 text-sm text-slate">No tienes permisos para gestionar usuarios.</p>
        </div>
      </main>
    );
  }

  const selectedUserRole: BbsRole = selectedUser?.role || "viewer";
  const extractError = (result: { error: string | null; data: unknown }) => {
    const parts: string[] = [];
    if (result.data && typeof result.data === "object") {
      const data = result.data as { detail?: string; error?: unknown };
      if (typeof data.detail === "string" && data.detail.trim()) parts.push(data.detail);
      if (typeof data.error === "string" && data.error.trim()) {
        parts.push(data.error);
      } else if (data.error) {
        parts.push(JSON.stringify(data.error));
      }
    }
    if (result.error) parts.push(result.error);
    return parts.join(" ") || "Unexpected error";
  };

  return (
    <main className="py-6 space-y-6">
      <section className="main-surface rounded-3xl p-6">
        <h1 className="text-2xl font-semibold">Admin · User Management</h1>
        <p className="mt-2 text-sm text-slate">
          Alta de usuarios, asignación de rol y control de visibilidad por sector/subsector/categoría.
        </p>
        {message && <p className="mt-3 text-sm text-emerald-700">{message}</p>}
        {error && <p className="mt-3 text-sm text-red-600">{error}</p>}
      </section>

      <section className="grid gap-6 lg:grid-cols-[1.2fr_1.8fr]">
        <div className="main-surface rounded-3xl p-6 space-y-4">
          <div className="flex items-center justify-between">
            <h2 className="text-lg font-semibold">Usuarios</h2>
            <button
              type="button"
              className="rounded-full border border-ink/10 px-4 py-1.5 text-xs font-medium"
              onClick={() => void loadUsers()}
              disabled={loadingUsers}
            >
              Refresh
            </button>
          </div>

          <div className="space-y-2 max-h-[420px] overflow-auto pr-1">
            {users.map((user) => (
              <button
                key={user.id}
                type="button"
                onClick={() => setSelectedUserId(user.id)}
                className={`w-full rounded-2xl border px-3 py-2 text-left text-sm ${
                  selectedUserId === user.id
                    ? "border-emerald-400 bg-emerald-50"
                    : "border-ink/10 bg-white hover:bg-slate-50"
                }`}
              >
                <p className="font-medium text-ink">{user.email || user.id}</p>
              <p className="text-xs text-slate">
                  Role: {roleLabel(user.role)} {user.disabled ? "· Disabled" : "· Enabled"} · Scopes: MS:{user.scope_counts.market_sector} SEG:
                  {user.scope_counts.market_subsector} CAT:{user.scope_counts.market_category}
                </p>
              </button>
            ))}
            {!users.length && <p className="text-xs text-slate">No users found.</p>}
          </div>

          <div className="rounded-2xl border border-ink/10 bg-white p-3 space-y-3">
            <p className="text-sm font-semibold">Alta de usuario</p>
            <input
              className="w-full rounded-xl border border-ink/10 px-3 py-2 text-sm"
              placeholder="email@dominio.com"
              value={newEmail}
              onChange={(event) => setNewEmail(event.target.value)}
            />
            <select
              className="w-full rounded-xl border border-ink/10 px-3 py-2 text-sm"
              value={newRole}
              onChange={(event) => setNewRole(event.target.value as BbsRole)}
            >
              {ROLE_OPTIONS.map((role) => (
                <option key={role} value={role}>
                  {roleLabel(role)}
                </option>
              ))}
            </select>
            <button
              type="button"
              className="rounded-full bg-emerald-600 px-4 py-2 text-xs font-semibold text-white disabled:opacity-60"
              disabled={saving || !newEmail.trim()}
              onClick={async () => {
                setSaving(true);
                setMessage(null);
                setError(null);
                const result = await createAdminUserDetailed({ email: newEmail.trim(), role: newRole });
                if (!result.ok) {
                  setError(`No se pudo crear el usuario. ${extractError(result)}`);
                  setSaving(false);
                  return;
                }
                const data = (result.data || {}) as {
                  invite?: { sent?: boolean; manual_link?: string | null; error?: unknown };
                };
                if (data.invite?.sent) {
                  setMessage("Usuario creado y correo de invitación enviado.");
                } else if (typeof data.invite?.manual_link === "string" && data.invite.manual_link.trim()) {
                  setMessage(
                    `Usuario creado. No se pudo enviar email automático; usa este link manual: ${data.invite.manual_link}`
                  );
                } else {
                  setMessage("Usuario creado. No se pudo enviar email automático.");
                }
                setNewEmail("");
                setNewRole("viewer");
                await loadUsers();
                setSaving(false);
              }}
            >
              Create user
            </button>
          </div>
        </div>

        <div className="main-surface rounded-3xl p-6 space-y-4">
          <h2 className="text-lg font-semibold">Detalle y Accesos</h2>
          {!selectedUser && <p className="text-sm text-slate">Selecciona un usuario para editar.</p>}

          {selectedUser && (
            <>
              <div className="rounded-2xl border border-ink/10 bg-white p-4 space-y-3">
                <p className="text-sm">
                  <span className="font-semibold">Email:</span> {selectedUser.email || selectedUser.id}
                </p>
                <p className="text-xs text-slate">
                  Created: {selectedUser.created_at || "-"} · Last sign in: {selectedUser.last_sign_in_at || "-"}
                </p>
                <p className="text-xs text-slate">
                  Estado: {selectedUser.disabled ? "Deshabilitado" : "Habilitado"}
                </p>
                <div className="flex items-center gap-2">
                  <span className="text-sm font-medium">Role</span>
                  <select
                    className="rounded-xl border border-ink/10 px-3 py-2 text-sm"
                    value={selectedUserRole}
                    onChange={async (event) => {
                      const role = event.target.value as BbsRole;
                      setSaving(true);
                      setError(null);
                      const result = await patchAdminUserRoleDetailed(selectedUser.id, { role });
                      if (!result.ok) {
                        setError(`No se pudo actualizar el rol. ${extractError(result)}`);
                        setSaving(false);
                        return;
                      }
                      setMessage("Rol actualizado.");
                      await loadUsers();
                      setSaving(false);
                    }}
                  >
                    {ROLE_OPTIONS.map((role) => (
                      <option key={role} value={role}>
                        {roleLabel(role)}
                      </option>
                    ))}
                  </select>
                </div>
                <div className="flex flex-wrap items-center gap-2 pt-1">
                  <button
                    type="button"
                    className="rounded-full border border-ink/10 px-3 py-1.5 text-xs font-medium"
                    disabled={saving}
                    onClick={async () => {
                      setSaving(true);
                      setError(null);
                      const result = await patchAdminUserRoleDetailed(selectedUser.id, {
                        disabled: !Boolean(selectedUser.disabled),
                      });
                      if (!result.ok) {
                        setError(`No se pudo actualizar el estado del usuario. ${extractError(result)}`);
                        setSaving(false);
                        return;
                      }
                      setMessage(
                        !Boolean(selectedUser.disabled)
                          ? "Usuario deshabilitado."
                          : "Usuario habilitado."
                      );
                      await loadUsers();
                      setSaving(false);
                    }}
                  >
                    {selectedUser.disabled ? "Enable user" : "Disable user"}
                  </button>
                  <button
                    type="button"
                    className="rounded-full border border-red-300 px-3 py-1.5 text-xs font-medium text-red-700"
                    disabled={saving}
                    onClick={async () => {
                      const confirmed = window.confirm(
                        "¿Eliminar este usuario? Esta acción borra accesos/roles y no se puede deshacer."
                      );
                      if (!confirmed) return;
                      setSaving(true);
                      setError(null);
                      const result = await deleteAdminUserDetailed(selectedUser.id);
                      if (!result.ok) {
                        setError(`No se pudo eliminar el usuario. ${extractError(result)}`);
                        setSaving(false);
                        return;
                      }
                      setMessage("Usuario eliminado.");
                      setSelectedUserId("");
                      setAccess(null);
                      await loadUsers();
                      setSaving(false);
                    }}
                  >
                    Delete user
                  </button>
                </div>
              </div>

              <div className="rounded-2xl border border-ink/10 bg-white p-4 space-y-3">
                <p className="text-sm font-semibold">Permisos adicionales (Viewer)</p>
                {loadingAccess && <p className="text-xs text-slate">Loading access...</p>}
                {access && (
                  <>
                    <label className="flex items-center gap-2 text-sm">
                      <input
                        type="checkbox"
                        checked={access.can_toggle_brands}
                        onChange={(event) =>
                          setAccess((prev) =>
                            prev
                              ? { ...prev, can_toggle_brands: event.target.checked }
                              : prev
                          )
                        }
                      />
                      Enable Brands (brands.toggle)
                    </label>

                    <div className="grid gap-4 md:grid-cols-3">
                      {MARKET_SCOPE_TYPES.map((scopeType) => (
                        <div key={scopeType} className="space-y-2">
                          <p className="text-xs font-semibold uppercase tracking-wide text-slate">
                            {MARKET_SCOPE_LABELS[scopeType]}
                          </p>
                          <div className="max-h-48 overflow-auto space-y-1 rounded-xl border border-ink/10 p-2">
                            {access.available[scopeType].map((value) => (
                              <label key={`${scopeType}-${value}`} className="flex items-center gap-2 text-xs">
                                <input
                                  type="checkbox"
                                  checked={access.scopes[scopeType].includes(value)}
                                  onChange={() =>
                                    setAccess((prev) =>
                                      prev
                                        ? {
                                            ...prev,
                                            scopes: {
                                              ...prev.scopes,
                                              [scopeType]: toggleValue(prev.scopes[scopeType], value),
                                            },
                                          }
                                        : prev
                                    )
                                  }
                                />
                                <span>{value}</span>
                              </label>
                            ))}
                          </div>
                        </div>
                      ))}
                    </div>

                    <button
                      type="button"
                      className="rounded-full bg-emerald-600 px-4 py-2 text-xs font-semibold text-white disabled:opacity-60"
                      disabled={saving}
                      onClick={async () => {
                        if (!access) return;
                        setSaving(true);
                        setError(null);
                        const result = await patchAdminUserAccessDetailed(selectedUser.id, {
                          can_toggle_brands: access.can_toggle_brands,
                          scopes: access.scopes,
                        });
                        if (!result.ok) {
                          setError(`No se pudieron guardar los accesos. ${extractError(result)}`);
                          setSaving(false);
                          return;
                        }
                        setMessage("Accesos guardados correctamente.");
                        await loadUsers();
                        await loadAccess(selectedUser.id);
                        setSaving(false);
                      }}
                    >
                      Save access
                    </button>
                  </>
                )}
              </div>
            </>
          )}
        </div>
      </section>
    </main>
  );
}

