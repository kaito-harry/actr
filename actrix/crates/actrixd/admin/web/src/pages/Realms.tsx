import { useEffect, useState, useCallback } from "react";
import { api, type RealmInfo } from "../lib/api";
import { RealmTable } from "../components/realms/RealmTable";
import { RealmForm } from "../components/realms/RealmForm";
import { HowItWorks } from "../components/ui/HowItWorks";
import { ServicePageLayout } from "../components/layout/ServicePageLayout";

/* ── Realm isolation & cross-realm access diagram ────────────── */

function RealmDiagram() {
  const W = 640;
  const H = 404;
  const nH = 28;

  /* ── Layout: consumers on top, provider on bottom ── */

  const r1 = { x: 16, y: 20, w: 270, h: 84 };      // Realm 1001
  const r3 = { x: 400, y: 20, w: 224, h: 84 };      // Realm 1003
  const r2 = { x: 120, y: 184, w: 390, h: 100 };    // Realm 1002

  /* Nodes */
  const pA = { x: 30, y: 58, w: 80 };
  const pB = { x: 192, y: 58, w: 80 };
  const pC = { x: 472, y: 58, w: 80 };
  const sF = { x: 160, y: 220, w: 114 };
  const sB = { x: 356, y: 220, w: 114 };

  const cx = (n: { x: number; w: number }) => n.x + n.w / 2;

  return (
    <svg viewBox={`0 0 ${W} ${H}`} className="max-w-2xl mx-auto" xmlns="http://www.w3.org/2000/svg">
      <defs>
        <marker id="ag" markerWidth="6" markerHeight="4" refX="6" refY="2" orient="auto">
          <path d="M0,0 L6,2 L0,4" fill="#059669" />
        </marker>
        <marker id="ag2" markerWidth="6" markerHeight="4" refX="0" refY="2" orient="auto">
          <path d="M6,0 L0,2 L6,4" fill="#059669" />
        </marker>
        <marker id="ar" markerWidth="6" markerHeight="4" refX="6" refY="2" orient="auto">
          <path d="M0,0 L6,2 L0,4" fill="#dc2626" />
        </marker>
      </defs>

      {/* ── Layer 1: Realm containers ── */}
      <rect x={r1.x} y={r1.y} width={r1.w} height={r1.h} rx="12"
        fill="#f9fafb" stroke="#e5e7eb" strokeWidth="1" />
      <rect x={r3.x} y={r3.y} width={r3.w} height={r3.h} rx="12"
        fill="#f9fafb" stroke="#e5e7eb" strokeWidth="1" />
      <rect x={r2.x} y={r2.y} width={r2.w} height={r2.h} rx="12"
        fill="#f9fafb" stroke="#e5e7eb" strokeWidth="1" />

      {/* ── Layer 2: Connection lines (drawn over containers, under nodes) ── */}

      {/* PeerB → ServiceFoo (B is closer, arrives at left side of Foo) */}
      <line x1={cx(pB) - 8} y1={pB.y + nH} x2={cx(sF) - 4} y2={sF.y}
        stroke="#059669" strokeWidth="1" markerEnd="url(#ag)" />

      {/* PeerB → ServiceBar (diagonal right) */}
      <line x1={cx(pB) + 18} y1={pB.y + nH} x2={cx(sB) - 24} y2={sB.y}
        stroke="#059669" strokeWidth="1" markerEnd="url(#ag)" />

      {/* PeerC → ServiceFoo (arrives at right side of Foo, no crossing with B→Foo) */}
      <line x1={cx(pC) - 18} y1={pC.y + nH} x2={cx(sF) + 24} y2={sF.y}
        stroke="#059669" strokeWidth="1" markerEnd="url(#ag)" />

      {/* PeerC → ServiceBar (nearly vertical — BLOCKED) */}
      <line x1={cx(pC) + 8} y1={pC.y + nH} x2={cx(sB) + 14} y2={sB.y}
        stroke="#dc2626" strokeWidth="1" strokeDasharray="4 3" markerEnd="url(#ar)" />

      {/* ── Layer 3: Node pills ── */}

      {/* Realm 1001 title */}
      <text x={r1.x + 14} y={r1.y + 20} fontSize="12" fontWeight="700" fill="#9ca3af">
        Realm 1001</text>

      {/* PeerA */}
      <rect x={pA.x} y={pA.y} width={pA.w} height={nH} rx="14"
        fill="white" stroke="#d1d5db" strokeWidth="1" />
      <text x={cx(pA)} y={pA.y + 17} textAnchor="middle"
        fontSize="9" fontWeight="600" fill="#374151">PeerA</text>

      {/* PeerB */}
      <rect x={pB.x} y={pB.y} width={pB.w} height={nH} rx="14"
        fill="white" stroke="#d1d5db" strokeWidth="1" />
      <text x={cx(pB)} y={pB.y + 17} textAnchor="middle"
        fontSize="9" fontWeight="600" fill="#374151">PeerB</text>

      {/* A ↔ B */}
      <line x1={pA.x + pA.w + 3} y1={pA.y + nH / 2} x2={pB.x - 3} y2={pB.y + nH / 2}
        stroke="#059669" strokeWidth="1" markerStart="url(#ag2)" markerEnd="url(#ag)" />
      <text x={(cx(pA) + cx(pB)) / 2} y={pA.y + nH + 3} textAnchor="middle"
        fontSize="7.5" fill="#059669">same realm</text>

      {/* Isolation indicator next to C→Bar blocked line */}
      {(() => {
        const lx = (cx(pC) + 18 + cx(sB) + 24) / 2 + 14;
        const ly = (pC.y + nH + sB.y) / 2;
        return (<>
          <text x={lx} y={ly - 4} textAnchor="start"
            fontSize="13" fontWeight="700" fill="#dc2626">✕</text>
          <text x={lx} y={ly + 9} textAnchor="start"
            fontSize="7.5" fill="#dc2626">isolated</text>
          <text x={lx} y={ly + 19} textAnchor="start"
            fontSize="7.5" fill="#dc2626">by default</text>
        </>);
      })()}

      {/* Realm 1003 title */}
      <text x={r3.x + 14} y={r3.y + 20} fontSize="12" fontWeight="700" fill="#9ca3af">
        Realm 1003</text>

      {/* PeerC */}
      <rect x={pC.x} y={pC.y} width={pC.w} height={nH} rx="14"
        fill="white" stroke="#d1d5db" strokeWidth="1" />
      <text x={cx(pC)} y={pC.y + 17} textAnchor="middle"
        fontSize="9" fontWeight="600" fill="#374151">PeerC</text>

      {/* Realm 1002 title */}
      <text x={r2.x + 14} y={r2.y + 20} fontSize="12" fontWeight="700" fill="#9ca3af">
        Realm 1002</text>

      {/* ServiceFoo */}
      <rect x={sF.x} y={sF.y} width={sF.w} height={nH} rx="14"
        fill="white" stroke="#d1d5db" strokeWidth="1" />
      <text x={cx(sF)} y={sF.y + 17} textAnchor="middle"
        fontSize="9" fontWeight="600" fill="#374151">ServiceFoo</text>

      {/* ServiceBar */}
      <rect x={sB.x} y={sB.y} width={sB.w} height={nH} rx="14"
        fill="white" stroke="#d1d5db" strokeWidth="1" />
      <text x={cx(sB)} y={sB.y + 17} textAnchor="middle"
        fontSize="9" fontWeight="600" fill="#374151">ServiceBar</text>

      {/* ACL labels below services */}
      <text x={cx(sF)} y={sF.y + nH + 14} textAnchor="middle"
        fontSize="7.5" fontWeight="600" fill="#0d9488">allow realms: *</text>
      <text x={cx(sF)} y={sF.y + nH + 25} textAnchor="middle"
        fontSize="7" fill="#9ca3af">(public infrastructure)</text>
      <text x={cx(sB)} y={sB.y + nH + 14} textAnchor="middle"
        fontSize="7.5" fontWeight="600" fill="#0d9488">allow realms: [1001]</text>

      {/* ── Enforcement pipeline ── */}
      <line x1={20} y1={310} x2={W - 20} y2={310} stroke="#f1f5f9" strokeWidth="1" />
      <text x={W / 2} y={326} textAnchor="middle"
        fontSize="9" fontWeight="600" fill="#64748b">Enforcement pipeline</text>

      {(() => {
        const pY = 338;
        const bH = 46;
        const gap = 6;
        const stages = [
          { label: "Provision@Admin", desc: "realm_id + secret", color: "#d97706", bg: "#fffbeb" },
          { label: "Admit@AIS", desc: "verify secret → cred", color: "#e11d48", bg: "#fff1f2" },
          { label: "Route@Signaling", desc: "realm + ACL routing", color: "#8b5cf6", bg: "#f5f3ff" },
          { label: "Verify@Actor", desc: "per-message check", color: "#059669", bg: "#ecfdf5" },
          { label: "Relay@TURN", desc: "cred + realm validity", color: "#0891b2", bg: "#ecfeff" },
        ];
        const n = stages.length;
        const bW = (W - 40 - gap * (n - 1)) / n;
        return stages.map((s, i) => {
          const bx = 20 + i * (bW + gap);
          return (
            <g key={i}>
              <rect x={bx} y={pY} width={bW} height={bH} rx="8"
                fill={s.bg} stroke={s.color} strokeWidth="1" />
              <text x={bx + bW / 2} y={pY + 18} textAnchor="middle"
                fontSize="8" fontWeight="700" fill={s.color}>{s.label}</text>
              <text x={bx + bW / 2} y={pY + 32} textAnchor="middle"
                fontSize="7.5" fill={s.color} opacity="0.7">{s.desc}</text>
            </g>
          );
        });
      })()}

      {/* Legend + footnote */}
      <g>
        <line x1={8} y1={4} x2={28} y2={4}
          stroke="#059669" strokeWidth="1" />
        <text x={31} y={7} fontSize="7" fill="#9ca3af">allowed</text>
        <line x1={68} y1={4} x2={88} y2={4}
          stroke="#dc2626" strokeWidth="1" strokeDasharray="4 3" />
        <text x={91} y={7} fontSize="7" fill="#9ca3af">blocked</text>
        {(() => {
          const gap = 6;
          const bW = (W - 40 - gap * 4) / 5;
          const regX = 20 + (bW + gap) + bW / 2;
          return (
            <text x={regX} y={338 + 46 + 12} textAnchor="middle"
              fontSize="7" fill="#d1d5db">
              exists · active · not expired · secret
            </text>
          );
        })()}
      </g>
    </svg>
  );
}

/* ── Realms page ─────────────────────────────────────────────── */

import { ConfirmModal } from "../components/ui/ConfirmModal";
import { RealmSecretModal } from "../components/realms/RealmSecretModal";

const REALM_WRITES_DISABLED_MESSAGE =
  "Realm writes are managed by superv while NodeAdminService gRPC API is enabled.";

export function Realms() {
  const [realms, setRealms] = useState<RealmInfo[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");
  const [realmWritesEnabled, setRealmWritesEnabled] = useState(true);
  const [realmSecretNotice, setRealmSecretNotice] = useState<{
    realmId: number;
    secret: string;
    previousValidUntil?: number | null;
  } | null>(null);
  const [showForm, setShowForm] = useState(false);
  const [editingRealm, setEditingRealm] = useState<RealmInfo | null>(null);
  const [deleteConfirm, setDeleteConfirm] = useState<{ isOpen: boolean; realmId: number | null }>({
    isOpen: false,
    realmId: null,
  });
  const [rotateConfirm, setRotateConfirm] = useState<{ isOpen: boolean; realmId: number | null }>({
    isOpen: false,
    realmId: null,
  });

  const ensureRealmWritesEnabled = useCallback(() => {
    if (realmWritesEnabled) {
      return true;
    }
    setError(REALM_WRITES_DISABLED_MESSAGE);
    return false;
  }, [realmWritesEnabled]);

  const fetchCapabilities = useCallback(async () => {
    try {
      const capabilities = await api.getCapabilities();
      setRealmWritesEnabled(capabilities.realm_writes_enabled);
    } catch (err) {
      setError(err instanceof Error ? err.message : "Failed to load admin capabilities");
    }
  }, []);

  const fetchRealms = useCallback(async () => {
    try {
      const data = await api.listRealms();
      setRealms(data.realms);
      setError("");
    } catch (err) {
      setError(err instanceof Error ? err.message : "Failed to load realms");
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    fetchRealms();
  }, [fetchRealms]);

  useEffect(() => {
    fetchCapabilities();
  }, [fetchCapabilities]);

  useEffect(() => {
    if (realmWritesEnabled) {
      return;
    }
    setShowForm(false);
    setEditingRealm(null);
    setDeleteConfirm({ isOpen: false, realmId: null });
    setRotateConfirm({ isOpen: false, realmId: null });
  }, [realmWritesEnabled]);

  async function handleToggleEnabled(realm: RealmInfo) {
    if (!ensureRealmWritesEnabled()) return;

    try {
      await api.updateRealm(realm.realm_id, { enabled: !realm.enabled });
      await fetchRealms();
    } catch (err) {
      setError(err instanceof Error ? err.message : "Failed to update realm");
    }
  }

  async function handleDeleteClick(realmId: number) {
    if (!ensureRealmWritesEnabled()) return;
    setDeleteConfirm({ isOpen: true, realmId });
  }

  async function handleConfirmDelete() {
    if (!ensureRealmWritesEnabled()) {
      setDeleteConfirm({ isOpen: false, realmId: null });
      return;
    }

    const realmId = deleteConfirm.realmId;
    if (realmId === null) return;

    try {
      await api.deleteRealm(realmId);
      if (realmSecretNotice?.realmId === realmId) {
        setRealmSecretNotice(null);
      }
      setDeleteConfirm({ isOpen: false, realmId: null });
      await fetchRealms();
    } catch (err) {
      setError(err instanceof Error ? err.message : "Failed to delete realm");
      setDeleteConfirm({ isOpen: false, realmId: null });
    }
  }

  async function handleRotateSecretClick(realmId: number) {
    if (!ensureRealmWritesEnabled()) return;
    setRotateConfirm({ isOpen: true, realmId });
  }

  async function handleConfirmRotate() {
    if (!ensureRealmWritesEnabled()) {
      setRotateConfirm({ isOpen: false, realmId: null });
      return;
    }

    const realmId = rotateConfirm.realmId;
    if (realmId === null) return;

    try {
      const resp = await api.rotateRealmSecret(realmId);
      setRealmSecretNotice({
        realmId: resp.realm_id,
        secret: resp.realm_secret,
        previousValidUntil: resp.previous_valid_until,
      });
      setRotateConfirm({ isOpen: false, realmId: null });
      await fetchRealms();
    } catch (err) {
      setError(err instanceof Error ? err.message : "Failed to rotate realm secret");
      setRotateConfirm({ isOpen: false, realmId: null });
    }
  }

  function handleEdit(realm: RealmInfo) {
    if (!ensureRealmWritesEnabled()) return;

    setEditingRealm(realm);
    setShowForm(true);
  }

  function handleFormClose() {
    setShowForm(false);
    setEditingRealm(null);
  }

  async function handleFormSubmit(data: {
    realm_id?: number;
    name: string;
    enabled: boolean;
  }) {
    if (!ensureRealmWritesEnabled()) {
      throw new Error(REALM_WRITES_DISABLED_MESSAGE);
    }

    try {
      if (editingRealm) {
        await api.updateRealm(editingRealm.realm_id, {
          name: data.name,
          enabled: data.enabled,
        });
        handleFormClose();
        await fetchRealms();
        return; // Return void for update
      } else {
        const resp = await api.createRealm(data);
        await fetchRealms(); // Refresh list immediately in background
        return resp; // Return response for secret display
      }
    } catch (err) {
      throw err;
    }
  }

  if (loading) {
    return <div className="text-sm text-gray-500">Loading...</div>;
  }

  return (
    <ServicePageLayout
      title="Realms"
      description="Multi-tenant isolation boundaries for WebRTC actors"
      headerActions={
        <button
          onClick={() => {
            if (ensureRealmWritesEnabled()) {
              setShowForm(true);
            }
          }}
          disabled={!realmWritesEnabled}
          title={realmWritesEnabled ? "Create Realm" : "Managed by superv"}
          className="rounded-lg bg-blue-600 px-4 py-2 text-sm font-medium text-white transition-colors hover:bg-blue-700 disabled:cursor-not-allowed disabled:bg-gray-300 disabled:text-gray-500"
        >
          Create Realm
        </button>
      }
    >
      <HowItWorks storageKey="realms">
        <p className="text-xs text-gray-500 mb-4">
          Each realm is an isolated tenant boundary. Actors within the same realm can discover
          each other and relay media, while cross-realm communication is blocked by default.
          Joining a realm requires a realm secret — issued once at creation and verified by AIS
          on every registration. A service provider can selectively open access to specific
          realms via ACL rules, or
          use <code className="text-[11px] bg-gray-100 px-1 rounded">allow realms: *</code> to
          serve all realms as public infrastructure.
        </p>
        <RealmDiagram />

        <div className="mt-5 space-y-2 text-xs text-gray-500 border-t border-gray-100 pt-4">
          <p className="font-semibold text-gray-600">Key concepts</p>
          <ul className="list-disc pl-4 space-y-1.5">
            <li>
              <strong className="text-gray-600">Default isolation</strong> — actors in different
              realms cannot discover or relay to each other unless the provider explicitly allows it.
            </li>
            <li>
              <strong className="text-gray-600">allow realms: [1001, 1002]</strong> — a service
              provider's ACL can whitelist specific realms. Only actors from listed realms
              gain cross-realm access to that service.
            </li>
            <li>
              <strong className="text-gray-600">allow realms: *</strong> — marks a service as
              public infrastructure, accessible from any realm (e.g. shared SFU, recording service).
            </li>
            <li>
              <strong className="text-gray-600">Mutual consent</strong> — cross-realm access
              requires both sides: the provider must whitelist the consumer's realm,
              and the consumer must declare a dependency on the provider's service type.
            </li>
            <li>
              <strong className="text-gray-600">Realm secret</strong> — every realm has a secret
              issued at creation. Actors must present it at registration; AIS verifies the
              hash before issuing credentials. Secrets can be rotated with a 4h dual-validity window.
            </li>
            <li>
              <strong className="text-gray-600">Lifecycle</strong> — suspended or expired realms
              block all authentication. Actors from inactive realms cannot connect to any service.
            </li>
          </ul>
        </div>
      </HowItWorks>

      {error && (
        <div className="rounded-lg border border-red-200 bg-red-50 p-3 text-sm text-red-700">
          {error}
        </div>
      )}

      {!realmWritesEnabled && (
        <div className="rounded-lg border border-blue-200 bg-blue-50 p-3 text-sm text-blue-800">
          Realm writes are disabled because this node is managed by superv through NodeAdminService.
        </div>
      )}

      {/* realmSecretNotice block removed in favor of modal */}

      <RealmTable
        realms={realms}
        writesEnabled={realmWritesEnabled}
        onToggleEnabled={handleToggleEnabled}
        onEdit={handleEdit}
        onRotateSecret={handleRotateSecretClick}
        onDelete={handleDeleteClick}
      />

      {showForm && (
        <RealmForm
          realm={editingRealm}
          onSubmit={handleFormSubmit}
          onClose={handleFormClose}
          onRefresh={fetchRealms}
        />
      )}

      <ConfirmModal
        isOpen={deleteConfirm.isOpen}
        title="Delete Realm"
        message={
          <span>
            Are you sure you want to delete realm <strong>{deleteConfirm.realmId}</strong>?
            <br />
            This action cannot be undone.
          </span>
        }
        confirmLabel="Delete"
        isDestructive={true}
        onConfirm={handleConfirmDelete}
        onCancel={() => setDeleteConfirm({ isOpen: false, realmId: null })}
      />

      <ConfirmModal
        isOpen={rotateConfirm.isOpen}
        title="Rotate Realm Secret"
        message={
          <span>
            Are you sure you want to rotate the secret for realm <strong>{rotateConfirm.realmId}</strong>?
            <br />
            <br />
            <span className="text-xs text-gray-500 block">
              The old secret will remain valid for 4 hours to allow graceful rotation.
              After confirmation, the new secret will be displayed once.
            </span>
          </span>
        }
        confirmLabel="Rotate Secret"
        cancelLabel="Cancel"
        isDestructive={false}
        onConfirm={handleConfirmRotate}
        onCancel={() => setRotateConfirm({ isOpen: false, realmId: null })}
      />

      {realmSecretNotice && (
        <RealmSecretModal
          isOpen={!!realmSecretNotice}
          realmId={realmSecretNotice.realmId}
          secret={realmSecretNotice.secret}
          previousValidUntil={realmSecretNotice.previousValidUntil}
          onClose={() => setRealmSecretNotice(null)}
        />
      )}
    </ServicePageLayout>
  );
}
