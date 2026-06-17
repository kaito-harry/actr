import type { RealmInfo } from "../../lib/api";
import { cn } from "../../lib/utils";
import { KeyRound, Pencil, Trash2, Clock } from "lucide-react";

interface RealmTableProps {
  realms: RealmInfo[];
  writesEnabled: boolean;
  onToggleEnabled: (realm: RealmInfo) => void;
  onEdit: (realm: RealmInfo) => void;
  onRotateSecret: (realmId: number) => void;
  onDelete: (realmId: number) => void;
}

export function RealmTable({
  realms,
  writesEnabled,
  onToggleEnabled,
  onEdit,
  onRotateSecret,
  onDelete,
}: RealmTableProps) {
  if (realms.length === 0) {
    return (
      <div className="rounded-xl border border-gray-200 bg-white p-8 text-center text-sm text-gray-500">
        {writesEnabled
          ? "No realms yet. Create one to get started."
          : "No realms have been synced from superv yet."}
      </div>
    );
  }

  return (
    <div className="rounded-xl border border-gray-200 bg-white overflow-hidden">
      <table className="w-full text-sm">
        <thead>
          <tr className="border-b border-gray-200 bg-gray-50">
            <th className="px-4 py-3 text-left font-medium text-gray-500">ID</th>
            <th className="px-4 py-3 text-left font-medium text-gray-500">Name</th>
            <th className="px-4 py-3 text-left font-medium text-gray-500">Active</th>
            <th className="px-4 py-3 text-left font-medium text-gray-500">Secret State</th>
            <th className="px-4 py-3 text-left font-medium text-gray-500">Version</th>
            <th className="px-4 py-3 text-right font-medium text-gray-500">Actions</th>
          </tr>
        </thead>
        <tbody className="divide-y divide-gray-100">
          {realms.map((realm) => {
            const isRotating = !!(realm.secret_rotation_state?.previous_valid_until);
            
            return (
            <tr key={realm.realm_id} className="hover:bg-gray-50 transition-colors">
              <td className="px-4 py-3 font-mono text-gray-700">{realm.realm_id}</td>
              <td className="px-4 py-3 text-gray-900">{realm.name}</td>
              <td className="px-4 py-3">
                <button
                  onClick={() => onToggleEnabled(realm)}
                  disabled={!writesEnabled}
                  title={writesEnabled ? (realm.enabled ? "Deactivate" : "Activate") : "Managed by superv"}
                  className={cn(
                    "relative inline-flex h-5 w-9 items-center rounded-full transition-colors disabled:cursor-not-allowed disabled:opacity-50",
                    realm.enabled ? "bg-blue-600" : "bg-gray-300",
                  )}
                >
                  <span
                    className={cn(
                      "inline-block h-3.5 w-3.5 transform rounded-full bg-white transition-transform",
                      realm.enabled ? "translate-x-4" : "translate-x-0.5",
                    )}
                  />
                </button>
              </td>
              <td className="px-4 py-3">
                {isRotating ? (
                  <div className="flex items-center gap-1.5 text-xs text-amber-600 bg-amber-50 px-2 py-1 rounded border border-amber-100 w-fit" title={`Old secret valid until ${new Date((realm.secret_rotation_state?.previous_valid_until || 0) * 1000).toLocaleString()}`}>
                    <Clock className="h-3 w-3" />
                    <span className="font-medium">Rotating</span>
                  </div>
                ) : (
                  <span className="text-xs text-gray-400">Stable</span>
                )}
              </td>
              <td className="px-4 py-3 font-mono text-gray-700">{realm.version}</td>
              <td className="px-4 py-3 text-right">
                <div className="flex items-center justify-end gap-1">
                  <button
                    onClick={() => onEdit(realm)}
                    disabled={!writesEnabled}
                    className="rounded-lg p-1.5 text-gray-400 transition-colors hover:bg-gray-100 hover:text-gray-600 disabled:cursor-not-allowed disabled:opacity-40 disabled:hover:bg-transparent disabled:hover:text-gray-400"
                    title={writesEnabled ? "Edit" : "Managed by superv"}
                  >
                    <Pencil className="h-4 w-4" />
                  </button>
                  <button
                    onClick={() => onRotateSecret(realm.realm_id)}
                    disabled={!writesEnabled}
                    className="rounded-lg p-1.5 text-gray-400 transition-colors hover:bg-amber-50 hover:text-amber-600 disabled:cursor-not-allowed disabled:opacity-40 disabled:hover:bg-transparent disabled:hover:text-gray-400"
                    title={writesEnabled ? "Rotate secret" : "Managed by superv"}
                  >
                    <KeyRound className="h-4 w-4" />
                  </button>
                  <button
                    onClick={() => onDelete(realm.realm_id)}
                    disabled={!writesEnabled}
                    className="rounded-lg p-1.5 text-gray-400 transition-colors hover:bg-red-50 hover:text-red-600 disabled:cursor-not-allowed disabled:opacity-40 disabled:hover:bg-transparent disabled:hover:text-gray-400"
                    title={writesEnabled ? "Delete" : "Managed by superv"}
                  >
                    <Trash2 className="h-4 w-4" />
                  </button>
                </div>
              </td>
            </tr>
            );
          })}
        </tbody>
      </table>
    </div>
  );
}
