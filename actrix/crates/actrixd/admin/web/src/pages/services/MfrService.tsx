import { useState, useEffect, useCallback, useRef } from 'react';
import { Building2, CheckCircle, Clock, XCircle, AlertTriangle, Package, Key, Copy, Plus, Terminal, Download, ShieldOff, Timer } from 'lucide-react';
import { mfrApi, type Manufacturer, type ActrPackage, type ActivateResponse, type ApplyResponse, type MfrKeyHistory } from '../../lib/api';

function copyText(text: string) {
  if (navigator.clipboard?.writeText) {
    navigator.clipboard.writeText(text).catch(() => fallbackCopy(text));
  } else {
    fallbackCopy(text);
  }
}
function fallbackCopy(text: string) {
  const ta = document.createElement('textarea');
  ta.value = text;
  ta.style.position = 'fixed';
  ta.style.opacity = '0';
  document.body.appendChild(ta);
  ta.select();
  document.execCommand('copy');
  document.body.removeChild(ta);
}

function CopyButton({ text, label = 'Copy', className = '' }: { text: string; label?: string; className?: string }) {
  const [copied, setCopied] = useState(false);
  const onClick = () => {
    copyText(text);
    setCopied(true);
    setTimeout(() => setCopied(false), 1500);
  };
  return (
    <button
      onClick={onClick}
      className={`inline-flex items-center gap-1 transition-all duration-150 ${copied ? 'scale-95 opacity-70' : ''} ${className}`}
    >
      <Copy size={12} className={`transition-transform duration-150 ${copied ? 'scale-0' : 'scale-100'}`} />
      <CheckCircle size={12} className={`absolute transition-transform duration-150 ${copied ? 'scale-100 text-green-600' : 'scale-0'}`} />
      <span>{copied ? 'Copied' : label}</span>
    </button>
  );
}

const VERIFY_REPO = 'actr-mfr-verify';
const VERIFY_COOLDOWN_SECS = 15;

// ── Status badge ──────────────────────────────────────────────────

function StatusBadge({ status }: { status: Manufacturer['status'] }) {
  const config = {
    active: { color: 'bg-green-100 text-green-800', icon: CheckCircle, label: 'Active' },
    pending: { color: 'bg-yellow-100 text-yellow-800', icon: Clock, label: 'Pending' },
    suspended: { color: 'bg-orange-100 text-orange-800', icon: AlertTriangle, label: 'Suspended' },
    revoked: { color: 'bg-red-100 text-red-800', icon: XCircle, label: 'Revoked' },
  }[status];
  const Icon = config.icon;
  return (
    <span className={`inline-flex items-center gap-1 px-2 py-0.5 rounded-full text-xs font-medium ${config.color}`}>
      <Icon size={12} />
      {config.label}
    </span>
  );
}

// ── Key Expiry Badge ──────────────────────────────────────────────

function KeyExpiryBadge({ expiresAt }: { expiresAt?: number }) {
  if (!expiresAt) {
    return <span className="text-gray-300 text-xs">—</span>;
  }

  const now = Date.now() / 1000;
  const remaining = expiresAt - now;
  const days = Math.ceil(remaining / 86400);
  const expiryDate = new Date(expiresAt * 1000);
  const dateStr = expiryDate.toLocaleDateString();

  let colorClass: string;
  let label: string;
  let Icon = Timer;

  if (remaining <= 0) {
    // Expired
    colorClass = 'bg-red-600 text-white';
    label = 'Expired';
    Icon = XCircle;
  } else if (days <= 7) {
    // ≤ 7 days — critical
    colorClass = 'bg-red-100 text-red-800 ring-1 ring-red-300';
    label = `${days}d left`;
    Icon = AlertTriangle;
  } else if (days <= 15) {
    // ≤ 15 days — warning
    colorClass = 'bg-orange-100 text-orange-800 ring-1 ring-orange-300';
    label = `${days}d left`;
    Icon = AlertTriangle;
  } else if (days <= 30) {
    // ≤ 30 days — attention
    colorClass = 'bg-yellow-100 text-yellow-800 ring-1 ring-yellow-200';
    label = `${days}d left`;
    Icon = Clock;
  } else {
    // Healthy
    colorClass = 'bg-green-50 text-green-700';
    label = dateStr;
    Icon = CheckCircle;
  }

  return (
    <span
      className={`inline-flex items-center gap-1 px-2 py-0.5 rounded-full text-xs font-medium whitespace-nowrap ${colorClass}`}
      title={`Expires: ${expiryDate.toLocaleString()}${remaining > 0 ? ` (${days} days remaining)` : ' (EXPIRED)'}`}
    >
      <Icon size={11} />
      {label}
    </span>
  );
}

// ── Key History Panel ────────────────────────────────────────────────

function KeyHistoryPanel({ mfr, onRevoked }: { mfr: Manufacturer; onRevoked: () => void }) {
  const [keys, setKeys] = useState<MfrKeyHistory[] | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [revoking, setRevoking] = useState<number | null>(null);

  useEffect(() => {
    setLoading(true);
    mfrApi.listKeys(mfr.id)
      .then(setKeys)
      .catch(e => setError(String(e)))
      .finally(() => setLoading(false));
  }, [mfr.id]);

  const handleRevoke = async (k: MfrKeyHistory) => {
    if (!confirm(`Revoke key ${k.key_id}?\n\nThis is an emergency action. All packages signed with this key will fail verification immediately.`)) return;
    setRevoking(k.id);
    try {
      await mfrApi.revokeHistoricalKey(k.id);
      setKeys(prev => prev ? prev.map(x => x.id === k.id ? { ...x, status: 'revoked' } : x) : prev);
      onRevoked();
    } catch (e) {
      setError(String(e));
    } finally {
      setRevoking(null);
    }
  };

  const ts = (t: number) => new Date(t * 1000).toLocaleDateString();

  return (
    <div className="bg-gray-50 border-t border-gray-100 px-6 py-4">
      <div className="flex items-center gap-2 mb-3">
        <Key size={13} className="text-gray-400" />
        <span className="text-xs font-semibold text-gray-600 uppercase">Key History — {mfr.name}</span>
        <span className="text-xs text-gray-400 ml-1">(Retired keys still verify old packages until manually revoked)</span>
      </div>
      {loading && <div className="text-xs text-gray-400">Loading...</div>}
      {error && <div className="text-xs text-red-600">{error}</div>}
      {!loading && keys !== null && keys.length === 0 && (
        <div className="text-xs text-gray-400">No historical keys.</div>
      )}
      {!loading && keys && keys.length > 0 && (
        <table className="w-full text-xs">
          <thead>
            <tr className="text-gray-400 uppercase">
              <th className="text-left pb-1 pr-4 font-medium">Key ID</th>
              <th className="text-left pb-1 pr-4 font-medium">Status</th>
              <th className="text-left pb-1 pr-4 font-medium">Active from</th>
              <th className="text-left pb-1 pr-4 font-medium">Retired at</th>
              <th className="text-left pb-1 font-medium">Action</th>
            </tr>
          </thead>
          <tbody className="divide-y divide-gray-100">
            {keys.map(k => (
              <tr key={k.id} className="hover:bg-white">
                <td className="py-1.5 pr-4 font-mono text-gray-800">{k.key_id}</td>
                <td className="py-1.5 pr-4">
                  {k.status === 'retired' ? (
                    <span className="inline-flex items-center gap-1 px-1.5 py-0.5 bg-gray-100 rounded text-gray-600">
                      <Clock size={10} /> retired
                    </span>
                  ) : (
                    <span className="inline-flex items-center gap-1 px-1.5 py-0.5 bg-red-100 rounded text-red-700">
                      <XCircle size={10} /> revoked
                    </span>
                  )}
                </td>
                <td className="py-1.5 pr-4 text-gray-500">{ts(k.created_at)}</td>
                <td className="py-1.5 pr-4 text-gray-500">{ts(k.retired_at)}</td>
                <td className="py-1.5">
                  {k.status === 'retired' && (
                    <button
                      onClick={() => void handleRevoke(k)}
                      disabled={revoking === k.id}
                      title="Emergency revoke: packages signed with this key will fail verification"
                      className="inline-flex items-center gap-1 px-2 py-0.5 bg-red-500 text-white rounded text-[11px] hover:bg-red-600 disabled:opacity-50"
                    >
                      <ShieldOff size={10} /> Revoke
                    </button>
                  )}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      )}
    </div>
  );
}

// ── Keychain modal ────────────────────────────────────────────────

function KeychainModal({ response, onClose }: { response: ActivateResponse; onClose: () => void }) {
  const json = JSON.stringify(response, null, 2);
  const name = response.certificate.mfr_name;
  const isGenerated = response.key_source === 'generated';
  const filename = `mfr-${name}-keychain.json`;
  const saveCommand = `mkdir -p ~/.config/actrix && cat > ~/.config/actrix/${filename} << 'KEYCHAIN'\n${json}\nKEYCHAIN`;

  const handleDownload = () => {
    const blob = new Blob([json], { type: 'application/json' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = filename;
    a.click();
    URL.revokeObjectURL(url);
  };

  return (
    <div className="fixed inset-0 bg-black/50 flex items-center justify-center z-50">
      <div className="bg-white rounded-xl shadow-xl p-6 max-w-2xl w-full mx-4">
        <div className="flex items-center gap-2 mb-4">
          <Key className="text-amber-500" size={20} />
          <h2 className="text-lg font-semibold">
            {isGenerated ? 'MFR Keychain Issued' : 'MFR Certificate Issued'}
          </h2>
          <span className={`ml-auto text-xs px-2 py-0.5 rounded-full font-medium ${
            isGenerated ? 'bg-amber-100 text-amber-800' : 'bg-teal-100 text-teal-800'
          }`}>
            {isGenerated ? 'Server Generated' : 'Key Uploaded'}
          </span>
        </div>

        <div className="mb-4">
          <label className="block text-xs text-gray-500 uppercase font-semibold mb-1">Key ID (Fingerprint)</label>
          <div className="flex items-center gap-2">
            <code className="text-sm bg-gray-100 border border-gray-200 rounded px-2 py-1 text-gray-800">{response.certificate.key_id}</code>
            <CopyButton text={response.certificate.key_id} label="Copy ID" className="text-xs text-gray-500 hover:text-gray-700" />
          </div>
        </div>

        {isGenerated ? (
          <div className="bg-amber-50 border border-amber-200 rounded-lg p-3 mb-4 text-sm text-amber-800">
            <p className="font-medium">Save this private key now. It will NOT be shown again.</p>
            <p className="mt-1 text-amber-700">The platform only stores your public key for signature verification. Your private key is never stored, logged, or backed up — if lost, it cannot be recovered.</p>
          </div>
        ) : (
          <div className="bg-teal-50 border border-teal-200 rounded-lg p-3 mb-4 text-sm text-teal-800">
            Your uploaded public key has been registered. Use your own private key to sign packages.
          </div>
        )}

        {/* One-liner save command (only for generated mode) */}
        {isGenerated && (
          <div className="mb-4">
            <div className="flex items-center gap-2 mb-1">
              <Terminal size={12} className="text-gray-400" />
              <label className="text-xs text-gray-500">Save to ~/.config/actrix/</label>
              <CopyButton text={saveCommand} className="text-xs text-blue-600 hover:text-blue-800 ml-auto relative" />
            </div>
            <pre className="bg-gray-900 text-green-400 rounded-lg p-3 text-xs overflow-x-auto whitespace-pre-wrap font-mono max-h-40">{saveCommand}</pre>
          </div>
        )}

        {/* Raw JSON (collapsed) */}
        <details className="mb-4">
          <summary className="text-xs text-gray-500 cursor-pointer hover:text-gray-700">Raw JSON</summary>
          <pre className="bg-gray-900 text-green-400 rounded-lg p-3 text-xs overflow-auto max-h-40 font-mono mt-1">{json}</pre>
        </details>

        <div className="flex gap-2">
          <CopyButton
            text={json}
            label="Copy JSON"
            className="relative flex items-center gap-2 px-4 py-2 bg-gray-800 text-white rounded-lg text-sm hover:bg-gray-700"
          />
          <button
            onClick={handleDownload}
            className="flex items-center gap-2 px-4 py-2 border border-gray-300 rounded-lg text-sm hover:bg-gray-50"
          >
            <Download size={14} /> Download
          </button>
          <button
            onClick={onClose}
            className="ml-auto px-4 py-2 border border-gray-300 rounded-lg text-sm hover:bg-gray-50"
          >
            Close
          </button>
        </div>
      </div>
    </div>
  );
}

// ── How it works diagram ──────────────────────────────────────────

function HowItWorks() {
  return (
    <div className="bg-white rounded-xl border border-gray-200 overflow-hidden">
      <details>
        <summary className="px-4 py-3 cursor-pointer hover:bg-gray-50 text-sm font-semibold text-gray-800 select-none">
          How it Works
        </summary>
        <div className="px-4 pb-5 space-y-5">
          {/* Row 1: Registration & Identity */}
          <div>
            <div className="text-xs font-medium text-gray-500 mb-2">Phase 1: MFR Registration & Identity</div>
            <svg viewBox="0 0 760 145" className="w-full" xmlns="http://www.w3.org/2000/svg">
              <defs>
                <linearGradient id="g1" x1="0" y1="0" x2="0" y2="1"><stop offset="0%" stopColor="#f8fafc"/><stop offset="100%" stopColor="#e2e8f0"/></linearGradient>
                <linearGradient id="g2" x1="0" y1="0" x2="0" y2="1"><stop offset="0%" stopColor="#fefce8"/><stop offset="100%" stopColor="#fef08a"/></linearGradient>
                <linearGradient id="g3" x1="0" y1="0" x2="0" y2="1"><stop offset="0%" stopColor="#eff6ff"/><stop offset="100%" stopColor="#bfdbfe"/></linearGradient>
                <linearGradient id="g4" x1="0" y1="0" x2="0" y2="1"><stop offset="0%" stopColor="#f0fdf4"/><stop offset="100%" stopColor="#bbf7d0"/></linearGradient>
                <filter id="ds"><feDropShadow dx="0" dy="1" stdDeviation="2" floodOpacity="0.1"/></filter>
              </defs>

              {/* Person icon */}
              <g transform="translate(30,12)" filter="url(#ds)">
                <rect width="100" height="90" rx="12" fill="url(#g1)" stroke="#cbd5e1" strokeWidth="1"/>
                <circle cx="50" cy="32" r="14" fill="#94a3b8" />
                <circle cx="50" cy="28" r="6" fill="#f1f5f9" />
                <ellipse cx="50" cy="42" rx="10" ry="6" fill="#f1f5f9" />
                <text x="50" y="72" textAnchor="middle" fontSize="10" fontWeight="600" fill="#475569">Register</text>
                <text x="50" y="84" textAnchor="middle" fontSize="8" fill="#94a3b8">GitHub name</text>
              </g>

              {/* Register desc */}
              <text x="80" y="116" textAnchor="middle" fontSize="7.5" fill="#94a3b8">Account or Org</text>

              {/* Curved arrow 1 */}
              <path d="M138,57 C158,57 158,57 178,57" fill="none" stroke="#cbd5e1" strokeWidth="1.5" strokeDasharray="4 2"/>
              <polygon points="176,53 184,57 176,61" fill="#cbd5e1"/>

              {/* Token / Challenge */}
              <g transform="translate(190,12)" filter="url(#ds)">
                <rect width="100" height="90" rx="12" fill="url(#g2)" stroke="#fbbf24" strokeWidth="1"/>
                {/* Shield with star */}
                <path d="M50,22 L62,28 L62,40 C62,48 50,54 50,54 C50,54 38,48 38,40 L38,28 Z" fill="#fbbf24" opacity="0.3"/>
                <path d="M50,22 L62,28 L62,40 C62,48 50,54 50,54 C50,54 38,48 38,40 L38,28 Z" fill="none" stroke="#f59e0b" strokeWidth="1.2"/>
                <text x="50" y="41" textAnchor="middle" fontSize="12" fill="#b45309">?</text>
                <text x="50" y="72" textAnchor="middle" fontSize="10" fontWeight="600" fill="#92400e">Challenge</text>
                <text x="50" y="84" textAnchor="middle" fontSize="8" fill="#b45309">Unique token</text>
              </g>

              {/* Challenge desc */}
              <text x="240" y="116" textAnchor="middle" fontSize="7.5" fill="#94a3b8">{'actrix-verify={…} · 24h'}</text>

              {/* Curved arrow 2 */}
              <path d="M298,57 C318,57 318,57 338,57" fill="none" stroke="#cbd5e1" strokeWidth="1.5" strokeDasharray="4 2"/>
              <polygon points="336,53 344,57 336,61" fill="#cbd5e1"/>

              {/* GitHub repo */}
              <g transform="translate(350,12)" filter="url(#ds)">
                <rect width="100" height="90" rx="12" fill="url(#g3)" stroke="#60a5fa" strokeWidth="1"/>
                {/* GitHub-like mark */}
                <circle cx="50" cy="34" r="14" fill="#1e40af" opacity="0.15"/>
                {/* Simplified octocat silhouette - a circle with tentacles hint */}
                <circle cx="50" cy="31" r="8" fill="none" stroke="#3b82f6" strokeWidth="1.5"/>
                <path d="M44,35 C44,41 56,41 56,35" fill="none" stroke="#3b82f6" strokeWidth="1.2"/>
                <circle cx="47" cy="30" r="1.5" fill="#3b82f6"/>
                <circle cx="53" cy="30" r="1.5" fill="#3b82f6"/>
                <text x="50" y="72" textAnchor="middle" fontSize="10" fontWeight="600" fill="#1e40af">Prove</text>
                <text x="50" y="84" textAnchor="middle" fontSize="8" fill="#3b82f6">Public repo</text>
              </g>

              {/* Prove desc */}
              <text x="400" y="116" textAnchor="middle" fontSize="7.5" fill="#94a3b8">{'actr-mfr-verify/{domain}.txt'}</text>

              {/* Curved arrow 3 */}
              <path d="M458,57 C478,57 478,57 498,57" fill="none" stroke="#cbd5e1" strokeWidth="1.5" strokeDasharray="4 2"/>
              <polygon points="496,53 504,57 496,61" fill="#cbd5e1"/>

              {/* Verify + issue */}
              <g transform="translate(510,12)" filter="url(#ds)">
                <rect width="100" height="90" rx="12" fill="#f0fdf4" stroke="#4ade80" strokeWidth="1"/>
                <circle cx="50" cy="33" r="13" fill="#22c55e" opacity="0.15"/>
                <path d="M42,33 L48,39 L58,27" fill="none" stroke="#16a34a" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round"/>
                <text x="50" y="72" textAnchor="middle" fontSize="10" fontWeight="600" fill="#166534">Verified</text>
                <text x="50" y="84" textAnchor="middle" fontSize="8" fill="#16a34a">Server confirms</text>
              </g>

              {/* Verified desc */}
              <text x="560" y="116" textAnchor="middle" fontSize="7.5" fill="#94a3b8">via GitHub API</text>

              {/* Arrow 4 */}
              <path d="M618,57 C638,57 638,57 658,57" fill="none" stroke="#22c55e" strokeWidth="1.5"/>
              <polygon points="656,53 664,57 656,61" fill="#22c55e"/>

              {/* Key issued */}
              <g transform="translate(670,12)" filter="url(#ds)">
                <rect width="80" height="90" rx="12" fill="url(#g4)" stroke="#22c55e" strokeWidth="1.5"/>
                {/* Key icon */}
                <circle cx="40" cy="30" r="8" fill="none" stroke="#f59e0b" strokeWidth="2"/>
                <circle cx="40" cy="30" r="3" fill="#f59e0b" opacity="0.3"/>
                <line x1="48" y1="30" x2="58" y2="30" stroke="#f59e0b" strokeWidth="2" strokeLinecap="round"/>
                <line x1="54" y1="30" x2="54" y2="35" stroke="#f59e0b" strokeWidth="2" strokeLinecap="round"/>
                <line x1="58" y1="30" x2="58" y2="35" stroke="#f59e0b" strokeWidth="2" strokeLinecap="round"/>
                <text x="40" y="58" textAnchor="middle" fontSize="9" fontWeight="700" fill="#166534">Keychain</text>
                <text x="40" y="70" textAnchor="middle" fontSize="7.5" fill="#16a34a">Ed25519</text>
                <text x="40" y="80" textAnchor="middle" fontSize="7.5" fill="#16a34a">keypair</text>
              </g>

              {/* Keychain desc */}
              <text x="710" y="116" textAnchor="middle" fontSize="7.5" fill="#94a3b8">+ Certificate · 365 days</text>

            </svg>
          </div>

          {/* Row 2: Phase 2 — Build + Sign */}
          <div>
            <div className="text-xs font-medium text-gray-500 mb-2">Phase 2: actr pkg build — SHA-256 + Ed25519 Sign</div>
            <svg viewBox="0 0 760 210" className="w-full" xmlns="http://www.w3.org/2000/svg" fontFamily="system-ui, sans-serif">
              <defs>
                <marker id="ar2a" viewBox="0 0 10 7" refX="9" refY="3.5" markerWidth="7" markerHeight="5" orient="auto">
                  <path d="M0,0 L10,3.5 L0,7Z" fill="#3b82f6"/>
                </marker>
                <marker id="ar2a-amber" viewBox="0 0 10 7" refX="9" refY="3.5" markerWidth="7" markerHeight="5" orient="auto">
                  <path d="M0,0 L10,3.5 L0,7Z" fill="#f59e0b"/>
                </marker>
              </defs>

              {/* ── Developer ── */}
              <g transform="translate(10,20)">
                <circle cx="22" cy="12" r="10" fill="#bfdbfe"/>
                <circle cx="22" cy="9" r="5" fill="#eff6ff"/>
                <ellipse cx="22" cy="18" rx="7" ry="4" fill="#eff6ff"/>
                <rect x="10" y="30" width="24" height="16" rx="2" fill="#93c5fd" stroke="#3b82f6" strokeWidth="0.8"/>
                <rect x="13" y="33" width="18" height="10" rx="1" fill="#eff6ff"/>
                <rect x="6" y="46" width="32" height="3" rx="1.5" fill="#60a5fa"/>
              </g>
              <text x="32" y="92" textAnchor="middle" fontSize="9" fill="#1e40af" fontWeight="600">Dev</text>

              {/* Arrow: dev → inputs */}
              <line x1="54" y1="55" x2="88" y2="55" stroke="#3b82f6" strokeWidth="1.5" markerEnd="url(#ar2a)"/>
              <text x="71" y="48" textAnchor="middle" fontSize="7" fill="#1e40af">actr pkg build</text>

              {/* ── Inputs: binary + proto + actr.toml ── */}
              <g transform="translate(92,22)">
                <rect width="92" height="66" rx="6" fill="#f0f9ff" stroke="#93c5fd" strokeWidth="1"/>
                <text x="46" y="14" textAnchor="middle" fontSize="8" fontWeight="600" fill="#1e40af">Inputs</text>
                <rect x="6" y="19" width="80" height="12" rx="2" fill="#dbeafe"/>
                <text x="46" y="28" textAnchor="middle" fontSize="6.5" fill="#1e40af">binary(wasm/so)</text>
                <rect x="6" y="34" width="80" height="12" rx="2" fill="#dbeafe"/>
                <text x="46" y="43" textAnchor="middle" fontSize="7" fill="#1e40af">*.proto files</text>
                <rect x="6" y="49" width="80" height="12" rx="2" fill="#e0e7ff"/>
                <text x="46" y="58" textAnchor="middle" fontSize="7" fill="#3730a3">actr.toml</text>
              </g>

              {/* Arrow: inputs → SHA-256 */}
              <line x1="184" y1="55" x2="218" y2="55" stroke="#3b82f6" strokeWidth="1.5" markerEnd="url(#ar2a)"/>

              {/* ── SHA-256 Hash ── */}
              <g transform="translate(220,35)">
                <rect width="100" height="40" rx="6" fill="#e0e7ff" stroke="#6366f1" strokeWidth="1.2"/>
                <text x="50" y="18" textAnchor="middle" fontSize="9" fontWeight="600" fill="#3730a3">SHA-256</text>
                <text x="50" y="30" textAnchor="middle" fontSize="7" fill="#4f46e5">binary + proto hash</text>
              </g>
              <text x="270" y="90" textAnchor="middle" fontSize="7" fill="#6366f1">hash written to manifest</text>

              {/* Arrow: SHA-256 → Sign */}
              <line x1="320" y1="55" x2="374" y2="55" stroke="#3b82f6" strokeWidth="1.5" markerEnd="url(#ar2a)"/>
              <text x="347" y="48" textAnchor="middle" fontSize="7" fill="#1e40af">manifest bytes</text>

              {/* ── Ed25519 Sign ── */}
              <g transform="translate(376,35)">
                <rect width="110" height="40" rx="6" fill="#fef3c7" stroke="#f59e0b" strokeWidth="1.2"/>
                <text x="55" y="18" textAnchor="middle" fontSize="9" fontWeight="600" fill="#92400e">Ed25519 Sign</text>
                <text x="55" y="30" textAnchor="middle" fontSize="7" fill="#b45309">actr.toml bytes</text>
              </g>

              {/* Private Key → Sign */}
              <g transform="translate(416,90)">
                <circle cx="12" cy="12" r="8" fill="none" stroke="#f59e0b" strokeWidth="1.5"/>
                <circle cx="12" cy="12" r="3" fill="#fbbf24" opacity="0.4"/>
                <line x1="20" y1="12" x2="32" y2="12" stroke="#f59e0b" strokeWidth="1.5" strokeLinecap="round"/>
                <line x1="26" y1="12" x2="26" y2="17" stroke="#f59e0b" strokeWidth="1.5" strokeLinecap="round"/>
                <line x1="30" y1="12" x2="30" y2="17" stroke="#f59e0b" strokeWidth="1.5" strokeLinecap="round"/>
              </g>
              <text x="432" y="124" textAnchor="middle" fontSize="8" fill="#b45309" fontWeight="600">MFR Keychain</text>
              <text x="432" y="132" textAnchor="middle" fontSize="6.5" fill="#d97706">(Private Key)</text>
              <line x1="432" y1="90" x2="432" y2="80" stroke="#f59e0b" strokeWidth="1.2" markerEnd="url(#ar2a-amber)"/>

              {/* Arrow: Sign → .actr output */}
              <line x1="486" y1="55" x2="538" y2="55" stroke="#3b82f6" strokeWidth="1.5" markerEnd="url(#ar2a)"/>
              <text x="512" y="48" textAnchor="middle" fontSize="7" fill="#1e40af">64 bytes sig</text>

              {/* ── .actr Package (ZIP) ── */}
              <g transform="translate(540,16)">
                <rect width="120" height="78" rx="8" fill="#f0fdf4" stroke="#16a34a" strokeWidth="1.5"/>
                <text x="60" y="16" textAnchor="middle" fontSize="10" fontWeight="700" fill="#166534">.actr Package</text>
                <rect x="8" y="22" width="104" height="13" rx="2" fill="#dcfce7"/>
                <text x="60" y="32" textAnchor="middle" fontSize="7" fill="#166534">actr.toml - manifest</text>
                <rect x="8" y="37" width="104" height="13" rx="2" fill="#fef9c3"/>
                <text x="60" y="47" textAnchor="middle" fontSize="7" fill="#92400e">actr.sig - 64 bytes Ed25519</text>
                <rect x="8" y="52" width="50" height="13" rx="2" fill="#dbeafe"/>
                <text x="33" y="62" textAnchor="middle" fontSize="6.5" fill="#1e40af">bin/ (wasm/so)</text>
                <rect x="62" y="52" width="50" height="13" rx="2" fill="#e0e7ff"/>
                <text x="87" y="62" textAnchor="middle" fontSize="7" fill="#3730a3">proto/*.proto</text>
                <text x="60" y="75" textAnchor="middle" fontSize="6" fill="#94a3b8">ZIP STORE format</text>
              </g>

              {/* Signing chain summary at bottom */}
              <g transform="translate(92,144)">
                <rect width="566" height="56" rx="6" fill="#fafafa" stroke="#e5e7eb" strokeWidth="1" strokeDasharray="4 2"/>
                <text x="283" y="14" textAnchor="middle" fontSize="8" fontWeight="600" fill="#6b7280">Signing Chain</text>
                <text x="283" y="28" textAnchor="middle" fontSize="7" fill="#9ca3af">binary bytes -{`>`} SHA-256 -{`>`} actr.toml binary.hash</text>
                <text x="283" y="38" textAnchor="middle" fontSize="7" fill="#9ca3af">proto bytes  -{`>`} SHA-256 -{`>`} actr.toml proto_files.hash</text>
                <text x="283" y="48" textAnchor="middle" fontSize="7" fill="#9ca3af">actr.toml bytes -{`>`} Ed25519 Sign -{`>`} actr.sig 64 bytes</text>
              </g>
            </svg>
          </div>

          {/* Row 3: Phase 3 — Publish with Nonce Challenge-Response */}
          <div>
            <div className="text-xs font-medium text-gray-500 mb-2">Phase 3: actr pkg publish — Nonce Challenge-Response + Dual Verify</div>
            <svg viewBox="0 0 760 270" className="w-full" xmlns="http://www.w3.org/2000/svg" fontFamily="system-ui, sans-serif">
              <defs>
                <marker id="ar2b" viewBox="0 0 10 7" refX="9" refY="3.5" markerWidth="7" markerHeight="5" orient="auto">
                  <path d="M0,0 L10,3.5 L0,7Z" fill="#3b82f6"/>
                </marker>
                <marker id="ar2b-green" viewBox="0 0 10 7" refX="9" refY="3.5" markerWidth="7" markerHeight="5" orient="auto">
                  <path d="M0,0 L10,3.5 L0,7Z" fill="#16a34a"/>
                </marker>
                <marker id="ar2b-gray" viewBox="0 0 10 7" refX="9" refY="3.5" markerWidth="7" markerHeight="5" orient="auto">
                  <path d="M0,0 L10,3.5 L0,7Z" fill="#94a3b8"/>
                </marker>
                <marker id="ar2b-amber" viewBox="0 0 10 7" refX="9" refY="3.5" markerWidth="7" markerHeight="5" orient="auto">
                  <path d="M0,0 L10,3.5 L0,7Z" fill="#f59e0b"/>
                </marker>
              </defs>

              {/* ====== LEFT: CLI Side ====== */}
              <text x="120" y="18" textAnchor="middle" fontSize="13" fontWeight="700" fill="#3b82f6">actr CLI</text>

              {/* Step 1: Request Nonce */}
              <g transform="translate(20,28)">
                <rect width="200" height="32" rx="6" fill="#dbeafe" stroke="#2563eb" strokeWidth="1.2"/>
                <text x="100" y="14" textAnchor="middle" fontSize="8" fontWeight="600" fill="#1d4ed8">1. POST /mfr/pkg/nonce</text>
                <text x="100" y="26" textAnchor="middle" fontSize="7" fill="#3b82f6">manufacturer name</text>
              </g>

              {/* Arrow: CLI → MFR (nonce request) */}
              <line x1="220" y1="44" x2="430" y2="44" stroke="#3b82f6" strokeWidth="1.5" markerEnd="url(#ar2b)"/>

              {/* Arrow: MFR → CLI (nonce response) */}
              <line x1="430" y1="58" x2="220" y2="58" stroke="#94a3b8" strokeWidth="1.2" strokeDasharray="4 2" markerEnd="url(#ar2b-gray)"/>
              <text x="325" y="54" textAnchor="middle" fontSize="7" fill="#94a3b8">nonce base64 32 bytes</text>

              {/* Step 2: Build nonce_sig */}
              <g transform="translate(20,76)">
                <rect width="200" height="56" rx="6" fill="#fef3c7" stroke="#f59e0b" strokeWidth="1.2"/>
                {/* MFR Keychain icon inside box */}
                <g transform="translate(20,12)">
                  <circle cx="12" cy="12" r="8" fill="none" stroke="#f59e0b" strokeWidth="1.5"/>
                  <circle cx="12" cy="12" r="3" fill="#fbbf24" opacity="0.4"/>
                  <line x1="20" y1="12" x2="32" y2="12" stroke="#f59e0b" strokeWidth="1.5" strokeLinecap="round"/>
                  <line x1="26" y1="12" x2="26" y2="17" stroke="#f59e0b" strokeWidth="1.5" strokeLinecap="round"/>
                  <line x1="30" y1="12" x2="30" y2="17" stroke="#f59e0b" strokeWidth="1.5" strokeLinecap="round"/>
                  <text x="16" y="34" textAnchor="middle" fontSize="7" fill="#b45309" fontWeight="600">MFR Keychain</text>
                </g>
                <text x="130" y="14" textAnchor="middle" fontSize="8" fontWeight="600" fill="#92400e">2. Sign Nonce Payload</text>
                <text x="130" y="26" textAnchor="middle" fontSize="7" fill="#b45309">ACTR-PUBLISH-V1</text>
                <text x="130" y="36" textAnchor="middle" fontSize="7" fill="#b45309">mfr + method + path</text>
                <text x="130" y="46" textAnchor="middle" fontSize="7" fill="#b45309">+ nonce_hex + body_sha256</text>
              </g>

              {/* Step 3: POST publish */}
              <g transform="translate(20,146)">
                <rect width="200" height="48" rx="6" fill="#dbeafe" stroke="#2563eb" strokeWidth="1.2"/>
                <text x="100" y="14" textAnchor="middle" fontSize="8" fontWeight="600" fill="#1d4ed8">3. POST /mfr/pkg/publish</text>
                <text x="100" y="26" textAnchor="middle" fontSize="7" fill="#3b82f6">manifest + actr.sig + proto_files</text>
                <text x="100" y="36" textAnchor="middle" fontSize="7" fill="#3b82f6">+ nonce + nonce_sig</text>
              </g>

              {/* Arrow: CLI → MFR (publish) */}
              <line x1="220" y1="170" x2="430" y2="170" stroke="#3b82f6" strokeWidth="1.5" markerEnd="url(#ar2b)"/>

              {/* ====== RIGHT: MFR Service Side ====== */}
              <text x="560" y="18" textAnchor="middle" fontSize="13" fontWeight="700" fill="#16a34a">MFR Service</text>

              {/* Nonce generation */}
              <g transform="translate(440,28)">
                <rect width="240" height="32" rx="6" fill="#f0fdf4" stroke="#16a34a" strokeWidth="1"/>
                <text x="120" y="14" textAnchor="middle" fontSize="8" fontWeight="600" fill="#166534">Generate Nonce</text>
                <text x="120" y="26" textAnchor="middle" fontSize="7" fill="#16a34a">Store pending nonce in DB (TTL 5m)</text>
              </g>

              {/* MFR verification pipeline */}
              <g transform="translate(440,76)">
                <rect width="240" height="126" rx="8" fill="#fafafa" stroke="#d1d5db" strokeWidth="1"/>
                <text x="120" y="18" textAnchor="middle" fontSize="9" fontWeight="700" fill="#374151">Verification Pipeline</text>

                <g transform="translate(10,26)">
                  <rect width="220" height="22" rx="4" fill="#fef9c3" stroke="#ca8a04" strokeWidth="0.8"/>
                  <text x="110" y="15" textAnchor="middle" fontSize="7" fontWeight="600" fill="#854d0e">Validate Active MFR Identity &amp; Nonce State</text>
                </g>

                <g transform="translate(10,54)">
                  <rect width="220" height="22" rx="4" fill="#fef3c7" stroke="#f59e0b" strokeWidth="0.8"/>
                  <text x="110" y="15" textAnchor="middle" fontSize="7" fontWeight="600" fill="#92400e">Dual Verify: nonce_sig + manifest actr.sig</text>
                </g>

                <g transform="translate(10,82)">
                  <rect width="220" height="30" rx="4" fill="#f0fdf4" stroke="#16a34a" strokeWidth="0.8"/>
                  <text x="110" y="13" textAnchor="middle" fontSize="7" fontWeight="600" fill="#166534">Atomic Consume Nonce</text>
                  <text x="110" y="24" textAnchor="middle" fontSize="6" fill="#16a34a">Cross-validate TOML vs request</text>
                </g>
              </g>

              {/* Step arrows within pipeline */}
              <line x1="560" y1="124" x2="560" y2="130" stroke="#94a3b8" strokeWidth="0.8"/>
              <line x1="560" y1="152" x2="560" y2="158" stroke="#94a3b8" strokeWidth="0.8"/>

              {/* ====== BOTTOM: Result ====== */}

              {/* DB insert */}
              <g transform="translate(440,216)">
                <rect width="240" height="36" rx="6" fill="#dbeafe" stroke="#2563eb" strokeWidth="1.2"/>
                <text x="120" y="14" textAnchor="middle" fontSize="8" fontWeight="600" fill="#1d4ed8">Save Package Metadata</text>
                <text x="120" y="28" textAnchor="middle" fontSize="7" fill="#3b82f6">type_str + manifest + sig + proto_files</text>
              </g>

              {/* Arrow: pipeline → DB */}
              <line x1="560" y1="202" x2="560" y2="216" stroke="#16a34a" strokeWidth="1.5" markerEnd="url(#ar2b-green)"/>

              {/* Arrow: MFR → CLI (success response) */}
              <line x1="440" y1="234" x2="220" y2="234" stroke="#16a34a" strokeWidth="1.5" markerEnd="url(#ar2b-green)"/>
              <text x="330" y="230" textAnchor="middle" fontSize="8" fill="#16a34a" fontWeight="600">Published: mfr:name:version</text>

              {/* CLI receives result */}
              <g transform="translate(20,220)">
                <rect width="200" height="28" rx="6" fill="#d1fae5" stroke="#16a34a" strokeWidth="1.2"/>
                <text x="100" y="18" textAnchor="middle" fontSize="9" fontWeight="600" fill="#166534">Publish Success</text>
              </g>

            </svg>
          </div>

          {/* Row 4: MFR Key Rotation */}
          <div className="mt-8 relative pt-4 before:absolute before:inset-x-8 before:top-0 before:h-px before:bg-gradient-to-r before:from-transparent before:via-gray-200 before:to-transparent">
            <div className="text-xs font-medium text-gray-500 mb-2">Phase 4: MFR Key Rotation (Backward Compatible)</div>
            <svg viewBox="0 0 760 170" className="w-full" xmlns="http://www.w3.org/2000/svg" fontFamily="system-ui, sans-serif">
              <defs>
                <marker id="ar4b" viewBox="0 0 10 7" refX="9" refY="3.5" markerWidth="7" markerHeight="5" orient="auto">
                  <path d="M0,0 L10,3.5 L0,7Z" fill="#4f46e5"/>
                </marker>
                <marker id="ar4b-green" viewBox="0 0 10 7" refX="9" refY="3.5" markerWidth="7" markerHeight="5" orient="auto">
                  <path d="M0,0 L10,3.5 L0,7Z" fill="#16a34a"/>
                </marker>
                <marker id="ar4b-gray" viewBox="0 0 10 7" refX="9" refY="3.5" markerWidth="7" markerHeight="5" orient="auto">
                  <path d="M0,0 L10,3.5 L0,7Z" fill="#94a3b8"/>
                </marker>
              </defs>

               {/* Left: Admin */}
               <text x="120" y="18" textAnchor="middle" fontSize="13" fontWeight="700" fill="#4f46e5">MFR Admin</text>

               {/* Right: MFR */}
               <text x="560" y="18" textAnchor="middle" fontSize="13" fontWeight="700" fill="#16a34a">MFR Service / DB</text>

               {/* Step 1: POST renew */}
               <g transform="translate(20,40)">
                 <rect width="200" height="36" rx="6" fill="#e0e7ff" stroke="#4f46e5" strokeWidth="1.2"/>
                 <text x="100" y="16" textAnchor="middle" fontSize="8" fontWeight="600" fill="#312e81">1. POST /mfr/admin/{`{id}`}/renew</text>
                 <text x="100" y="28" textAnchor="middle" fontSize="7" fill="#4f46e5">(Optional: Provide new public key)</text>
               </g>

               {/* Arrow Request */}
               <line x1="220" y1="58" x2="430" y2="58" stroke="#4f46e5" strokeWidth="1.5" markerEnd="url(#ar4b)"/>

               {/* Archive Old Key */}
               <g transform="translate(440,30)">
                 <rect width="240" height="32" rx="6" fill="#fef3c7" stroke="#d97706" strokeWidth="1.2" strokeDasharray="4 2"/>
                 <text x="120" y="14" textAnchor="middle" fontSize="8" fontWeight="600" fill="#92400e">2. Archive Current Key</text>
                 <text x="120" y="26" textAnchor="middle" fontSize="7" fill="#b45309">Save to key history (status = retired)</text>
               </g>

               {/* Step Arrow */}
               <line x1="560" y1="62" x2="560" y2="76" stroke="#94a3b8" strokeWidth="1" markerEnd="url(#ar4b-gray)"/>

               {/* Set New Key */}
               <g transform="translate(440,76)">
                 <rect width="240" height="32" rx="6" fill="#dcfce7" stroke="#16a34a" strokeWidth="1.2"/>
                 <text x="120" y="14" textAnchor="middle" fontSize="8" fontWeight="600" fill="#166534">3. Activate New Key</text>
                 <text x="120" y="26" textAnchor="middle" fontSize="7" fill="#15803d">Update MFR public_key &amp; key_id</text>
               </g>

               {/* Step Arrow */}
               <line x1="560" y1="108" x2="560" y2="122" stroke="#94a3b8" strokeWidth="1" markerEnd="url(#ar4b-gray)"/>

               {/* Success Response handling */}
               <g transform="translate(440,122)">
                 <rect width="240" height="28" rx="6" fill="#f0fdf4" stroke="#16a34a" strokeWidth="1"/>
                 <text x="120" y="18" textAnchor="middle" fontSize="8" fontWeight="600" fill="#166534">Return new mfr-keychain.json</text>
               </g>

               {/* Arrow Response */}
               <line x1="440" y1="136" x2="220" y2="136" stroke="#16a34a" strokeWidth="1.5" markerEnd="url(#ar4b-green)"/>
               <text x="330" y="132" textAnchor="middle" fontSize="8" fill="#16a34a" fontWeight="600">ActivateResponse</text>

               {/* Admin Receives */}
               <g transform="translate(20,118)">
                 <rect width="200" height="36" rx="6" fill="#d1fae5" stroke="#10b981" strokeWidth="1.2"/>
                 <text x="100" y="16" textAnchor="middle" fontSize="8" fontWeight="600" fill="#065f46">Saved: New MFR Keychain</text>
                 <text x="100" y="28" textAnchor="middle" fontSize="7" fill="#047857">Old keys still valid for old packages</text>
               </g>
            </svg>
          </div>
        </div>
      </details>
    </div>
  );
}

// ── Create modal (3-step) ─────────────────────────────────────────

type CreateStep = 'input' | 'verify' | 'done';

function CreateModal({
  onClose,
  onDone,
  resumeMfr,
}: {
  onClose: () => void;
  onDone: (response: ActivateResponse) => void;
  resumeMfr?: Manufacturer;
}) {
  const [step, setStep] = useState<CreateStep>(resumeMfr ? 'verify' : 'input');
  const [name, setName] = useState(resumeMfr?.name ?? '');
  const [contact, setContact] = useState('');
  const [loading, setLoading] = useState(!!resumeMfr);
  const [error, setError] = useState<string | null>(null);
  const [applyResult, setApplyResult] = useState<ApplyResponse | null>(null);
  const [cooldown, setCooldown] = useState(0);
  const [verifyAttempted, setVerifyAttempted] = useState(false);
  const [useOwnKey, setUseOwnKey] = useState(false);
  const [publicKey, setPublicKey] = useState('');
  const cooldownRef = useRef<ReturnType<typeof setInterval> | null>(null);

  // Resume: fetch existing challenge
  useEffect(() => {
    if (!resumeMfr) return;
    mfrApi.getChallenge(resumeMfr.id).then(res => {
      setApplyResult(res);
      setLoading(false);
    }).catch(e => {
      setError(String(e));
      setLoading(false);
    });
  }, [resumeMfr]);

  // Cleanup cooldown timer
  useEffect(() => () => {
    if (cooldownRef.current) clearInterval(cooldownRef.current);
  }, []);

  // Cancel: void the pending record only if freshly created (not resumed)
  const handleCancel = () => {
    if (applyResult && !resumeMfr) {
      mfrApi.delete(applyResult.mfr_id).catch(() => {});
    }
    onClose();
  };

  const startCooldown = () => {
    setCooldown(VERIFY_COOLDOWN_SECS);
    if (cooldownRef.current) clearInterval(cooldownRef.current);
    cooldownRef.current = setInterval(() => {
      setCooldown(prev => {
        if (prev <= 1) {
          clearInterval(cooldownRef.current!);
          cooldownRef.current = null;
          return 0;
        }
        return prev - 1;
      });
    }, 1000);
  };

  const handleApply = async () => {
    if (!name.trim()) return;
    setLoading(true);
    setError(null);
    try {
      const res = await mfrApi.apply({
        github_login: name.trim(),
        contact: contact.trim() || undefined,
      });
      setApplyResult(res);
      setStep('verify');
    } catch (e) {
      setError(String(e));
    } finally {
      setLoading(false);
    }
  };

  const handleVerify = async () => {
    if (!applyResult) return;
    setLoading(true);
    setError(null);
    try {
      const kc = await mfrApi.verify(applyResult.mfr_id, useOwnKey && publicKey.trim() ? publicKey.trim() : undefined);
      onDone(kc);
    } catch (e) {
      setError(String(e));
      setVerifyAttempted(true);
      startCooldown();
    } finally {
      setLoading(false);
    }
  };

  const loginName = name.trim().toLowerCase();
  const verifyFile = applyResult?.verify_file ?? '';
  const ghCommand = applyResult
    ? `gh repo create ${loginName}/${VERIFY_REPO} --public 2>/dev/null; if [ -d ${VERIFY_REPO} ]; then cd ${VERIFY_REPO} && git pull; else gh repo clone ${loginName}/${VERIFY_REPO} && cd ${VERIFY_REPO}; fi && echo "${applyResult.challenge_token}" > ${verifyFile} && git add . && git commit -m "actrix verify" && git push -u origin main && cd -`
    : '';

  return (
    <div className="fixed inset-0 bg-black/50 flex items-center justify-center z-50">
      <div className="bg-white rounded-xl shadow-xl p-6 max-w-2xl w-full mx-4">
        {/* Header */}
        <div className="flex items-center justify-between mb-5">
          <h2 className="text-lg font-semibold text-gray-900">Register Manufacturer</h2>
          <button onClick={handleCancel} className="text-gray-400 hover:text-gray-600 text-xl leading-none">&times;</button>
        </div>

        {/* Step indicators */}
        <div className="flex items-center gap-2 mb-5 text-xs">
          {(['input', 'verify', 'done'] as CreateStep[]).map((s, i) => {
            const labels = ['1. Name', '2. Verify', '3. Certificate'];
            const active = s === step;
            const done = (['input', 'verify', 'done'].indexOf(step) > i);
            return (
              <span
                key={s}
                className={`px-2 py-1 rounded-full ${
                  active ? 'bg-gray-800 text-white' : done ? 'bg-green-100 text-green-800' : 'bg-gray-100 text-gray-400'
                }`}
              >
                {labels[i]}
              </span>
            );
          })}
        </div>

        {error && (
          <div className="bg-red-50 border border-red-200 rounded-lg p-3 text-sm text-red-700 mb-4">{error}</div>
        )}

        {/* Step 1: Input + Key Mode */}
        {step === 'input' && (
          <div className="space-y-4">
            <div>
              <label className="block text-sm font-medium text-gray-700 mb-1">GitHub Account / Org Name</label>
              <input
                type="text"
                value={name}
                onChange={e => setName(e.target.value)}
                onKeyDown={e => e.key === 'Enter' && void handleApply()}
                placeholder="user or org name"
                className="w-full px-3 py-2 border border-gray-300 rounded-lg text-sm focus:outline-none focus:ring-2 focus:ring-gray-400"
                autoFocus
              />
              <p className="text-xs text-gray-400 mt-1">This will be the manufacturer name in package identifiers.</p>
            </div>
            <div>
              <label className="block text-sm font-medium text-gray-700 mb-1">Contact (optional)</label>
              <input
                type="text"
                value={contact}
                onChange={e => setContact(e.target.value)}
                onKeyDown={e => e.key === 'Enter' && void handleApply()}
                placeholder="email or URL"
                className="w-full px-3 py-2 border border-gray-300 rounded-lg text-sm focus:outline-none focus:ring-2 focus:ring-gray-400"
              />
            </div>

            {/* Signing Key Mode */}
            <div className="border border-gray-200 rounded-lg p-3">
              <div className="flex items-center gap-3 mb-2">
                <label className="text-sm font-medium text-gray-700">Signing Key</label>
                <div className="flex bg-gray-100 rounded-lg p-0.5 text-xs">
                  <button
                    type="button"
                    onClick={() => setUseOwnKey(false)}
                    className={`px-3 py-1 rounded-md transition-all ${!useOwnKey ? 'bg-white shadow text-gray-900 font-medium' : 'text-gray-500 hover:text-gray-700'}`}
                  >Generate for me</button>
                  <button
                    type="button"
                    onClick={() => setUseOwnKey(true)}
                    className={`px-3 py-1 rounded-md transition-all ${useOwnKey ? 'bg-white shadow text-gray-900 font-medium' : 'text-gray-500 hover:text-gray-700'}`}
                  >Use my own key</button>
                </div>
              </div>
              {useOwnKey ? (
                <div>
                  <input
                    type="text"
                    value={publicKey}
                    onChange={e => setPublicKey(e.target.value)}
                    placeholder="Base64-encoded Ed25519 public key (32 bytes)"
                    className="w-full px-3 py-2 border border-gray-300 rounded-lg text-sm font-mono focus:outline-none focus:ring-2 focus:ring-gray-400"
                  />
                  <p className="text-xs text-gray-400 mt-1">Paste your Ed25519 public key. The private key stays with you — the platform will never see it.</p>
                </div>
              ) : (
                <p className="text-xs text-gray-500">The platform will generate an Ed25519 keypair. The private key will be shown <strong>once</strong> and is never stored on the server.</p>
              )}
            </div>

            <div className="flex justify-end gap-2 pt-2">
              <button onClick={handleCancel} className="px-4 py-2 border border-gray-300 rounded-lg text-sm hover:bg-gray-50">Cancel</button>
              <button
                onClick={() => void handleApply()}
                disabled={loading || !name.trim() || (useOwnKey && !publicKey.trim())}
                className="px-4 py-2 bg-gray-800 text-white rounded-lg text-sm hover:bg-gray-700 disabled:opacity-50"
              >
                {loading ? 'Submitting...' : 'Next'}
              </button>
            </div>
          </div>
        )}

        {/* Step 2: Verify */}
        {step === 'verify' && applyResult && (
          <div className="space-y-4">
            <p className="text-sm text-gray-600">
              On <strong>GitHub.com</strong>, create a public repo <code className="bg-gray-100 px-1 rounded text-gray-800">{loginName}/{VERIFY_REPO}</code> with a file <code className="bg-gray-100 px-1 rounded text-gray-800">{verifyFile}</code> containing the token below.
            </p>

            {/* Token */}
            <div>
              <div className="flex items-center justify-between mb-1">
                <label className="text-xs text-gray-500">Challenge Token</label>
                <CopyButton text={applyResult.challenge_token} className="text-xs text-blue-600 hover:text-blue-800 relative" />
              </div>
              <div className="bg-gray-100 rounded-lg p-3 font-mono text-xs break-all select-all">{applyResult.challenge_token}</div>
            </div>

            {/* gh command */}
            <div>
              <div className="flex items-center gap-2 mb-1">
                <Terminal size={12} className="text-gray-400" />
                <label className="text-xs text-gray-500">gh CLI (one-liner)</label>
                <CopyButton text={ghCommand} className="text-xs text-blue-600 hover:text-blue-800 ml-auto relative" />
              </div>
              <pre className="bg-gray-900 text-green-400 rounded-lg p-3 text-xs overflow-x-auto whitespace-pre-wrap">{ghCommand}</pre>
            </div>

            {/* Manual steps */}
            <details className="text-xs text-gray-500">
              <summary className="cursor-pointer hover:text-gray-700">Manual steps</summary>
              <ol className="list-decimal ml-4 mt-2 space-y-1">
                <li>Go to <a href="https://github.com/new" target="_blank" rel="noopener noreferrer" className="text-blue-600 hover:underline">github.com/new</a> and create a public repo named <code>{VERIFY_REPO}</code>{loginName.includes('-') || loginName.length > 15 ? '' : ` under ${loginName}`}</li>
                <li>Add a file named <code>{verifyFile}</code></li>
                <li>Paste the token above as the file content</li>
                <li>Commit and push</li>
              </ol>
            </details>

            <div className="text-xs text-gray-400">
              Expires: {new Date(applyResult.expires_at * 1000).toLocaleString()}
            </div>

            <div className="flex justify-end gap-2 pt-2">
              <button onClick={handleCancel} className="px-4 py-2 border border-gray-300 rounded-lg text-sm hover:bg-gray-50">Cancel</button>
              <button
                onClick={() => void handleVerify()}
                disabled={loading || cooldown > 0}
                className="px-4 py-2 bg-green-600 text-white rounded-lg text-sm hover:bg-green-700 disabled:opacity-50"
              >
                {loading ? 'Verifying...' : cooldown > 0 ? `Retry in ${cooldown}s` : verifyAttempted ? 'Verify Again' : 'Verify'}
              </button>
            </div>
          </div>
        )}
      </div>
    </div>
  );
}

// ── Proto expandable row ──────────────────────────────────────────

function ProtoExpandableRow({
  pkg,
  protoData,
  protoCount,
  hasProto,
  ts,
  onRevoke,
}: {
  pkg: ActrPackage;
  protoData: { protobufs?: { name: string; content: string }[] } | null;
  protoCount: number;
  hasProto: boolean;
  ts: (t: number) => string;
  onRevoke: () => void;
}) {
  const [expanded, setExpanded] = useState(false);

  return (
    <>
      <tr className={`hover:bg-gray-50 ${expanded ? 'bg-blue-50/30' : ''}`}>
        <td className="px-4 py-3 font-mono text-gray-900">{pkg.type_str}</td>
        <td className="px-4 py-3 text-gray-500 font-mono text-xs">{pkg.target || '—'}</td>
        <td className="px-4 py-3 text-gray-600">{pkg.manufacturer}</td>
        <td className="px-4 py-3">
          {hasProto && protoCount > 0 ? (
            <button
              onClick={() => setExpanded(!expanded)}
              className={`inline-flex items-center gap-1 px-2 py-0.5 rounded-full text-xs font-medium transition-colors ${
                expanded
                  ? 'bg-blue-600 text-white'
                  : 'bg-blue-100 text-blue-800 hover:bg-blue-200'
              }`}
            >
              <span className="text-[10px]">{expanded ? '▼' : '▶'}</span>
              {protoCount} file{protoCount > 1 ? 's' : ''}
            </button>
          ) : (
            <span className="text-gray-300 text-xs">—</span>
          )}
        </td>
        <td className="px-4 py-3">
          <span className={`inline-flex items-center px-2 py-0.5 rounded-full text-xs font-medium ${
            pkg.status === 'active' ? 'bg-green-100 text-green-800' : 'bg-red-100 text-red-800'
          }`}>{pkg.status}</span>
        </td>
        <td className="px-4 py-3 text-gray-500">{ts(pkg.published_at)}</td>
        <td className="px-4 py-3">
          {pkg.status === 'active' && (
            <button
              onClick={onRevoke}
              className="px-2 py-1 text-xs bg-red-500 text-white rounded hover:bg-red-600"
            >Revoke</button>
          )}
        </td>
      </tr>
      {expanded && protoData?.protobufs && (
        <tr>
          <td colSpan={7} className="px-0 py-0">
            <div className="bg-gray-900 border-t border-gray-700">
              <div className="flex items-center gap-2 px-4 py-2 border-b border-gray-700">
                <span className="text-gray-400 text-xs font-medium">Proto Files — {pkg.type_str}</span>
              </div>
              <div className="divide-y divide-gray-700">
                {protoData.protobufs.map((proto, i) => (
                  <div key={i}>
                    <div className="flex items-center justify-between px-4 py-1.5 bg-gray-800">
                      <span className="text-xs font-mono text-teal-400">{proto.name}</span>
                      <CopyButton
                        text={proto.content}
                        label="Copy"
                        className="text-[10px] text-gray-400 hover:text-gray-200 relative"
                      />
                    </div>
                    <pre className="px-4 py-3 text-xs font-mono text-green-400 overflow-x-auto whitespace-pre leading-relaxed max-h-80 overflow-y-auto">
                      {proto.content}
                    </pre>
                  </div>
                ))}
              </div>
            </div>
          </td>
        </tr>
      )}
    </>
  );
}

// ── Rotate key modal ──────────────────────────────────────────────

function RotateKeyModal({ mfr, onClose, onDone }: { mfr: Manufacturer; onClose: () => void; onDone: (res: ActivateResponse) => void }) {
  const [useOwnKey, setUseOwnKey] = useState(false);
  const [publicKey, setPublicKey] = useState('');
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const handleSubmit = async () => {
    setLoading(true);
    setError(null);
    try {
      const res = await mfrApi.renewKey(mfr.id, useOwnKey && publicKey.trim() ? publicKey.trim() : undefined);
      onDone(res);
    } catch (e) {
      setError(String(e));
    } finally {
      setLoading(false);
    }
  };

  return (
    <div className="fixed inset-0 bg-black/50 flex items-center justify-center z-50">
      <div className="bg-white rounded-xl shadow-xl p-6 max-w-lg w-full mx-4">
        <div className="flex items-center justify-between mb-5">
          <h2 className="text-lg font-semibold text-gray-900 flex items-center gap-2">
            <Key size={20} className="text-blue-500" /> Rotate Key — {mfr.name}
          </h2>
          <button onClick={onClose} className="text-gray-400 hover:text-gray-600 text-xl leading-none">&times;</button>
        </div>

        {error && <div className="bg-red-50 border border-red-200 rounded-lg p-3 text-sm text-red-700 mb-4">{error}</div>}

        <div className="mb-4 text-sm text-gray-600">
          <p>Rotating the key will instantly invalidate the current public key for signing any <strong>NEW</strong> packages.</p>
          <p className="mt-2">Packages signed with the old key will still verify successfully using the key history.</p>
        </div>

        <div className="border border-gray-200 rounded-lg p-3 mb-4">
          <div className="flex items-center gap-3 mb-2">
            <label className="text-sm font-medium text-gray-700">New Signing Key</label>
            <div className="flex bg-gray-100 rounded-lg p-0.5 text-xs">
              <button
                type="button"
                onClick={() => setUseOwnKey(false)}
                className={`px-3 py-1 rounded-md transition-all ${!useOwnKey ? 'bg-white shadow text-gray-900 font-medium' : 'text-gray-500 hover:text-gray-700'}`}
              >Generate for me</button>
              <button
                type="button"
                onClick={() => setUseOwnKey(true)}
                className={`px-3 py-1 rounded-md transition-all ${useOwnKey ? 'bg-white shadow text-gray-900 font-medium' : 'text-gray-500 hover:text-gray-700'}`}
              >Use my own key</button>
            </div>
          </div>
          {useOwnKey ? (
            <div>
              <input
                type="text"
                value={publicKey}
                onChange={e => setPublicKey(e.target.value)}
                placeholder="Base64-encoded Ed25519 public key (32 bytes)"
                className="w-full px-3 py-2 border border-gray-300 rounded-lg text-sm font-mono focus:outline-none focus:ring-2 focus:ring-blue-400"
              />
            </div>
          ) : (
            <p className="text-xs text-gray-500">The platform will generate a new Ed25519 keypair and display the private key once.</p>
          )}
        </div>

        <div className="flex justify-end gap-2 pt-2">
          <button onClick={onClose} disabled={loading} className="px-4 py-2 border border-gray-300 rounded-lg text-sm hover:bg-gray-50">Cancel</button>
          <button
            onClick={() => void handleSubmit()}
            disabled={loading || (useOwnKey && !publicKey.trim())}
            className="px-4 py-2 bg-blue-600 text-white rounded-lg text-sm hover:bg-blue-700 disabled:opacity-50"
          >
            {loading ? 'Rotating...' : 'Rotate Key'}
          </button>
        </div>
      </div>
    </div>
  );
}

// ── Main component ────────────────────────────────────────────────

export function MfrService() {
  const [manufacturers, setManufacturers] = useState<Manufacturer[]>([]);
  const [packages, setPackages] = useState<ActrPackage[]>([]);
  const [selectedMfr, setSelectedMfr] = useState<Manufacturer | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [activateResult, setActivateResult] = useState<ActivateResponse | null>(null);
  const [actionLoading, setActionLoading] = useState<number | null>(null);
  const [showCreate, setShowCreate] = useState(false);
  const [resumeMfr, setResumeMfr] = useState<Manufacturer | null>(null);
  const [rotateMfr, setRotateMfr] = useState<Manufacturer | null>(null);

  const loadData = useCallback(async () => {
    try {
      const [mfrs, pkgs] = await Promise.all([
        mfrApi.list(),
        mfrApi.listPackages(),
      ]);
      setManufacturers(mfrs);
      setPackages(pkgs);
      setError(null);
    } catch (e) {
      setError(String(e));
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => { void loadData(); }, [loadData]);

  const handleSuspend = async (mfr: Manufacturer) => {
    if (!confirm(`Suspend "${mfr.name}"?`)) return;
    setActionLoading(mfr.id);
    try { await mfrApi.suspend(mfr.id); await loadData(); }
    catch (e) { setError(String(e)); }
    finally { setActionLoading(null); }
  };

  const handleReinstate = async (mfr: Manufacturer) => {
    setActionLoading(mfr.id);
    try { await mfrApi.reinstate(mfr.id); await loadData(); }
    catch (e) { setError(String(e)); }
    finally { setActionLoading(null); }
  };

  const handleDelete = async (mfr: Manufacturer) => {
    if (!confirm(`Delete "${mfr.name}" and all its packages? This cannot be undone.`)) return;
    setActionLoading(mfr.id);
    try { await mfrApi.delete(mfr.id); await loadData(); }
    catch (e) { setError(String(e)); }
    finally { setActionLoading(null); }
  };

  const handleRevokePackage = async (pkg: ActrPackage) => {
    if (!confirm(`Revoke package "${pkg.type_str}"?`)) return;
    try { await mfrApi.revokePackage(pkg.id); await loadData(); }
    catch (e) { setError(String(e)); }
  };

  const handleCreateDone = (res: ActivateResponse) => {
    setActivateResult(res);
    setShowCreate(false);
    setResumeMfr(null);
    void loadData();
  };

  const stats = {
    total: manufacturers.length,
    active: manufacturers.filter(m => m.status === 'active').length,
    pending: manufacturers.filter(m => m.status === 'pending').length,
    suspended: manufacturers.filter(m => m.status === 'suspended').length,
  };

  const filteredPackages = selectedMfr
    ? packages.filter(p => p.mfr_id === selectedMfr.id)
    : packages;

  const ts = (t: number) => new Date(t * 1000).toLocaleDateString();

  if (loading) return <div className="p-8 text-gray-500">Loading...</div>;

  return (
    <div className="p-6 space-y-6">
      {activateResult && <KeychainModal response={activateResult} onClose={() => setActivateResult(null)} />}
      {(showCreate || resumeMfr) && (
        <CreateModal
          onClose={() => { setShowCreate(false); setResumeMfr(null); }}
          onDone={handleCreateDone}
          resumeMfr={resumeMfr ?? undefined}
        />
      )}
      {rotateMfr && (
        <RotateKeyModal
          mfr={rotateMfr}
          onClose={() => setRotateMfr(null)}
          onDone={(res) => {
            setRotateMfr(null);
            setActivateResult(res);
            void loadData();
          }}
        />
      )}

      <div className="flex items-start justify-between">
        <div>
          <h1 className="text-2xl font-bold text-gray-900 flex items-center gap-2">
            <Building2 size={24} /> Manufacturer Registry
          </h1>
          <p className="text-gray-500 text-sm mt-1">Manage registered actor manufacturers and published packages.</p>
        </div>
        <button
          onClick={() => setShowCreate(true)}
          className="flex items-center gap-1 px-4 py-2 text-sm bg-gray-800 text-white rounded-lg hover:bg-gray-700"
        >
          <Plus size={14} /> New
        </button>
      </div>

      <HowItWorks />

      {error && (
        <div className="bg-red-50 border border-red-200 rounded-lg p-3 text-sm text-red-700">{error}</div>
      )}

      {/* Stats */}
      <div className="grid grid-cols-4 gap-4">
        {[
          { label: 'Total', value: stats.total, color: 'text-gray-700' },
          { label: 'Active', value: stats.active, color: 'text-green-700' },
          { label: 'Pending', value: stats.pending, color: 'text-yellow-700' },
          { label: 'Suspended', value: stats.suspended, color: 'text-orange-700' },
        ].map(s => (
          <div key={s.label} className="bg-white rounded-xl border border-gray-200 p-4">
            <div className={`text-2xl font-bold ${s.color}`}>{s.value}</div>
            <div className="text-gray-500 text-sm">{s.label}</div>
          </div>
        ))}
      </div>

      {/* MFR Table */}
      <div className="bg-white rounded-xl border border-gray-200 overflow-hidden">
        <div className="px-4 py-3 border-b border-gray-100 flex items-center justify-between">
          <h2 className="font-semibold text-gray-800">Manufacturers</h2>
          {selectedMfr && (
            <button onClick={() => setSelectedMfr(null)} className="text-xs text-gray-500 hover:text-gray-800">
              Clear filter
            </button>
          )}
        </div>
        <table className="w-full text-sm">
          <thead className="bg-gray-50 text-gray-500 text-xs uppercase">
            <tr>
              {['Name', 'Status', 'Key ID', 'Key Expires', 'Packages', 'Actions'].map(h => (
                <th key={h} className="px-4 py-2 text-left font-medium">{h}</th>
              ))}
            </tr>
          </thead>
          <tbody className="divide-y divide-gray-100">
            {manufacturers.length === 0 && (
              <tr><td colSpan={6} className="px-4 py-8 text-center text-gray-400">No manufacturers registered</td></tr>
            )}
            {manufacturers.map(mfr => {
              const pkgCount = packages.filter(p => p.mfr_id === mfr.id).length;
              const isSelected = selectedMfr?.id === mfr.id;
              return (
                <>
                  <tr
                    key={mfr.id}
                    className={`hover:bg-gray-50 cursor-pointer ${isSelected ? 'bg-blue-50' : ''}`}
                    onClick={() => setSelectedMfr(isSelected ? null : mfr)}
                  >
                    <td className="px-4 py-3 font-mono font-medium text-gray-900">{mfr.name}</td>
                    <td className="px-4 py-3"><StatusBadge status={mfr.status} /></td>
                    <td className="px-4 py-3 text-gray-500 font-mono text-[11px] truncate max-w-[120px]" title={mfr.key_id}>{mfr.key_id || '—'}</td>
                    <td className="px-4 py-3">
                      <KeyExpiryBadge expiresAt={mfr.key_expires_at} />
                    </td>
                    <td className="px-4 py-3">
                      <span className="inline-flex items-center gap-1 text-gray-600">
                        <Package size={12} /> {pkgCount}
                      </span>
                    </td>
                    <td className="px-4 py-3" onClick={e => e.stopPropagation()}>
                      <div className="flex gap-1">
                        {mfr.status === 'pending' && (
                          <button
                            onClick={() => setResumeMfr(mfr)}
                            className="px-2 py-1 text-xs bg-gray-800 text-white rounded hover:bg-gray-700"
                          >Continue</button>
                        )}
                        {mfr.status === 'active' && (
                          <>
                            <button
                              onClick={() => setRotateMfr(mfr)}
                              disabled={actionLoading === mfr.id}
                              className="px-2 py-1 text-xs bg-blue-600 text-white rounded hover:bg-blue-700 disabled:opacity-50"
                            >Rotate Key</button>
                            <button
                              onClick={() => void handleSuspend(mfr)}
                              disabled={actionLoading === mfr.id}
                              className="px-2 py-1 text-xs bg-orange-500 text-white rounded hover:bg-orange-600 disabled:opacity-50"
                            >Suspend</button>
                          </>
                        )}
                        {mfr.status === 'suspended' && (
                          <button
                            onClick={() => void handleReinstate(mfr)}
                            disabled={actionLoading === mfr.id}
                            className="px-2 py-1 text-xs bg-green-600 text-white rounded hover:bg-green-700 disabled:opacity-50"
                          >Reinstate</button>
                        )}
                        <button
                          onClick={() => void handleDelete(mfr)}
                          disabled={actionLoading === mfr.id}
                          className="px-2 py-1 text-xs bg-red-500 text-white rounded hover:bg-red-600 disabled:opacity-50"
                        >Delete</button>
                      </div>
                    </td>
                  </tr>
                  {isSelected && (
                    <tr key={`${mfr.id}-history`}>
                      <td colSpan={6} className="p-0">
                        <KeyHistoryPanel mfr={mfr} onRevoked={() => void loadData()} />
                      </td>
                    </tr>
                  )}
                </>
              );
            })}
          </tbody>
        </table>
      </div>

      {/* Package Table */}
      <div className="bg-white rounded-xl border border-gray-200 overflow-hidden">
        <div className="px-4 py-3 border-b border-gray-100">
          <h2 className="font-semibold text-gray-800">
            {selectedMfr ? `Packages — ${selectedMfr.name}` : 'All Packages'}
          </h2>
        </div>
        <table className="w-full text-sm">
          <thead className="bg-gray-50 text-gray-500 text-xs uppercase">
            <tr>
              {['Type', 'Target', 'Manufacturer', 'Proto', 'Status', 'Published', 'Actions'].map(h => (
                <th key={h} className="px-4 py-2 text-left font-medium">{h}</th>
              ))}
            </tr>
          </thead>
          <tbody className="divide-y divide-gray-100">
            {filteredPackages.length === 0 && (
              <tr><td colSpan={7} className="px-4 py-8 text-center text-gray-400">No packages</td></tr>
            )}
            {filteredPackages.map(pkg => {
              const hasProto = !!pkg.proto_files;
              let protoData: { protobufs?: { name: string; content: string }[] } | null = null;
              if (hasProto) {
                try { protoData = JSON.parse(pkg.proto_files!); } catch { /* ignore */ }
              }
              const protoCount = protoData?.protobufs?.length ?? 0;
              return (
                <ProtoExpandableRow
                  key={pkg.id}
                  pkg={pkg}
                  protoData={protoData}
                  protoCount={protoCount}
                  hasProto={hasProto}
                  ts={ts}
                  onRevoke={() => void handleRevokePackage(pkg)}
                />
              );
            })}
          </tbody>
        </table>
      </div>
    </div>
  );
}
