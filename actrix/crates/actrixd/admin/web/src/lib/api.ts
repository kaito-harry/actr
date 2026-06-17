const BASE = "/admin/api";
const TOKEN_KEY = "actrix_admin_token";

async function request<T>(
  path: string,
  options: RequestInit = {},
): Promise<T> {
  const token = localStorage.getItem(TOKEN_KEY);
  const headers: Record<string, string> = {
    "Content-Type": "application/json",
    ...(options.headers as Record<string, string> | undefined),
  };
  if (token) {
    headers["Authorization"] = `Bearer ${token}`;
  }

  const res = await fetch(`${BASE}${path}`, { ...options, headers });

  if (res.status === 401) {
    localStorage.removeItem(TOKEN_KEY);
    window.location.href = "/admin/login";
    throw new Error("Unauthorized");
  }

  if (!res.ok) {
    const body = await res.json().catch(() => ({}));
    throw new Error(body.error || body.error_message || res.statusText);
  }

  return res.json();
}

// Types
export interface LoginResponse {
  token: string;
  expires_in: number;
}

export interface PublicNodeNameResponse {
  name: string;
}

export interface AdminCapabilities {
  realm_writes_enabled: boolean;
  superv_managed: boolean;
}

export interface MetricDetail {
  value: number;
  score: number;
}

export interface ServiceStatus {
  name: string;
  type: number;
  is_healthy: boolean;
  active_connections: number;
  total_requests: number;
  failed_requests: number;
  average_latency_ms: number;
  url: string | null;
  port: number | null;
  domain: string | null;
}

export interface NodeInfo {
  success: boolean;
  node_id: string;
  name: string;
  version: string;
  location_tag: string;
  uptime_secs: number;
  power_reserve: number;
  metrics: Record<string, MetricDetail> | null;
  services: ServiceStatus[];
}

export interface RealmInfo {
  realm_id: number;
  name: string;
  enabled: boolean;
  created_at: number;
  updated_at: number | null;
  use_servers: number[];
  version: number;
  expires_at: number;
  status: string;
  secret_rotation_state?: {
    current_hash_preview: string;
    previous_hash_preview?: string;
    previous_valid_until?: number;
  } | null;
}

export interface RealmsResponse {
  success: boolean;
  realms: RealmInfo[];
  total_count: number;
}

export interface RealmMutationResponse {
  success: boolean;
  error_message: string | null;
  realm?: RealmInfo | null;
  realm_secret?: string | null;
}

export interface RealmSecretRotationResponse {
  success: boolean;
  realm_id: number;
  realm_secret: string;
  previous_valid_until: number | null;
  grace_seconds: number;
}

export interface ResolvedField {
  key: string;
  value_type: string;
  description: string;
  dynamic: boolean;
  reloadable: boolean;
  default_value: string;
  config_file_value: string | null;
  override_value: string | null;
  effective_value: string;
  source: "default" | "config_file" | "override";
  choices?: string[];
}

export interface ConfigFieldDef {
  key: string;
  toml_path: string;
  value_type: string;
  default_value: string;
  dynamic: boolean;
  reloadable: boolean;
  description: string;
  service: string;
}

export interface ConfigOverrideEntry {
  key_path: string;
  value: string;
  updated_at: string;
  updated_by: string;
}

export interface ServiceDetail {
  enabled: boolean;
  status: ServiceStatus | null;
  config: Record<string, unknown> | null;
  config_fields?: ResolvedField[];
}

export interface PlatformDetail {
  config: Record<string, unknown>;
  config_fields: ResolvedField[];
}

export interface KeyEntry {
  key_id: number;
  pk_size: number;
  created_at?: number;
  fetched_at?: number;
  expires_at: number;
  tolerance_seconds?: number;
  is_expired: boolean;
}

export interface MetricSample {
  ts: number;
  active_conns: number;
  requests: number;
  failed_requests: number;
  latency_p95_ms: number;
}

export interface TimeseriesResponse {
  service_type: number;
  tier: number;
  interval_secs: number;
  samples: MetricSample[];
}

export const api = {
  login: (password: string) =>
    request<LoginResponse>("/auth/login", {
      method: "POST",
      body: JSON.stringify({ password }),
    }),

  getPublicNodeName: async (): Promise<PublicNodeNameResponse> => {
    const fallback = await fetch("/admin/health", {
      headers: { Accept: "application/json" },
    });
    if (fallback.ok) {
      const body = await fallback.json().catch(() => null);
      const name = typeof body?.node === "string"
        ? body.node.trim()
        : "";
      if (name.length > 0) {
        return { name };
      }
    }

    throw new Error("Failed to load node name");
  },

  getNodeInfo: () => request<NodeInfo>("/node"),

  getCapabilities: () => request<AdminCapabilities>("/capabilities"),

  getServices: () =>
    request<{ services: ServiceStatus[] }>("/node/services"),

  listRealms: () => request<RealmsResponse>("/realms"),

  createRealm: (data: {
    realm_id?: number;
    name: string;
    enabled?: boolean;
  }) =>
    request<RealmMutationResponse>("/realms", {
      method: "POST",
      body: JSON.stringify(data),
    }),

  getRealm: (id: number) =>
    request<{ success: boolean; realm: RealmInfo | null }>(`/realms/${id}`),

  updateRealm: (id: number, data: { name?: string; enabled?: boolean }) =>
    request<RealmMutationResponse>(`/realms/${id}`, {
      method: "PUT",
      body: JSON.stringify(data),
    }),

  deleteRealm: (id: number) =>
    request<{ success: boolean; error_message: string | null }>(
      `/realms/${id}`,
      { method: "DELETE" },
    ),

  rotateRealmSecret: (id: number) =>
    request<RealmSecretRotationResponse>(`/realms/${id}/secret/rotate`, {
      method: "POST",
    }),

  getConfig: (configType: number, key: string) =>
    request<{
      success: boolean;
      config_value: string | null;
    }>(`/config/${configType}/${key}`),

  updateConfig: (configType: number, key: string, value: string) =>
    request<{ success: boolean; old_value: string | null }>(
      `/config/${configType}/${key}`,
      { method: "PUT", body: JSON.stringify({ config_value: value }) },
    ),

  getConfigFile: () =>
    request<{ content: string; path: string }>("/config-file"),

  saveConfigFile: (content: string) =>
    request<{ saved: boolean; error?: string }>("/config-file", {
      method: "PUT",
      body: JSON.stringify({ content }),
    }),

  reloadNode: () =>
    request<{ accepted: boolean }>("/node/reload", { method: "POST" }),

  restartNode: () =>
    request<{ accepted: boolean; error_message?: string }>("/node/restart", {
      method: "POST",
    }),

  shutdown: (data: {
    graceful?: boolean;
    timeout_secs?: number;
    reason?: string;
  }) =>
    request<{ accepted: boolean }>("/node/shutdown", {
      method: "POST",
      body: JSON.stringify(data),
    }),

  getPlatformDetail: () =>
    request<PlatformDetail>("/platform"),

  getServiceDetail: (name: string) =>
    request<ServiceDetail>(`/services/${name}`),

  getSignerKeys: () =>
    request<{ keys: KeyEntry[]; total_count: number }>("/services/signer/keys"),

  cleanupSignerKeys: () =>
    request<{ deleted: number; remaining: number; tolerance_seconds: number }>(
      "/services/signer/keys/cleanup",
      { method: "POST" },
    ),

  getAisKeys: () =>
    request<{ keys: KeyEntry[] }>("/services/ais/keys"),

  getRegistry: () =>
    request<ConfigFieldDef[]>("/registry"),

  getOverrides: () =>
    request<ConfigOverrideEntry[]>("/config/overrides"),

  setOverride: (key: string, value: string) =>
    request<{ success: boolean }>(`/config/overrides/${encodeURIComponent(key)}`, {
      method: "PUT",
      body: JSON.stringify({ value }),
    }),

  deleteOverride: (key: string) =>
    request<{ success: boolean; deleted: boolean }>(`/config/overrides/${encodeURIComponent(key)}`, {
      method: "DELETE",
    }),

  probePort: (port: number) =>
    request<{ reachable: boolean; latency_ms?: number; error?: string }>(`/network/probe/${port}`),

  getMetricsTimeseries: (serviceType: number, tier: number) =>
    request<TimeseriesResponse>(`/metrics/timeseries?service_type=${serviceType}&tier=${tier}`),
};

// MFR types
export interface Manufacturer {
  id: number;
  name: string;
  public_key: string;
  key_id: string;
  contact?: string;
  status: 'pending' | 'active' | 'suspended' | 'revoked';
  created_at: number;
  updated_at?: number;
  verified_at?: number;
  suspended_at?: number;
  revoked_at?: number;
  key_expires_at?: number;
}

export interface ActrPackage {
  id: number;
  mfr_id: number;
  manufacturer: string;
  name: string;
  version: string;
  type_str: string;
  target: string;
  manifest: string;
  signature: string;
  proto_files?: string;
  status: 'active' | 'revoked';
  published_at: number;
  revoked_at?: number;
}

export interface MfrCertificate {
  key_id: string;
  mfr_name: string;
  mfr_pubkey: string;
  issued_at: number;
  expires_at: number;
}

export interface MfrKeyHistory {
  id: number;
  mfr_id: number;
  key_id: string;
  public_key: string;
  status: 'retired' | 'revoked';
  created_at: number;
  retired_at: number;
}

export type KeySource = 'generated' | 'uploaded';

export interface ActivateResponse {
  key_source: KeySource;
  /** Present ONLY when key_source == 'generated'. */
  private_key?: string;
  certificate: MfrCertificate;
}

export interface ApplyRequest {
  github_login: string;
  contact?: string;
}

export interface ApplyResponse {
  mfr_id: number;
  challenge_token: string;
  expires_at: number;
  verify_file: string;
  instructions: string;
}

// MFR API functions — routed through /admin/api/mfr, so paths are relative to BASE
export const mfrApi = {
  list: (status?: string) => {
    const params = status ? `?status=${status}` : '';
    return request<Manufacturer[]>(`/mfr/admin/list${params}`);
  },

  apply: (req: ApplyRequest) =>
    request<ApplyResponse>('/mfr/apply', {
      method: 'POST',
      body: JSON.stringify(req),
    }),

  verify: (id: number, publicKey?: string) =>
    request<ActivateResponse>(`/mfr/${id}/verify`, {
      method: 'POST',
      body: publicKey ? JSON.stringify({ public_key: publicKey }) : undefined,
    }),

  getChallenge: (id: number) =>
    request<ApplyResponse>(`/mfr/${id}/challenge`),

  approve: (id: number, publicKey?: string) =>
    request<ActivateResponse>(`/mfr/admin/${id}/approve`, {
      method: 'POST',
      body: publicKey ? JSON.stringify({ public_key: publicKey }) : undefined,
    }),

  suspend: (id: number) =>
    request<void>(`/mfr/admin/${id}/suspend`, { method: 'POST' }),

  renewKey: (id: number, publicKey?: string) =>
    request<ActivateResponse>(`/mfr/admin/${id}/renew`, {
      method: 'POST',
      body: publicKey ? JSON.stringify({ public_key: publicKey }) : undefined,
    }),

  reinstate: (id: number) =>
    request<void>(`/mfr/admin/${id}/reinstate`, { method: 'POST' }),

  delete: (id: number) =>
    request<void>(`/mfr/admin/${id}`, { method: 'DELETE' }),

  listPackages: (mfr?: string) => {
    const params = mfr ? `?mfr=${mfr}` : '';
    return request<ActrPackage[]>(`/mfr/pkg${params}`);
  },

  revokePackage: (id: number) =>
    request<void>(`/mfr/pkg/${id}/revoke`, { method: 'POST' }),

  listKeys: (mfrId: number) =>
    request<MfrKeyHistory[]>(`/mfr/admin/${mfrId}/keys`),

  revokeHistoricalKey: (historyId: number) =>
    request<void>(`/mfr/admin/keys/${historyId}/revoke`, { method: 'POST' }),
};
