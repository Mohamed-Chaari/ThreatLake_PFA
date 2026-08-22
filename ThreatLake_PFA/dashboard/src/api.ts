// Thin fetch wrappers over threatlake.api.app's 3 routes. Field names
// below mirror threatlake.api.schemas.py exactly, so a change there is a
// visible, one-place change here too rather than a silent mismatch.

export interface AlertItem {
  event_id: string;
  scored_at: string;
  alert_source: string | null;
  anomaly_score: number | null;
  is_anomaly: boolean;
  event_time: string | null;
  src_ip: string | null;
  dst_ip: string | null;
  dst_port: number | null;
  source_type: string | null;
  severity: number | null;
  attack_category: string | null;
}

export interface AlertsResponse {
  total: number;
  limit: number;
  offset: number;
  items: AlertItem[];
}

export interface CopilotQueryResponse {
  rejected: boolean;
  reason: string | null;
  sql: string | null;
  rows: Record<string, unknown>[] | null;
  row_count: number | null;
}

export interface AttackerProfile {
  src_ip: string;
  first_seen: string | null;
  last_seen: string | null;
  total_events: number;
  distinct_ports_hit: number;
  distinct_honeypots_hit: number;
  attack_categories: string[];
  // Null when this src_ip didn't resolve via MaxMind GeoLite2 (private/
  // reserved range, or a free-tier coverage gap) - see
  // threatlake.api.schemas.AttackerProfile.
  latitude: number | null;
  longitude: number | null;
}

export interface AttackerProfilesResponse {
  total: number;
  limit: number;
  offset: number;
  items: AttackerProfile[];
}

async function getJson<T>(url: string): Promise<T> {
  const response = await fetch(url);
  if (!response.ok) {
    throw new Error(`${url} returned HTTP ${response.status}`);
  }
  return (await response.json()) as T;
}

export function fetchAlerts(): Promise<AlertsResponse> {
  return getJson<AlertsResponse>("/alerts?limit=100");
}

export function fetchAttackerProfiles(): Promise<AttackerProfilesResponse> {
  return getJson<AttackerProfilesResponse>("/attacker_profiles?limit=200");
}

export async function askCopilot(question: string): Promise<CopilotQueryResponse> {
  const response = await fetch("/copilot/query", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ question }),
  });
  if (!response.ok) {
    throw new Error(`/copilot/query returned HTTP ${response.status}`);
  }
  return (await response.json()) as CopilotQueryResponse;
}
