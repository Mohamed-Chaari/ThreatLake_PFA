import { useMemo, useState } from "react";
import AttackerDrawer from "./AttackerDrawer";
import type { AttackerProfile } from "./api";
import SearchInput from "./SearchInput";

interface AttackersViewProps {
  profiles: AttackerProfile[] | null;
  error: string | null;
}

type SortKey =
  | "src_ip"
  | "total_events"
  | "distinct_ports_hit"
  | "first_seen"
  | "last_seen"
  | "latitude";
type SortDir = "asc" | "desc";

const COLUMNS: { key: SortKey; label: string }[] = [
  { key: "src_ip", label: "Src IP" },
  { key: "total_events", label: "Total events" },
  { key: "distinct_ports_hit", label: "Distinct ports" },
  { key: "first_seen", label: "First seen" },
  { key: "last_seen", label: "Last seen" },
  { key: "latitude", label: "Location" },
];

//: How many credential pills to show inline before collapsing the rest
//: into a "+N" pill - most-tried pairs are already sorted first by the
//: API (threatlake.transform.gold.attacker_profiles), so the ones shown
//: are always this attacker's most common attempts, not an arbitrary cut.
const MAX_CREDENTIAL_PILLS = 3;

// Plain string comparison sorts "13.0.0.1" before "2.0.0.1" (lexicographic,
// not numeric) - wrong for IPv4 addresses. Splitting into its 4 octets and
// comparing each numerically is what "sorted" actually means here.
function compareIp(a: string, b: string): number {
  const aParts = a.split(".").map(Number);
  const bParts = b.split(".").map(Number);
  for (let i = 0; i < 4; i++) {
    if (aParts[i] !== bParts[i]) return aParts[i] - bParts[i];
  }
  return 0;
}

function compareValues(a: AttackerProfile, b: AttackerProfile, key: SortKey): number {
  const av = a[key];
  const bv = b[key];
  if (av === null && bv === null) return 0;
  if (av === null) return 1; // nulls sort last regardless of direction
  if (bv === null) return -1;
  if (key === "src_ip") return compareIp(av as string, bv as string);
  if (typeof av === "string" && typeof bv === "string") return av.localeCompare(bv);
  return (av as number) - (bv as number);
}

// A ranked leaderboard over the EXISTING GET /attacker_profiles response -
// no new endpoint, no new gold table, just a second view (alongside the
// Map tab) over data the pipeline already computes. Sortable client-side
// since the whole response (up to 200 profiles) is already in memory.
// Clicking a row opens AttackerDrawer with everything this same response
// already returns for that IP, just laid out richer.
//
// Takes the already-fetched profiles as a prop, same reasoning as
// AlertsView: App.tsx owns the single GET /attacker_profiles call so this
// table, the Map tab, and Overview's previews all read the exact same
// response rather than each fetching their own copy.
export default function AttackersView({ profiles, error }: AttackersViewProps) {
  const [sortKey, setSortKey] = useState<SortKey>("total_events");
  const [sortDir, setSortDir] = useState<SortDir>("desc");
  const [search, setSearch] = useState("");
  const [selected, setSelected] = useState<AttackerProfile | null>(null);

  const sorted = useMemo(() => {
    if (!profiles) return null;
    const copy = [...profiles];
    copy.sort((a, b) => compareValues(a, b, sortKey) * (sortDir === "asc" ? 1 : -1));
    return copy;
  }, [profiles, sortKey, sortDir]);

  const filtered = useMemo(() => {
    if (!sorted) return null;
    const needle = search.trim();
    if (!needle) return sorted;
    return sorted.filter((p) => p.src_ip.includes(needle));
  }, [sorted, search]);

  function handleSort(key: SortKey) {
    if (key === sortKey) {
      setSortDir((dir) => (dir === "asc" ? "desc" : "asc"));
    } else {
      setSortKey(key);
      // Numeric columns read more naturally starting high-to-low;
      // string/date columns starting low-to-high (A-Z, earliest-first).
      setSortDir(key === "src_ip" || key === "first_seen" || key === "last_seen" ? "asc" : "desc");
    }
  }

  if (error) {
    return <p className="error">Could not load attacker profiles: {error}</p>;
  }
  if (filtered === null) {
    return <p className="hint">Loading attacker profiles...</p>;
  }
  if (profiles && profiles.length === 0) {
    return <p className="hint">No attacker profiles yet - run scripts/run_pipeline.py first.</p>;
  }

  return (
    <div className="view-stack">
      <SearchInput value={search} onChange={setSearch} placeholder="Filter by src IP..." />

      <div className="card">
        {filtered.length === 0 ? (
          <p className="hint panel-section">No attackers match "{search}".</p>
        ) : (
          <table className="attackers-table">
            <thead>
              <tr>
                {COLUMNS.map((col) => (
                  <th key={col.key}>
                    <button
                      type="button"
                      className={`sort-header ${sortKey === col.key ? "sort-header-active" : ""}`}
                      onClick={() => handleSort(col.key)}
                    >
                      {col.label}
                      {sortKey === col.key && <span className="sort-arrow">{sortDir === "asc" ? "↑" : "↓"}</span>}
                    </button>
                  </th>
                ))}
                <th>Top credentials tried</th>
              </tr>
            </thead>
            <tbody>
              {filtered.map((profile) => {
                const shown = profile.top_credentials_tried.slice(0, MAX_CREDENTIAL_PILLS);
                const remaining = profile.top_credentials_tried.length - shown.length;
                return (
                  <tr key={profile.src_ip} className="clickable-row" onClick={() => setSelected(profile)}>
                    <td className="mono">{profile.src_ip}</td>
                    <td className="mono">{profile.total_events}</td>
                    <td className="mono">{profile.distinct_ports_hit}</td>
                    <td className="mono">{profile.first_seen ?? "-"}</td>
                    <td className="mono">{profile.last_seen ?? "-"}</td>
                    <td className="mono">
                      {profile.latitude !== null && profile.longitude !== null
                        ? `${profile.latitude.toFixed(2)}, ${profile.longitude.toFixed(2)}`
                        : "-"}
                    </td>
                    <td>
                      {profile.top_credentials_tried.length === 0 ? (
                        <span className="hint">-</span>
                      ) : (
                        <div className="cred-pills">
                          {shown.map((cred, i) => (
                            <span key={i} className="cred-pill">
                              {cred.username ?? "?"}/{cred.password ?? "?"}
                              {cred.count > 1 && <span className="cred-pill-count">×{cred.count}</span>}
                            </span>
                          ))}
                          {remaining > 0 && <span className="cred-pill cred-pill-more">+{remaining}</span>}
                        </div>
                      )}
                    </td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        )}
      </div>

      <AttackerDrawer profile={selected} onClose={() => setSelected(null)} />
    </div>
  );
}
