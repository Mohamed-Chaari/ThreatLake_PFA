import type { AttackerProfile } from "./api";

interface TopAttackersPreviewProps {
  profiles: AttackerProfile[] | null;
  error: string | null;
  onSelect: (profile: AttackerProfile) => void;
  // src_ips present in the latest poll that weren't present in the
  // previous one - see App.tsx's refetchAll. Only ever non-empty after
  // at least one real poll refresh has happened (never on first load),
  // so this is a genuine "what changed" diff, not a decorative effect.
  newSrcIps: Set<string>;
}

const PREVIEW_COUNT = 5;

// A compact top-5-by-total_events slice of the same leaderboard the
// Attackers tab shows in full - same data (App.tsx's single
// GET /attacker_profiles fetch), just the highest-signal rows and fewer
// columns. Clicking a row opens the same AttackerDrawer the full
// Attackers tab uses. The "view all" action lives in the card header at
// the call site (Overview.tsx), not here.
export default function TopAttackersPreview({ profiles, error, onSelect, newSrcIps }: TopAttackersPreviewProps) {
  if (error) {
    return <p className="error">Could not load attacker profiles: {error}</p>;
  }
  if (profiles === null) {
    return <p className="hint">Loading...</p>;
  }

  const top = [...profiles].sort((a, b) => b.total_events - a.total_events).slice(0, PREVIEW_COUNT);

  if (top.length === 0) {
    return <p className="hint">No attacker profiles yet.</p>;
  }

  return (
    <table className="preview-table">
      <tbody>
        {top.map((profile, i) => (
          <tr
            key={profile.src_ip}
            className={`clickable-row ${newSrcIps.has(profile.src_ip) ? "row-flash" : ""}`}
            onClick={() => onSelect(profile)}
          >
            <td className="preview-rank">{i + 1}</td>
            <td className="mono">{profile.src_ip}</td>
            <td className="mono preview-count">{profile.total_events}</td>
          </tr>
        ))}
      </tbody>
    </table>
  );
}
