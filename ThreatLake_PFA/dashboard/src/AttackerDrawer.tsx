import { useEffect, useState } from "react";
import type { AttackerProfile } from "./api";
import { colorForCategory } from "./categoryColors";
import MapView from "./MapView";

interface AttackerDrawerProps {
  profile: AttackerProfile | null;
  onClose: () => void;
}

// A slide-in detail panel over ONE attacker_profiles row - every field
// shown here is already in the GET /attacker_profiles response that fed
// the table/preview row that opened it; this is a richer LAYOUT of
// existing data, not new data. Reachable from the Attackers tab's table
// and Overview's Top attackers preview (both pass the clicked profile in
// as this same prop).
export default function AttackerDrawer({ profile, onClose }: AttackerDrawerProps) {
  // Keep showing the last profile's content while the close transition
  // plays, instead of the panel blanking mid-slide - `open` (from the
  // *current* profile prop) drives the transform, `displayed` (sticky)
  // drives what's rendered.
  const [displayed, setDisplayed] = useState<AttackerProfile | null>(null);
  useEffect(() => {
    if (profile) setDisplayed(profile);
  }, [profile]);

  const open = profile !== null;

  useEffect(() => {
    if (!open) return;
    const handleKey = (e: KeyboardEvent) => {
      if (e.key === "Escape") onClose();
    };
    document.addEventListener("keydown", handleKey);
    return () => document.removeEventListener("keydown", handleKey);
  }, [open, onClose]);

  return (
    <>
      <div className={`drawer-overlay ${open ? "drawer-overlay-open" : ""}`} onClick={onClose} aria-hidden="true" />
      <aside className={`drawer ${open ? "drawer-open" : ""}`} aria-hidden={!open}>
        {displayed && (
          <>
            <div className="drawer-header">
              <span className="mono drawer-title">{displayed.src_ip}</span>
              <button type="button" className="drawer-close" onClick={onClose} aria-label="Close">
                ×
              </button>
            </div>

            <div className="drawer-body">
              <div className="drawer-stats">
                <div>
                  <span className="stat-label">Total events</span>
                  <span className="stat-value">{displayed.total_events}</span>
                </div>
                <div>
                  <span className="stat-label">Distinct ports</span>
                  <span className="stat-value">{displayed.distinct_ports_hit}</span>
                </div>
              </div>

              <div className="panel-section">
                <span className="stat-label">Activity window</span>
                <div className="timeline">
                  <div className="timeline-point">
                    <span className="timeline-dot" />
                    <span className="timeline-label">First seen</span>
                    <span className="mono timeline-time">{displayed.first_seen ?? "-"}</span>
                  </div>
                  <div className="timeline-line" />
                  <div className="timeline-point">
                    <span className="timeline-dot" />
                    <span className="timeline-label">Last seen</span>
                    <span className="mono timeline-time">{displayed.last_seen ?? "-"}</span>
                  </div>
                </div>
              </div>

              <div className="panel-section">
                <span className="stat-label">Attack categories</span>
                <div className="cred-pills">
                  {displayed.attack_categories.map((category) => (
                    <span
                      key={category}
                      className="cred-pill category-pill"
                      style={{ borderColor: colorForCategory(category), color: colorForCategory(category) }}
                    >
                      {category}
                    </span>
                  ))}
                </div>
              </div>

              <div className="panel-section">
                <span className="stat-label">Credentials tried ({displayed.top_credentials_tried.length})</span>
                {displayed.top_credentials_tried.length === 0 ? (
                  <p className="hint">No credential attempts recorded for this attacker.</p>
                ) : (
                  <div className="cred-pills">
                    {displayed.top_credentials_tried.map((cred, i) => (
                      <span key={i} className="cred-pill">
                        {cred.username ?? "?"}/{cred.password ?? "?"}
                        {cred.count > 1 && <span className="cred-pill-count">×{cred.count}</span>}
                      </span>
                    ))}
                  </div>
                )}
              </div>

              <div className="panel-section">
                <span className="stat-label">Location</span>
                {displayed.latitude !== null && displayed.longitude !== null ? (
                  <>
                    <p className="mono hint">
                      {displayed.latitude.toFixed(4)}, {displayed.longitude.toFixed(4)}
                    </p>
                    <MapView profiles={[displayed]} error={null} compact />
                  </>
                ) : (
                  <p className="hint">This IP didn't resolve to a location (see ARCHITECTURE.md).</p>
                )}
              </div>
            </div>
          </>
        )}
      </aside>
    </>
  );
}
