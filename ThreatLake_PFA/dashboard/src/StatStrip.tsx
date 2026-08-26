import type { AlertItem } from "./api";

interface StatStripProps {
  alerts: AlertItem[] | null;
}

const SOURCES = ["rule", "ml", "both"] as const;

// Three stat cards, entirely derived from the SAME alerts list App.tsx
// already fetched for the Alerts tab - no second request, just a
// different view of one response. Sits above the tabs so it reads as a
// summary of the whole pipeline's current state, not a per-tab widget.
export default function StatStrip({ alerts }: StatStripProps) {
  const total = alerts?.length ?? 0;
  const uniqueAttackers = alerts ? new Set(alerts.map((a) => a.src_ip)).size : 0;
  const bySource = Object.fromEntries(
    SOURCES.map((source) => [source, alerts?.filter((a) => a.alert_source === source).length ?? 0]),
  ) as Record<(typeof SOURCES)[number], number>;

  return (
    <div className="stat-strip">
      <div className="stat-card">
        <span className="stat-label">Total alerts</span>
        {/* key={total}: forces React to remount this span whenever the
            number changes, which restarts the fade-update CSS animation -
            a poll refresh that changes the count visibly settles in
            instead of jump-cutting to the new value. */}
        <span key={total} className="stat-value stat-value-amber fade-update">
          {alerts === null ? "-" : total}
        </span>
      </div>

      <div className="stat-card">
        <span className="stat-label">Unique attackers</span>
        <span key={uniqueAttackers} className="stat-value fade-update">
          {alerts === null ? "-" : uniqueAttackers}
        </span>
      </div>

      <div className="stat-card stat-card-wide">
        <span className="stat-label">Alert source</span>
        {alerts === null || total === 0 ? (
          <span className="stat-value">-</span>
        ) : (
          <>
            <div className="source-bar" aria-hidden="true">
              {SOURCES.map((source) => (
                <div
                  key={source}
                  className={`source-bar-segment source-bar-${source}`}
                  style={{ flexGrow: bySource[source] }}
                />
              ))}
            </div>
            <div className="source-counts">
              {SOURCES.map((source) => (
                <span key={source} className={`source-count source-count-${source}`}>
                  <span className="source-dot" /> {source} {bySource[source]}
                </span>
              ))}
            </div>
          </>
        )}
      </div>
    </div>
  );
}
