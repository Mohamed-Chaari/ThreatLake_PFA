import { useMemo, useState } from "react";
import ActivityChart from "./ActivityChart";
import type { AlertItem } from "./api";
import SearchInput from "./SearchInput";
import TopPortsPanel from "./TopPortsPanel";

interface AlertsViewProps {
  alerts: AlertItem[] | null;
  error: string | null;
}

// A table of every alert threatlake.api.routers.alerts.list_alerts
// currently returns - both detectors' flagged events, in one place, with
// alert_source showing which one(s) fired for each row. Two summary
// panels (hourly activity, top targeted ports) sit above it, both pure
// client-side aggregations over the same already-fetched list.
//
// Takes the already-fetched list as a prop rather than fetching its own
// copy - App.tsx owns the single GET /alerts call so the stat strip above
// the tabs and this table are always looking at the exact same response,
// not two independent (and potentially inconsistent) fetches.
export default function AlertsView({ alerts, error }: AlertsViewProps) {
  const [search, setSearch] = useState("");

  const filtered = useMemo(() => {
    if (!alerts) return null;
    const needle = search.trim();
    if (!needle) return alerts;
    return alerts.filter((a) => a.src_ip?.includes(needle));
  }, [alerts, search]);

  if (error) {
    return <p className="error">Could not load alerts: {error}</p>;
  }
  if (alerts === null || filtered === null) {
    return <p className="hint">Loading alerts...</p>;
  }
  if (alerts.length === 0) {
    return <p className="hint">No alerts yet - run scripts/run_pipeline.py first.</p>;
  }

  return (
    <div className="view-stack">
      <div className="summary-grid">
        <div className="card">
          <ActivityChart alerts={alerts} />
        </div>
        <div className="card">
          <TopPortsPanel alerts={alerts} />
        </div>
      </div>

      <SearchInput value={search} onChange={setSearch} placeholder="Filter by src IP..." />

      <div className="card">
        {filtered.length === 0 ? (
          <p className="hint panel-section">No alerts match "{search}".</p>
        ) : (
          <table className="alerts-table">
            <thead>
              <tr>
                <th>Event time</th>
                <th>Src IP</th>
                <th>Dst port</th>
                <th>Category</th>
                <th>Severity</th>
                <th>Alert source</th>
                <th>Anomaly score</th>
              </tr>
            </thead>
            <tbody>
              {filtered.map((alert) => (
                <tr key={alert.event_id}>
                  <td className="mono">{alert.event_time ?? "-"}</td>
                  <td className="mono">{alert.src_ip ?? "-"}</td>
                  <td className="mono">{alert.dst_port ?? "-"}</td>
                  <td>{alert.attack_category ?? "-"}</td>
                  <td>{alert.severity ?? "-"}</td>
                  <td>
                    <span className={`badge badge-${alert.alert_source ?? "none"}`}>
                      {alert.alert_source ?? "-"}
                    </span>
                  </td>
                  <td className="mono">{alert.anomaly_score?.toFixed(3) ?? "-"}</td>
                </tr>
              ))}
            </tbody>
          </table>
        )}
      </div>
    </div>
  );
}
