import { useEffect, useState } from "react";
import { fetchAlerts, type AlertItem } from "./api";

// One page: a table of every alert threatlake.api.routers.alerts.list_alerts
// currently returns - both detectors' flagged events, in one place, with
// alert_source showing which one(s) fired for each row.
export default function AlertsView() {
  const [alerts, setAlerts] = useState<AlertItem[] | null>(null);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    fetchAlerts()
      .then((response) => setAlerts(response.items))
      .catch((err: Error) => setError(err.message));
  }, []);

  if (error) {
    return <p className="error">Could not load alerts: {error}</p>;
  }
  if (alerts === null) {
    return <p>Loading alerts...</p>;
  }
  if (alerts.length === 0) {
    return <p>No alerts yet - run scripts/run_pipeline.py first.</p>;
  }

  return (
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
        {alerts.map((alert) => (
          <tr key={alert.event_id}>
            <td>{alert.event_time ?? "-"}</td>
            <td>{alert.src_ip ?? "-"}</td>
            <td>{alert.dst_port ?? "-"}</td>
            <td>{alert.attack_category ?? "-"}</td>
            <td>{alert.severity ?? "-"}</td>
            <td>
              <span className={`badge badge-${alert.alert_source ?? "none"}`}>
                {alert.alert_source ?? "-"}
              </span>
            </td>
            <td>{alert.anomaly_score?.toFixed(3) ?? "-"}</td>
          </tr>
        ))}
      </tbody>
    </table>
  );
}
