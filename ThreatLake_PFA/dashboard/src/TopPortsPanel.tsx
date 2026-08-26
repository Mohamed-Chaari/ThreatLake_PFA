import type { AlertItem } from "./api";

interface TopPortsPanelProps {
  alerts: AlertItem[];
}

const MAX_PORTS = 5;

// Groups alerts by dst_port (skipping the null ones - e.g. cowrie command
// events carry no port context) and returns the top MAX_PORTS by count,
// descending. Pure client-side aggregation over the alerts list App
// already fetched - no new request, no new backend logic.
function topPorts(alerts: AlertItem[]): { port: number; count: number }[] {
  const counts = new Map<number, number>();
  for (const alert of alerts) {
    if (alert.dst_port === null) continue;
    counts.set(alert.dst_port, (counts.get(alert.dst_port) ?? 0) + 1);
  }
  return Array.from(counts.entries())
    .map(([port, count]) => ({ port, count }))
    .sort((a, b) => b.count - a.count)
    .slice(0, MAX_PORTS);
}

// A horizontal bar list of the most-hit destination ports - same idea as
// ActivityChart (plain CSS bars sized by percentage, no charting
// library), just laid out sideways since a ranked top-N list reads more
// naturally as horizontal bars than vertical columns.
export default function TopPortsPanel({ alerts }: TopPortsPanelProps) {
  const ports = topPorts(alerts);

  return (
    <div className="panel-section">
      <span className="stat-label">Top targeted ports</span>
      {ports.length === 0 ? (
        <p className="hint">No port data in the current alerts.</p>
      ) : (
        <div className="port-bars">
          {ports.map(({ port, count }) => (
            <div key={port} className="port-bar-row">
              <span className="port-bar-label">{port}</span>
              <div className="port-bar-track">
                <div
                  className="port-bar-fill"
                  style={{ width: `${(count / ports[0].count) * 100}%` }}
                />
              </div>
              <span className="port-bar-count">{count}</span>
            </div>
          ))}
        </div>
      )}
    </div>
  );
}
