import type { AlertItem } from "./api";

interface ActivityChartProps {
  alerts: AlertItem[];
}

interface HourBucket {
  hour: string;
  label: string;
  count: number;
}

// Groups alerts by the hour of their event_time (e.g. all events between
// 09:00:00 and 09:59:59 land in one "09:00" bucket) and returns buckets
// in chronological order. Pure client-side aggregation over the alerts
// list App.tsx already fetched - no new request, no new backend logic.
function bucketByHour(alerts: AlertItem[]): HourBucket[] {
  const counts = new Map<string, number>();
  for (const alert of alerts) {
    if (!alert.event_time) continue;
    const hour = alert.event_time.slice(0, 13); // "2026-08-20T09"
    counts.set(hour, (counts.get(hour) ?? 0) + 1);
  }
  return Array.from(counts.entries())
    .sort(([a], [b]) => a.localeCompare(b))
    .map(([hour, count]) => ({ hour, label: `${hour.slice(11, 13)}:00`, count }));
}

// A small bar-per-hour strip above the alerts table - plain CSS divs
// sized by percentage height, no charting library. Turns the already-
// fetched alerts list into something visual with zero extra requests.
export default function ActivityChart({ alerts }: ActivityChartProps) {
  const buckets = bucketByHour(alerts);
  if (buckets.length === 0) {
    return null;
  }

  const maxCount = Math.max(...buckets.map((b) => b.count));

  return (
    <div className="panel-section">
      <span className="stat-label">Alerts by hour</span>
      <div className="activity-bars">
        {buckets.map((bucket) => (
          <div key={bucket.hour} className="activity-bar-col" title={`${bucket.label} - ${bucket.count} alert${bucket.count === 1 ? "" : "s"}`}>
            <span className="activity-bar-count">{bucket.count}</span>
            <div className="activity-bar-track">
              <div
                className="activity-bar-fill"
                style={{ height: `${Math.max((bucket.count / maxCount) * 100, 8)}%` }}
              />
            </div>
            <span className="activity-bar-label">{bucket.label}</span>
          </div>
        ))}
      </div>
    </div>
  );
}
