import type { AlertItem } from "./api";
import { colorForCategory } from "./categoryColors";

interface CategoryChartProps {
  alerts: AlertItem[];
}

interface CategorySlice {
  category: string;
  count: number;
  pct: number;
}

const RADIUS = 40;
const STROKE_WIDTH = 16;
const CIRCUMFERENCE = 2 * Math.PI * RADIUS;

// Groups alerts by attack_category and returns slices sorted by count
// descending - pure client-side aggregation over the already-fetched
// alerts list, same as ActivityChart/TopPortsPanel.
function categorySlices(alerts: AlertItem[]): CategorySlice[] {
  const counts = new Map<string, number>();
  for (const alert of alerts) {
    const category = alert.attack_category ?? "other";
    counts.set(category, (counts.get(category) ?? 0) + 1);
  }
  const total = alerts.length;
  return Array.from(counts.entries())
    .map(([category, count]) => ({ category, count, pct: count / total }))
    .sort((a, b) => b.count - a.count);
}

// A real SVG donut chart - no charting library, just stacked <circle>
// strokes: each slice is one full-circumference circle whose
// stroke-dasharray shows only its own arc length, offset along the
// circumference by every slice drawn before it. Same underlying idea as
// ActivityChart/TopPortsPanel's plain-CSS bars, just a different shape.
export default function CategoryChart({ alerts }: CategoryChartProps) {
  const slices = categorySlices(alerts);
  if (slices.length === 0) {
    return (
      <div className="panel-section">
        <span className="stat-label">Alerts by category</span>
        <p className="hint">No category data in the current alerts.</p>
      </div>
    );
  }

  let offset = 0;

  return (
    <div className="panel-section">
      <span className="stat-label">Alerts by category</span>
      <div className="donut-row">
        <svg viewBox="0 0 100 100" className="donut-chart" role="img" aria-label="Alerts by attack category">
          <circle cx="50" cy="50" r={RADIUS} fill="none" stroke="var(--panel-raised)" strokeWidth={STROKE_WIDTH} />
          {slices.map((slice) => {
            const arcLength = slice.pct * CIRCUMFERENCE;
            const circle = (
              <circle
                key={slice.category}
                cx="50"
                cy="50"
                r={RADIUS}
                fill="none"
                stroke={colorForCategory(slice.category)}
                strokeWidth={STROKE_WIDTH}
                strokeDasharray={`${arcLength} ${CIRCUMFERENCE - arcLength}`}
                strokeDashoffset={-offset}
                transform="rotate(-90 50 50)"
              >
                <title>
                  {slice.category}: {slice.count} ({(slice.pct * 100).toFixed(0)}%)
                </title>
              </circle>
            );
            offset += arcLength;
            return circle;
          })}
          <text x="50" y="47" textAnchor="middle" className="donut-center-value">
            {alerts.length}
          </text>
          <text x="50" y="60" textAnchor="middle" className="donut-center-label">
            alerts
          </text>
        </svg>

        <ul className="donut-legend">
          {slices.map((slice) => (
            <li key={slice.category}>
              <span className="source-dot" style={{ background: colorForCategory(slice.category) }} />
              <span className="donut-legend-label">{slice.category}</span>
              <span className="donut-legend-count">
                {slice.count} · {(slice.pct * 100).toFixed(0)}%
              </span>
            </li>
          ))}
        </ul>
      </div>
    </div>
  );
}
