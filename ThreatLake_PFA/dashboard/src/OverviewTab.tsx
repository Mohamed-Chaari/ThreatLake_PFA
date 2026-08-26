import { useState } from "react";
import ActivityChart from "./ActivityChart";
import AttackerDrawer from "./AttackerDrawer";
import type { AlertItem, AttackerProfile } from "./api";
import CategoryChart from "./CategoryChart";
import LiveIndicator from "./LiveIndicator";
import MapView from "./MapView";
import StatStrip from "./StatStrip";
import TopAttackersPreview from "./TopAttackersPreview";
import TopPortsPanel from "./TopPortsPanel";

interface OverviewTabProps {
  alerts: AlertItem[] | null;
  alertsError: string | null;
  attackerProfiles: AttackerProfile[] | null;
  attackerProfilesError: string | null;
  lastUpdatedAt: Date | null;
  pollIntervalSeconds: number;
  newAttackerIps: Set<string>;
  onNavigate: (tab: "attackers" | "map") => void;
}

// The default landing tab: a "command center" grid consolidating what
// the other tabs already show - nothing computed here that Alerts,
// Attackers, or Map don't already compute, just composed into one dense
// view with a couple of compact previews. Every sub-panel takes the same
// alerts/attackerProfiles props App.tsx already fetches (and, while this
// tab is active, polls) - no dedicated Overview-only data or requests.
export default function OverviewTab({
  alerts,
  alertsError,
  attackerProfiles,
  attackerProfilesError,
  lastUpdatedAt,
  pollIntervalSeconds,
  newAttackerIps,
  onNavigate,
}: OverviewTabProps) {
  const [selected, setSelected] = useState<AttackerProfile | null>(null);

  return (
    <div className="view-stack">
      <div className="overview-toolbar">
        <StatStrip alerts={alerts} />
        <LiveIndicator lastUpdatedAt={lastUpdatedAt} intervalSeconds={pollIntervalSeconds} />
      </div>

      <div className="overview-grid overview-grid-3">
        <div className="card">
          {alerts === null ? (
            <p className="hint panel-section">Loading...</p>
          ) : alertsError ? (
            <p className="error panel-section">{alertsError}</p>
          ) : (
            <ActivityChart alerts={alerts} />
          )}
        </div>

        <div className="card">
          {alerts === null ? (
            <p className="hint panel-section">Loading...</p>
          ) : alertsError ? (
            <p className="error panel-section">{alertsError}</p>
          ) : (
            <TopPortsPanel alerts={alerts} />
          )}
        </div>

        <div className="card">
          {alerts === null ? (
            <p className="hint panel-section">Loading...</p>
          ) : alertsError ? (
            <p className="error panel-section">{alertsError}</p>
          ) : (
            <CategoryChart alerts={alerts} />
          )}
        </div>
      </div>

      <div className="overview-grid overview-grid-2">
        <div className="card panel">
          <div className="panel-header">
            <span className="stat-label">Top attackers</span>
            <button type="button" className="text-link" onClick={() => onNavigate("attackers")}>
              View all →
            </button>
          </div>
          <TopAttackersPreview
            profiles={attackerProfiles}
            error={attackerProfilesError}
            onSelect={setSelected}
            newSrcIps={newAttackerIps}
          />
        </div>

        <div className="card panel">
          <div className="panel-header">
            <span className="stat-label">Attacker map</span>
            <button type="button" className="text-link" onClick={() => onNavigate("map")}>
              View full map →
            </button>
          </div>
          <MapView profiles={attackerProfiles} error={attackerProfilesError} compact />
        </div>
      </div>

      <AttackerDrawer profile={selected} onClose={() => setSelected(null)} />
    </div>
  );
}
