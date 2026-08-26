import { useCallback, useEffect, useRef, useState } from "react";
import AlertsView from "./AlertsView";
import { type AlertItem, type AttackerProfile, fetchAlerts, fetchAttackerProfiles } from "./api";
import AttackersView from "./AttackersView";
import CopilotView from "./CopilotView";
import MapView from "./MapView";
import OverviewTab from "./OverviewTab";

type Tab = "overview" | "alerts" | "attackers" | "map" | "copilot";

//: How often Overview refetches while it's the active tab AND the browser
//: tab is visible - polling, never a push/streaming connection (this
//: project is a batch pipeline by design, see ARCHITECTURE.md).
const POLL_INTERVAL_SECONDS = 30;

// The whole dashboard: a tab switcher over five views, all reading from
// data threatlake.api.app already returns - an overview, an alert list,
// an attacker leaderboard, an attacker map, and the copilot chat panel.
// Nothing more: ThreatLake AI's real dashboard has a sidebar, 8 pages,
// and live websockets - see ARCHITECTURE.md's "Future extensions"
// section for why none of that is here.
//
// GET /alerts and GET /attacker_profiles are each fetched exactly ONCE
// here, not per-view: every tab that needs one reads the same App-level
// state, so switching tabs never re-fetches, and Overview's polling
// (below) refreshes the one shared copy every other tab also sees.
export default function App() {
  const [tab, setTab] = useState<Tab>("overview");

  const [alerts, setAlerts] = useState<AlertItem[] | null>(null);
  const [alertsError, setAlertsError] = useState<string | null>(null);
  const [attackerProfiles, setAttackerProfiles] = useState<AttackerProfile[] | null>(null);
  const [attackerProfilesError, setAttackerProfilesError] = useState<string | null>(null);
  const [lastUpdatedAt, setLastUpdatedAt] = useState<Date | null>(null);
  const [newAttackerIps, setNewAttackerIps] = useState<Set<string>>(new Set());

  // Diffs each new attackerProfiles response against the previous one to
  // find src_ips that weren't there before - drives the row-flash on
  // Overview's Top attackers preview. `null` until the first response has
  // landed, so the very first population is never flagged as "new"
  // (nothing to compare it against yet, not a real change).
  const previousAttackerIpsRef = useRef<Set<string> | null>(null);
  useEffect(() => {
    if (attackerProfiles === null) return;
    const currentIps = new Set(attackerProfiles.map((p) => p.src_ip));
    if (previousAttackerIpsRef.current !== null) {
      const previous = previousAttackerIpsRef.current;
      const added = new Set([...currentIps].filter((ip) => !previous.has(ip)));
      setNewAttackerIps(added);
    }
    previousAttackerIpsRef.current = currentIps;
  }, [attackerProfiles]);

  const refetchAll = useCallback(() => {
    fetchAlerts()
      .then((response) => setAlerts(response.items))
      .catch((err: Error) => setAlertsError(err.message));
    fetchAttackerProfiles()
      .then((response) => setAttackerProfiles(response.items))
      .catch((err: Error) => setAttackerProfilesError(err.message));
    setLastUpdatedAt(new Date());
  }, []);

  // Fetch both once on mount, regardless of which tab is active first -
  // every tab reads from this same state, so it needs to exist up front.
  useEffect(() => {
    refetchAll();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  // Overview's poll: only runs while tab === "overview", and further
  // pauses via the Page Visibility API when the browser tab itself is
  // backgrounded - both are "don't waste requests nobody's looking at",
  // just at two different levels (which app tab vs. which browser tab).
  useEffect(() => {
    if (tab !== "overview") return;

    let intervalId: number | undefined;
    const startPolling = () => {
      if (intervalId !== undefined) return;
      intervalId = window.setInterval(refetchAll, POLL_INTERVAL_SECONDS * 1000);
    };
    const stopPolling = () => {
      if (intervalId !== undefined) {
        window.clearInterval(intervalId);
        intervalId = undefined;
      }
    };
    const handleVisibilityChange = () => {
      if (document.visibilityState === "visible") {
        refetchAll(); // catch up immediately on return, not after a stale wait
        startPolling();
      } else {
        stopPolling();
      }
    };

    if (document.visibilityState === "visible") {
      startPolling();
    }
    document.addEventListener("visibilitychange", handleVisibilityChange);

    return () => {
      stopPolling();
      document.removeEventListener("visibilitychange", handleVisibilityChange);
    };
  }, [tab, refetchAll]);

  return (
    <div className="app">
      <header className="page-header">
        <h1>ThreatLake PFA</h1>
        <p className="subtitle">Batch pipeline dashboard — alerts, attackers, and the SQL copilot</p>
      </header>

      <nav>
        <button className={tab === "overview" ? "active" : ""} onClick={() => setTab("overview")}>
          Overview
        </button>
        <button className={tab === "alerts" ? "active" : ""} onClick={() => setTab("alerts")}>
          Alerts
        </button>
        <button className={tab === "attackers" ? "active" : ""} onClick={() => setTab("attackers")}>
          Attackers
        </button>
        <button className={tab === "map" ? "active" : ""} onClick={() => setTab("map")}>
          Map
        </button>
        <button className={tab === "copilot" ? "active" : ""} onClick={() => setTab("copilot")}>
          Copilot
        </button>
      </nav>

      <main>
        {tab === "overview" && (
          <OverviewTab
            alerts={alerts}
            alertsError={alertsError}
            attackerProfiles={attackerProfiles}
            attackerProfilesError={attackerProfilesError}
            lastUpdatedAt={lastUpdatedAt}
            pollIntervalSeconds={POLL_INTERVAL_SECONDS}
            newAttackerIps={newAttackerIps}
            onNavigate={(target) => setTab(target)}
          />
        )}
        {tab === "alerts" && <AlertsView alerts={alerts} error={alertsError} />}
        {tab === "attackers" && (
          <AttackersView profiles={attackerProfiles} error={attackerProfilesError} />
        )}
        {tab === "map" && <MapView profiles={attackerProfiles} error={attackerProfilesError} />}
        {tab === "copilot" && <CopilotView />}
      </main>
    </div>
  );
}
