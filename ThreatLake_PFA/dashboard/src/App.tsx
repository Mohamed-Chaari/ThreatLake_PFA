import { useState } from "react";
import AlertsView from "./AlertsView";
import CopilotView from "./CopilotView";
import MapView from "./MapView";

type Tab = "alerts" | "map" | "copilot";

// The whole dashboard: a tab switcher between the three views the PFA
// scope asked for - an alert list, an attacker map, and the copilot chat
// panel. Nothing more: ThreatLake AI's real dashboard has a sidebar, 8
// pages, and live websockets - see ARCHITECTURE.md's "Future extensions"
// section for why none of that is here.
export default function App() {
  const [tab, setTab] = useState<Tab>("alerts");

  return (
    <div className="app">
      <header>
        <h1>ThreatLake PFA</h1>
        <nav>
          <button className={tab === "alerts" ? "active" : ""} onClick={() => setTab("alerts")}>
            Alerts
          </button>
          <button className={tab === "map" ? "active" : ""} onClick={() => setTab("map")}>
            Map
          </button>
          <button className={tab === "copilot" ? "active" : ""} onClick={() => setTab("copilot")}>
            Copilot
          </button>
        </nav>
      </header>
      <main>
        {tab === "alerts" && <AlertsView />}
        {tab === "map" && <MapView />}
        {tab === "copilot" && <CopilotView />}
      </main>
    </div>
  );
}
