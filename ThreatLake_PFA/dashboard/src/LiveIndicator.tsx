import { useEffect, useState } from "react";

interface LiveIndicatorProps {
  lastUpdatedAt: Date | null;
  intervalSeconds: number;
}

function secondsAgo(since: Date): number {
  return Math.max(0, Math.floor((Date.now() - since.getTime()) / 1000));
}

// Honest about what this is: POLLING, not a push/streaming connection -
// ThreatLake PFA is a batch pipeline by design (see ARCHITECTURE.md), so
// this never claims "real-time". The dot pulses and the text counts up
// on its own 1s tick purely for the "Xs ago" display; the actual
// GET /alerts + GET /attacker_profiles refetch that updates `lastUpdatedAt`
// happens on Overview's own `intervalSeconds` timer (App.tsx), not here.
export default function LiveIndicator({ lastUpdatedAt, intervalSeconds }: LiveIndicatorProps) {
  const [, setTick] = useState(0);

  useEffect(() => {
    const id = setInterval(() => setTick((t) => t + 1), 1000);
    return () => clearInterval(id);
  }, []);

  return (
    <div className="live-indicator" title={`Auto-refreshing every ${intervalSeconds}s while this tab is active - polling, not a live push connection`}>
      <span className="live-dot" />
      <span>{lastUpdatedAt ? `Updated ${secondsAgo(lastUpdatedAt)}s ago` : "Loading..."}</span>
    </div>
  );
}
