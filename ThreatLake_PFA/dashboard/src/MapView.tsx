import "leaflet/dist/leaflet.css";
import { useEffect, useState } from "react";
import { CircleMarker, MapContainer, Popup, TileLayer } from "react-leaflet";
import { fetchAttackerProfiles, type AttackerProfile } from "./api";

// A single circle marker's radius, in pixels, scaled by how much activity
// this attacker generated - sqrt rather than linear so one very noisy
// attacker doesn't visually swallow every other dot on the map.
function markerRadius(totalEvents: number): number {
  return Math.min(6 + Math.sqrt(totalEvents) * 2, 24);
}

// One tab: every attacker_profiles row that resolved to a real lat/lon
// (via threatlake.enrichment.geo.GeoEnricher, MaxMind GeoLite2), plotted
// as a marker on an OpenStreetMap base layer. No clustering, no filters,
// no heatmap - one clean map, real markers, real data.
export default function MapView() {
  const [profiles, setProfiles] = useState<AttackerProfile[] | null>(null);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    fetchAttackerProfiles()
      .then((response) => setProfiles(response.items))
      .catch((err: Error) => setError(err.message));
  }, []);

  if (error) {
    return <p className="error">Could not load attacker profiles: {error}</p>;
  }
  if (profiles === null) {
    return <p>Loading map...</p>;
  }

  const located = profiles.filter(
    (p): p is AttackerProfile & { latitude: number; longitude: number } =>
      p.latitude !== null && p.longitude !== null,
  );

  return (
    <div className="map-view">
      <p className="hint">
        {located.length} of {profiles.length} attacker IPs resolved to a real location.
      </p>
      <MapContainer center={[20, 10]} zoom={2} className="map-container">
        <TileLayer
          attribution='&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors'
          url="https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png"
        />
        {located.map((attacker) => (
          <CircleMarker
            key={attacker.src_ip}
            center={[attacker.latitude, attacker.longitude]}
            radius={markerRadius(attacker.total_events)}
            pathOptions={{ color: "#f0c94a", fillColor: "#f0c94a", fillOpacity: 0.5 }}
          >
            <Popup>
              <strong>{attacker.src_ip}</strong>
              <br />
              {attacker.total_events} event{attacker.total_events === 1 ? "" : "s"}
            </Popup>
          </CircleMarker>
        ))}
      </MapContainer>
    </div>
  );
}
