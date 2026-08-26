import "leaflet/dist/leaflet.css";
import { CircleMarker, MapContainer, Popup, TileLayer } from "react-leaflet";
import type { AttackerProfile } from "./api";

interface MapViewProps {
  profiles: AttackerProfile[] | null;
  error: string | null;
  // Overview's preview card renders this same component at a smaller
  // fixed height and drops the resolution-count hint line, which reads
  // as clutter at preview size - the full Map tab keeps both.
  compact?: boolean;
}

// A single circle marker's radius, in pixels, scaled by how much activity
// this attacker generated - sqrt rather than linear so one very noisy
// attacker doesn't visually swallow every other dot on the map.
function markerRadius(totalEvents: number): number {
  return Math.min(6 + Math.sqrt(totalEvents) * 2, 24);
}

// Every attacker_profiles row that resolved to a real lat/lon (via
// threatlake.enrichment.geo.GeoEnricher, MaxMind GeoLite2), plotted as a
// marker on an OpenStreetMap base layer. No clustering, no filters, no
// heatmap - one clean map, real markers, real data. Used both as the
// full Map tab and, at `compact` size, as Overview's map preview card -
// same component, same data, just a smaller container.
export default function MapView({ profiles, error, compact = false }: MapViewProps) {
  if (error) {
    return <p className="error">Could not load attacker profiles: {error}</p>;
  }
  if (profiles === null) {
    return <p className="hint">Loading map...</p>;
  }

  const located = profiles.filter(
    (p): p is AttackerProfile & { latitude: number; longitude: number } =>
      p.latitude !== null && p.longitude !== null,
  );

  // A single-attacker map (the drawer's mini-map) centers and zooms in
  // on that one point instead of showing the whole-world default view -
  // a world view for one dot would waste almost all of the small
  // container. `key` forces MapContainer to remount (and re-apply
  // center/zoom) when the selected attacker changes, since react-leaflet
  // only applies center/zoom on initial mount, not on prop updates.
  const isSinglePoint = located.length === 1;
  const center: [number, number] = isSinglePoint ? [located[0].latitude, located[0].longitude] : [20, 10];
  const zoom = isSinglePoint ? 5 : compact ? 1 : 2;

  return (
    <div className="map-view">
      {!compact && (
        <p className="hint">
          {located.length} of {profiles.length} attacker IPs resolved to a real location.
        </p>
      )}
      <MapContainer
        key={isSinglePoint ? located[0].src_ip : "world"}
        center={center}
        zoom={zoom}
        className={compact ? "map-container map-container-compact" : "map-container"}
      >
        <TileLayer
          attribution='&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors'
          url="https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png"
        />
        {located.map((attacker) => (
          <CircleMarker
            key={attacker.src_ip}
            center={[attacker.latitude, attacker.longitude]}
            radius={compact ? markerRadius(attacker.total_events) * 0.6 : markerRadius(attacker.total_events)}
            pathOptions={{ color: "var(--amber)", fillColor: "var(--amber)", fillOpacity: 0.5 }}
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
