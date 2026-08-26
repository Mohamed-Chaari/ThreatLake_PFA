// A small, fixed categorical palette shared by CategoryChart (the donut
// on Overview) and AttackerDrawer (per-attacker category badges) - one
// place so a category always reads as the same color everywhere it
// appears. Deliberately non-purple, consistent with the rest of the
// theme's restrained amber/blue/red/muted-tone language (see App.css's
// design-tokens comment).
//
// threatlake.transform.silver.schema.ATTACK_CATEGORIES lists 11 possible
// values; this covers the ones cowrie's own mapper actually produces
// (threatlake.transform.silver.cowrie) plus a couple of others in the
// full taxonomy, with a stable neutral fallback for anything else so a
// category never renders without a color.
const CATEGORY_COLORS: Record<string, string> = {
  connection: "#d99a3d", // --amber
  credential_access: "#f0605a", // --both
  command_execution: "#4ac9f0", // --ml
  malware_delivery: "#e0824a",
  session_summary: "#6fae8a",
  service_probe: "#8a93a6",
  recon: "#c9a86a",
  network_intrusion: "#e05a8a",
  service_start: "#5a9ee0",
  network_flow: "#7a8aa0",
};

const FALLBACK_COLOR = "#5c6169"; // --text-faint

export function colorForCategory(category: string): string {
  return CATEGORY_COLORS[category] ?? FALLBACK_COLOR;
}
