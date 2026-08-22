"""ThreatLake PFA - a focused, local-only subset of ThreatLake AI.

One honeypot source (cowrie), a batch Bronze -> Silver -> Gold Delta
lakehouse, two attack detectors (IsolationForest + a port-scan rule), and
a small read-only API + dashboard. See README.md for scope and
ARCHITECTURE.md for what was left out and why.
"""

__version__ = "0.1.0"
