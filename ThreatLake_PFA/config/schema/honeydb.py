"""Explicit PySpark schema for HoneyDB community sensor-data events.

HoneyDB (https://honeydb.io) exposes a REST endpoint,
``GET /api/sensor-data``, that returns real events submitted by every
sensor in its community honeypot network (not just one's own - that
requires a registered sensor and a separate ``/mydata`` variant this
project does not use). One JSON object per event, all fields flat (no
per-eventid nesting like Cowrie).

Sourced directly from a real, authenticated response captured against
``https://honeydb.io/api/sensor-data?sensor-data-date=<date>``, retrieved
2026-08-26 - not from HoneyDB's own docs (their public Postman page
renders no static content to read), so every field below was seen in
real payloads, not guessed from a spec:

CONFIRMED (every field observed on every event, across SSH/Telnet/FTP/
HTTP/RDP/SIP/SNMP/Modbus/MSSQL/PostgreSQL/Redis/LDAP/TFTP services):
  date_time   "YYYY-MM-DD HH:MM:SS.mmm" (millisecond precision, no
              explicit UTC marker - see transform/silver/honeydb.py for
              why it is nonetheless treated as UTC).
  date, time  Redundant decompositions of date_time (date="YYYY-MM-DD",
              time="HH:MM:SS") - not carried into silver, see that
              module.
  millisecond Redundant with date_time's own ".mmm" suffix.
  session     A UUID grouping every event from one connection.
  protocol    "TCP" or "UDP" (transport layer, not the service).
  event       "CONNECT", "INFO", "RX", or "TX" observed live - HoneyDB's
              own event taxonomy, not Cowrie's eventid strings.
  service     The emulated service name, e.g. "SSH", "FTP", "RDP".
  remote_host The attacker's IP.
  data        Hex-encoded raw bytes exchanged on the connection (empty
              string on a bare CONNECT with nothing sent yet).
  bytes       Length of the decoded ``data`` payload, in bytes.
  data_hash   Hash of ``data`` (HoneyDB's own, not recomputed here) -
              this project's ``payload_hash`` mapping source.
"""

from __future__ import annotations

from pyspark.sql.types import IntegerType, LongType, StringType, StructField, StructType

SCHEMA = StructType(
    [
        StructField("date_time", StringType(), True),
        StructField("date", StringType(), True),
        StructField("time", StringType(), True),
        StructField("millisecond", IntegerType(), True),
        StructField("session", StringType(), True),
        StructField("protocol", StringType(), True),
        StructField("event", StringType(), True),
        StructField("service", StringType(), True),
        StructField("remote_host", StringType(), True),
        StructField("data", StringType(), True),
        StructField("bytes", LongType(), True),
        StructField("data_hash", StringType(), True),
    ]
)
