"""Explicit PySpark schema for Cowrie (SSH/Telnet) honeypot JSON logs.

Cowrie writes one JSON object per line, every event sharing a common
envelope plus event-specific fields keyed by ``eventid``.

Sourced from Cowrie's own output documentation
(https://github.com/cowrie/cowrie/blob/master/docs/OUTPUT.rst), retrieved
2026-08-02. Fields below are grouped by confidence:

CONFIRMED (documented shared envelope + documented per-eventid fields):
  eventid, message, sensor, timestamp, session, src_ip, src_port, dst_ip,
  dst_port, protocol, late
  cowrie.login.success / cowrie.login.failed: username, password,
    fingerprint, key, type
  cowrie.command.input: input, realm
  cowrie.session.file_download: url, outfile, shasum, destfile, duplicate
  cowrie.session.file_upload: filename, outfile, shasum, destfile, duplicate
  cowrie.session.closed / cowrie.log.closed: duration_ms, ttylog, size

NOT INCLUDED (known Cowrie event types whose exact field names were NOT
independently confirmed here - do not assume absence from real output, they
were deliberately left out rather than guessed):
  cowrie.client.version (SSH/Telnet client banner), cowrie.client.kex
  (key-exchange/algorithm negotiation, incl. hassh fingerprint fields),
  cowrie.direct-tcpip.request/data (SSH port-forward tunneling).
Add these once confirmed against Cowrie source or live T-Pot output.
"""

from __future__ import annotations

from pyspark.sql.types import BooleanType, DoubleType, IntegerType, LongType, StringType, StructField, StructType

SCHEMA = StructType(
    [
        # Shared envelope, present on every event.
        StructField("eventid", StringType(), True),
        StructField("message", StringType(), True),
        StructField("sensor", StringType(), True),
        StructField("timestamp", StringType(), True),
        StructField("session", StringType(), True),
        StructField("src_ip", StringType(), True),
        StructField("src_port", IntegerType(), True),
        StructField("dst_ip", StringType(), True),
        StructField("dst_port", IntegerType(), True),
        StructField("protocol", StringType(), True),  # "ssh" or "telnet"
        StructField("late", BooleanType(), True),
        # cowrie.login.success / cowrie.login.failed
        StructField("username", StringType(), True),
        StructField("password", StringType(), True),
        StructField("fingerprint", StringType(), True),
        StructField("key", StringType(), True),
        StructField("type", StringType(), True),
        # cowrie.command.input
        StructField("input", StringType(), True),
        StructField("realm", StringType(), True),
        # cowrie.session.file_download / cowrie.session.file_upload
        StructField("url", StringType(), True),
        StructField("filename", StringType(), True),
        StructField("outfile", StringType(), True),
        StructField("shasum", StringType(), True),
        StructField("destfile", StringType(), True),
        StructField("duplicate", BooleanType(), True),
        # cowrie.session.closed / cowrie.log.closed
        StructField("duration_ms", DoubleType(), True),
        StructField("ttylog", StringType(), True),
        StructField("size", LongType(), True),
    ]
)
