#!/usr/bin/env python3
"""
WebSocket Twist Client (relay leg)
==================================

Robot-side peer for the **WebSocket** browser leg.  The browser opens a
WebSocket to the Go relay's data hub (``/ws/data?role=browser``); this client
connects to the matching ``/ws/data?role=python`` endpoint, and the Go relay
forwards frames in both directions without inspecting them.

Architecture:
    Browser ──WS──► Go /ws/data ──WS──► Python (this client)
    Browser ◄──WS── Go /ws/data ◄──WS── Python (this client)

This client is functionally identical to the WebTransport variant
(``python_client_webtransport/main.py``) — both reach the Go relay over a
WebSocket because the relay terminates HTTP/3 datagrams there.  They're split
into separate folders so each can be run, logged, and reasoned about
independently of the WebRTC client in ``python_client/``.

Protocol (binary):
    0x01  Twist          Browser → Python (19 + N×8 bytes)
    0x02  TwistAck       Python → Browser (38 bytes)
    0x03  ClockSyncReq   Browser → Python (10 bytes)
    0x04  ClockSyncResp  Python → Browser (26 bytes)
Protocol (json): self-describing envelopes, see ``codec.py``.

Usage:
    python main.py [--data ws://localhost:8443/ws/data] [--format binary|json]
                   [--topic /cmd_vel]

Dependencies:
    pip install aiohttp
"""

import asyncio
import argparse
import csv
import logging
import os
import signal
import ssl
import sys
from collections import deque
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional, Callable

import aiohttp

from twist_protocol import TwistWithLatency
from codec import make_codec
from session import TeleopSession

# ─── Logging ──────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("WebSocket-TwistClient")


# ─── Optional ROS2 ────────────────────────────────────────────────────────────

ROS2_AVAILABLE = False
try:
    import rclpy
    from geometry_msgs.msg import Twist
    ROS2_AVAILABLE = True
    logger.info("ROS2 available")
except ImportError:
    logger.info("ROS2 not available (running without robot)")


class ROS2Publisher:
    """Publishes Twist to a ROS2 topic."""

    def __init__(self, topic: str):
        self.topic = topic
        self._node = None
        self._pub = None
        self._ok = False

    def init(self) -> bool:
        if not ROS2_AVAILABLE:
            return False
        try:
            if not rclpy.ok():
                rclpy.init()
            self._node = rclpy.create_node('twist_bridge')
            self._pub = self._node.create_publisher(Twist, self.topic, 10)
            self._ok = True
            logger.info(f"ROS2 publisher: {self.topic}")
            return True
        except Exception as e:
            logger.error(f"ROS2 init failed: {e}")
            return False

    def publish(self, twist: TwistWithLatency):
        if not self._ok:
            return
        msg = Twist()
        msg.linear.x = twist.linear_x
        msg.linear.y = twist.linear_y
        msg.linear.z = twist.linear_z
        msg.angular.x = twist.angular_x
        msg.angular.y = twist.angular_y
        msg.angular.z = twist.angular_z
        self._pub.publish(msg)

    def shutdown(self):
        if self._node:
            self._node.destroy_node()
        if rclpy.ok():
            rclpy.shutdown()


# ─── Stats ────────────────────────────────────────────────────────────────────

@dataclass
class Stats:
    def __init__(self, window: int = 100):
        self._latencies = deque(maxlen=window)
        self._decode_us = deque(maxlen=window)
        self._process_us = deque(maxlen=window)
        self._encode_us = deque(maxlen=window)
        self.rx_count = 0
        self.ack_count = 0

    def record(self, latency_ms: float, decode_us: int, process_us: int, encode_us: int):
        if latency_ms >= 0:
            self._latencies.append(latency_ms)
        self._decode_us.append(decode_us)
        self._process_us.append(process_us)
        self._encode_us.append(encode_us)
        self.rx_count += 1

    def avg(self, d: deque) -> float:
        return sum(d) / len(d) if d else 0.0

    def __str__(self) -> str:
        return (
            f"rx={self.rx_count} acks={self.ack_count} "
            f"lat={self.avg(self._latencies):.1f}ms "
            f"dec={self.avg(self._decode_us):.0f}μs "
            f"proc={self.avg(self._process_us):.0f}μs "
            f"enc={self.avg(self._encode_us):.0f}μs"
        )


# ─── Timestamp File Logger ────────────────────────────────────────────────────

class TimestampFileLogger:
    """CSV per-message timestamp log. Same schema as python_client/main.py."""

    ALL_FIELDS = [
        'time_iso', 'type', 'seq', 'msg_id',
        't1_browser_ms', 't3_python_rx_ms', 't4_python_ack_ms',
        'approx_lat_ms',
        'decode_us', 'process_us', 'encode_us', 'total_python_us',
        'linear_x', 'linear_y', 'linear_z',
        'angular_x', 'angular_y', 'angular_z',
        't2_python_rx_ms', 't3_python_tx_ms', 'sync_proc_us',
    ]

    def __init__(self, path: str):
        self._path   = path
        self._seq    = 0
        self._fh     = None
        self._writer = None

    def open(self):
        new_file = not os.path.exists(self._path) or os.path.getsize(self._path) == 0
        self._fh = open(self._path, 'a', newline='', encoding='utf-8')
        self._writer = csv.DictWriter(
            self._fh, fieldnames=self.ALL_FIELDS, extrasaction='ignore',
        )
        if new_file:
            self._writer.writeheader()
            self._fh.flush()
        logger.info(f"Timestamp log → {os.path.abspath(self._path)}")

    def close(self):
        if self._fh:
            self._fh.flush()
            self._fh.close()
            self._fh = None

    @staticmethod
    def _now_iso() -> str:
        return datetime.now(timezone.utc).isoformat(timespec='milliseconds')

    def log_twist(self, twist) -> None:
        if self._writer is None:
            return
        self._seq += 1
        ts       = twist.timestamps
        total_us = ts.python_decode_us + ts.python_process_us + ts.python_encode_us
        self._writer.writerow({
            'time_iso':         self._now_iso(),
            'type':             'TWIST',
            'seq':              self._seq,
            'msg_id':           twist.message_id,
            't1_browser_ms':    ts.t1_browser_send,
            't3_python_rx_ms':  ts.t3_python_rx,
            't4_python_ack_ms': ts.t4_python_ack,
            'approx_lat_ms':    ts.t3_python_rx - ts.t1_browser_send,
            'decode_us':        ts.python_decode_us,
            'process_us':       ts.python_process_us,
            'encode_us':        ts.python_encode_us,
            'total_python_us':  total_us,
            'linear_x':         round(twist.linear_x,  6),
            'linear_y':         round(twist.linear_y,  6),
            'linear_z':         round(twist.linear_z,  6),
            'angular_x':        round(twist.angular_x, 6),
            'angular_y':        round(twist.angular_y, 6),
            'angular_z':        round(twist.angular_z, 6),
        })
        self._fh.flush()

    def log_sync(self, t1: int, t2: int, t3: int) -> None:
        if self._writer is None:
            return
        self._seq += 1
        self._writer.writerow({
            'time_iso':         self._now_iso(),
            'type':             'SYNC',
            'seq':              self._seq,
            't1_browser_ms':    t1,
            't2_python_rx_ms':  t2,
            't3_python_tx_ms':  t3,
            'sync_proc_us':     (t3 - t2) * 1000,
        })
        self._fh.flush()


# ─── WebSocket Twist Client ──────────────────────────────────────────────────

class WebSocketTwistClient:
    """Robot-side peer for the WebSocket browser leg.

    The Go relay terminates the browser's WebSocket on /ws/data?role=browser
    and pumps frames to/from this client on /ws/data?role=python. The relay
    never rewrites the payload, so the wire protocol (and the browser↔Python
    clock-sync) is identical to the WebRTC P2P client — only the number of
    network hops differs.
    """

    def __init__(
        self,
        data_url: str,
        on_twist: Optional[Callable] = None,
        ros2_topic: Optional[str] = None,
        ts_logger: Optional['TimestampFileLogger'] = None,
        codec_name: str = "binary",
    ):
        if "?" in data_url:
            self._data_url = f"{data_url}&role=python"
        else:
            self._data_url = f"{data_url}?role=python"

        self.on_twist = on_twist
        self.stats    = Stats()
        self._ts_log  = ts_logger
        self._codec   = make_codec(codec_name)

        self._session: Optional[TeleopSession] = None
        self._session_ros2 = ROS2Publisher(ros2_topic) if ros2_topic else None

        self._session_aiohttp = None
        self._ws = None
        self._shutdown = asyncio.Event()

    async def run(self):
        logger.info(f"Connecting to data hub: {self._data_url}")
        if self._session_ros2:
            self._session_ros2.init()
        while not self._shutdown.is_set():
            try:
                await self._relay_loop()
            except Exception as e:
                logger.error(f"Relay error: {e}")
                await asyncio.sleep(3)
                logger.info("Reconnecting to data hub...")
        await self._cleanup()

    async def _relay_loop(self):
        self._session_aiohttp = aiohttp.ClientSession()
        try:
            ssl_ctx = None
            if self._data_url.startswith("wss://"):
                cafile = os.environ.get("TLS_CA")
                if cafile:
                    ssl_ctx = ssl.create_default_context(cafile=cafile)
                else:
                    ssl_ctx = ssl.create_default_context()
                    ssl_ctx.check_hostname = False
                    ssl_ctx.verify_mode = ssl.CERT_NONE
            self._ws = await self._session_aiohttp.ws_connect(
                self._data_url, heartbeat=25.0, ssl=ssl_ctx
            )
            logger.info("Data hub connected. Waiting for browser frames...")

            async def _send(payload):
                if isinstance(payload, str):
                    await self._ws.send_str(payload)
                else:
                    await self._ws.send_bytes(payload)

            self._session = TeleopSession(
                codec=self._codec,
                send=_send,
                on_twist=self.on_twist,
                ros2=self._session_ros2,
                ts_log=self._ts_log,
                stats=self.stats,
                log=logger,
            )

            async for msg in self._ws:
                if self._shutdown.is_set():
                    break
                if msg.type == aiohttp.WSMsgType.BINARY:
                    await self._session.handle_frame(msg.data)
                elif msg.type == aiohttp.WSMsgType.TEXT:
                    await self._session.handle_frame(msg.data)
                elif msg.type in (aiohttp.WSMsgType.CLOSE, aiohttp.WSMsgType.CLOSED,
                                  aiohttp.WSMsgType.ERROR):
                    logger.info("Data hub disconnected")
                    break
        finally:
            if self._ws and not self._ws.closed:
                await self._ws.close()
            if self._session_aiohttp:
                await self._session_aiohttp.close()
            self._ws = None
            self._session_aiohttp = None
            self._session = None

    async def _cleanup(self):
        if self._ws and not self._ws.closed:
            await self._ws.close()
        if self._session_aiohttp:
            await self._session_aiohttp.close()
        if self._session_ros2:
            self._session_ros2.shutdown()

    def stop(self):
        self._shutdown.set()


# ─── Main ─────────────────────────────────────────────────────────────────────

def parse_args():
    p = argparse.ArgumentParser(description="WebSocket Twist Client (relay leg)")
    p.add_argument(
        "--format", "-f",
        choices=["binary", "json"],
        default="binary",
        help="Wire codec. Must match the operator's selection in the browser. Default: binary."
    )
    p.add_argument(
        "--data", "-d",
        default="ws://localhost:8443/ws/data",
        help="Go data-hub URL (ws:// or wss://). Default: ws://localhost:8443/ws/data"
    )
    p.add_argument(
        "--ca-cert",
        default=None,
        metavar="PATH",
        help="CA certificate for verifying the TLS server cert (wss:// only). "
             "If omitted with wss://, certificate verification is skipped (dev only)."
    )
    p.add_argument("--topic", "-t", default=None, help="ROS2 topic name")
    p.add_argument("--verbose", "-v", action="store_true")
    p.add_argument(
        "--log-file", "-l",
        default="teleop_timestamps.csv",
        metavar="PATH",
        help="CSV file for per-message timestamp log (default: teleop_timestamps.csv)"
    )
    p.add_argument(
        "--no-log-file",
        action="store_true",
        help="Disable CSV timestamp logging entirely"
    )
    return p.parse_args()


async def main():
    args = parse_args()
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    if args.ca_cert:
        os.environ["TLS_CA"] = args.ca_cert

    tls_note = ""
    if args.data.startswith("wss://"):
        tls_note = f"  TLS CA     : {args.ca_cert or 'skipping verification (dev)'}\n"

    print()
    print(f"  Transport  : WebSocket  (relay leg)")
    print(f"  Format     : {args.format}")
    print(f"  Endpoint   : {args.data}")
    print(f"  ROS2 topic : {args.topic or 'disabled'}")
    print(f"  Timestamp log : {'disabled' if args.no_log_file else args.log_file}")
    if tls_note:
        print(tls_note, end="")
    print()

    ts_logger = None
    if not args.no_log_file:
        ts_logger = TimestampFileLogger(args.log_file)
        ts_logger.open()

    client = WebSocketTwistClient(
        data_url=args.data,
        ros2_topic=args.topic,
        ts_logger=ts_logger,
        codec_name=args.format,
    )

    shutdown = asyncio.Event()
    loop = asyncio.get_event_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, shutdown.set)

    async def stats_loop():
        while not shutdown.is_set():
            await asyncio.sleep(5.0)
            logger.info(f"Stats: {client.stats}")

    stats_task = asyncio.create_task(stats_loop())
    client_task = asyncio.create_task(client.run())

    await shutdown.wait()
    client.stop()
    stats_task.cancel()
    client_task.cancel()
    try:
        await asyncio.gather(stats_task, client_task, return_exceptions=True)
    except Exception:
        pass

    if ts_logger:
        ts_logger.close()
        logger.info(f"Timestamp log closed: {args.log_file}")

    return 0


if __name__ == "__main__":
    try:
        sys.exit(asyncio.run(main()))
    except KeyboardInterrupt:
        sys.exit(0)
