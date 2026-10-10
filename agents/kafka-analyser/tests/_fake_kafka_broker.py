"""Minimal fake Kafka broker on 127.0.0.1 for the wakeup-socket reproduction.

Answers ApiVersions (18), Metadata (3) and ListGroups (16) at any version the
client asks for, encoding responses with kafka-python-ng's own protocol
classes so the bytes are exactly what the client expects. Responses for an
api_key can be delayed (delays={api_key: seconds}).

extra_brokers adds (node_id, port) entries to the metadata; a "blackhole"
port is a listener whose accept queue is full and never accepted, so a TCP
connect to it never completes on Linux (SYNs are dropped) and the client's
connection to that node stays CONNECTING."""
import socket
import struct
import threading
import time

from kafka.protocol.admin import ApiVersionResponse, ListGroupsResponse
from kafka.protocol.metadata import MetadataResponse
from kafka.protocol.types import Array, Schema, String

API_VERSIONS = [(3, 0, 5), (16, 0, 2), (18, 0, 2)]  # (api_key, min, max)


def _fill(schema, data):
    values = []
    for name, field in zip(schema.names, schema.fields):
        v = data.get(name)
        if isinstance(field, Array):
            if isinstance(field.array_of, Schema):
                values.append([_fill(field.array_of, d) for d in (v or [])])
            else:
                values.append(list(v or []))
        elif isinstance(field, Schema):
            values.append(_fill(field, v or {}))
        elif v is not None:
            values.append(v)
        else:
            values.append(None if field is String or name == "rack" else 0)
    return tuple(values)


class FakeBroker:
    def __init__(self, delays=None, extra_brokers=()):
        self.delays = dict(delays or {})
        self.extra_brokers = list(extra_brokers)
        self.requests = []  # (time, api_key, api_version)
        self._srv = socket.socket()
        self._srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._srv.bind(("127.0.0.1", 0))
        self._srv.listen(64)
        self.port = self._srv.getsockname()[1]
        threading.Thread(target=self._accept, name="fake-broker-accept", daemon=True).start()

    def _accept(self):
        while True:
            conn, _ = self._srv.accept()
            threading.Thread(target=self._serve, args=(conn,), name="fake-broker-conn", daemon=True).start()

    @staticmethod
    def _read(conn, n):
        buf = b""
        while len(buf) < n:
            chunk = conn.recv(n - len(buf))
            if not chunk:
                raise ConnectionError("closed")
            buf += chunk
        return buf

    def _serve(self, conn):
        try:
            while True:
                size = struct.unpack(">i", self._read(conn, 4))[0]
                payload = self._read(conn, size)
                api_key, api_version, corr = struct.unpack(">hhi", payload[:8])
                self.requests.append((time.time(), api_key, api_version))
                body = self._respond(api_key, api_version)
                if body is None:
                    continue
                delay = self.delays.get(api_key)
                if delay:
                    time.sleep(delay)
                msg = struct.pack(">i", corr) + body
                conn.sendall(struct.pack(">i", len(msg)) + msg)
        except (ConnectionError, OSError):
            pass
        finally:
            conn.close()

    def _respond(self, api_key, v):
        if api_key == 18:
            cls = ApiVersionResponse[min(v, len(ApiVersionResponse) - 1)]
            data = {"api_versions": [{"api_key": k, "min_version": a, "max_version": b} for k, a, b in API_VERSIONS]}
        elif api_key == 3:
            cls = MetadataResponse[min(v, len(MetadataResponse) - 1)]
            brokers = [{"node_id": 0, "host": "127.0.0.1", "port": self.port}] + [
                {"node_id": nid, "host": "127.0.0.1", "port": port} for nid, port in self.extra_brokers]
            data = {"brokers": brokers, "controller_id": 0, "cluster_id": "fake-cluster",
                    "topics": [{"topic": "t1", "partitions": [{"partition": 0, "leader": 0, "replicas": [0], "isr": [0]}]}]}
        elif api_key == 16:
            cls = ListGroupsResponse[min(v, len(ListGroupsResponse) - 1)]
            data = {"groups": [{"group": "g1", "protocol_type": "consumer"}]}
        else:
            return None
        response = cls(*_fill(cls.SCHEMA, data))  # keep a reference: encode is a weak method
        return response.encode()


def blackhole_port():
    """A listener whose connects never complete: backlog 0, never accepted,
    accept queue pre-filled so further SYNs are dropped."""
    srv = socket.socket()
    srv.bind(("127.0.0.1", 0))
    srv.listen(0)
    port = srv.getsockname()[1]
    fillers = []
    for _ in range(4):
        s = socket.socket()
        s.setblocking(False)
        try:
            s.connect(("127.0.0.1", port))
        except BlockingIOError:
            pass
        fillers.append(s)
    time.sleep(0.2)
    blackhole_port.keep = (srv, fillers)  # keep alive
    return port
