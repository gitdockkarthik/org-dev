"""Scriptable in-process fake SMTP server and Teams webhook for the delivery
worker tests (test_outbox_worker.py). Both listen on 127.0.0.1 in daemon
threads; nothing beyond the loopback interface is used.

ScriptedSMTP: plain ESMTP (no STARTTLS, no AUTH). delay maps a step
("banner", "EHLO", "MAIL", "RCPT", "DATA_END") to seconds slept before the
reply; reply maps a step to a raw reply line that replaces the normal one;
drop_at_data_end closes the connection after the message body without a
reply (the client cannot know whether it was delivered -- the fake records
it as delivered). data_end_code (e.g. 452 or 554) refuses every message
with that code at the end of DATA; refused messages go to rejected, not to
messages. data_started is set when a message body starts arriving.

ScriptedTeams: records (path, card) per POST; script holds (status, headers)
or (status, headers, body) answers used in order, then status (default 202)."""
import email
import json
import socket
import socketserver
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer


class ScriptedSMTP:
    def __init__(self):
        self.port = None
        self._server = None
        self.reset()
        self.up()

    def reset(self):
        self.messages = []
        self.delay = {}
        self.reply = {}
        self.drop_at_data_end = False
        # A code (e.g. 452 or 554): every message is refused with it at the end
        # of DATA and kept in rejected, not in messages.
        self.data_end_code = None
        self.rejected = []
        self.data_started = threading.Event()

    def up(self):
        owner = self

        class Handler(socketserver.StreamRequestHandler):
            def _send(self, step, normal):
                time.sleep(owner.delay.get(step, 0))
                self.wfile.write(owner.reply.get(step, normal))

            def handle(self):
                self._send("banner", b"220 fake ESMTP\r\n")
                rcpts = []
                while True:
                    line = self.rfile.readline()
                    if not line:
                        return
                    cmd = line.decode("utf-8", "replace").strip()
                    upper = cmd.upper()
                    if upper.startswith(("EHLO", "HELO")):
                        self._send("EHLO", b"250-fake\r\n250 8BITMIME\r\n")
                    elif upper.startswith("MAIL FROM"):
                        rcpts = []
                        self._send("MAIL", b"250 ok\r\n")
                    elif upper.startswith("RCPT TO"):
                        rcpts.append(cmd.split(":", 1)[1].strip().strip("<>"))
                        self._send("RCPT", b"250 ok\r\n")
                    elif upper == "DATA":
                        self.wfile.write(b"354 go\r\n")
                        owner.data_started.set()
                        data = []
                        while True:
                            chunk = self.rfile.readline()
                            if chunk in (b".\r\n", b".\n", b""):
                                break
                            data.append(chunk)
                        parsed = (list(rcpts), email.message_from_bytes(b"".join(data)))
                        if owner.data_end_code:
                            owner.rejected.append(parsed)
                            # The text echoes an address, as real servers do: it must never be stored.
                            self.wfile.write(f"{owner.data_end_code} 5.7.1 <ops@operative.com> refused by test\r\n".encode())
                            continue
                        owner.messages.append(parsed)
                        if owner.drop_at_data_end:
                            time.sleep(owner.delay.get("DATA_END", 0))
                            return
                        self._send("DATA_END", b"250 queued\r\n")
                    elif upper == "QUIT":
                        self.wfile.write(b"221 bye\r\n")
                        return
                    else:
                        self.wfile.write(b"250 ok\r\n")

        class Server(socketserver.ThreadingTCPServer):
            allow_reuse_address = True
            daemon_threads = True

        self._server = Server(("127.0.0.1", self.port or 0), Handler)
        self.port = self._server.server_address[1]
        threading.Thread(target=self._server.serve_forever, daemon=True).start()

    def down(self):
        if self._server is not None:
            self._server.shutdown()
            self._server.server_close()
            self._server = None

    def subjects(self):
        return [m["Subject"] for _r, m in self.messages]

    def message_ids(self):
        return [m["Message-ID"] for _r, m in self.messages]


class ScriptedTeams:
    def __init__(self):
        self.port = None
        self._server = None
        self.reset()
        self.up()

    def reset(self):
        self.posts = []
        self.script = []
        self.status = 202

    def up(self):
        owner = self

        class Handler(BaseHTTPRequestHandler):
            def do_POST(self):
                body = self.rfile.read(int(self.headers.get("Content-Length") or 0))
                owner.posts.append((self.path, json.loads(body or b"{}")))
                answer = owner.script.pop(0) if owner.script else (owner.status, {})
                status, headers, body = (tuple(answer) + ("",))[:3]
                data = body.encode()
                self.send_response(status)
                for k, v in headers.items():
                    self.send_header(k, v)
                self.send_header("Content-Length", str(len(data)))
                self.end_headers()
                self.wfile.write(data)

            def log_message(self, *args):
                pass

        class Server(ThreadingHTTPServer):
            allow_reuse_address = True
            daemon_threads = True

        self._server = Server(("127.0.0.1", self.port or 0), Handler)
        self.port = self._server.server_address[1]
        threading.Thread(target=self._server.serve_forever, daemon=True).start()

    def down(self):
        if self._server is not None:
            self._server.shutdown()
            self._server.server_close()
            self._server = None

    def url(self, name="default"):
        return f"http://127.0.0.1:{self.port}/webhook/{name}/secret-signature-abc123"

    def texts(self):
        """First TextBlock texts of each posted card, in order."""
        out = []
        for _path, card in self.posts:
            body = card["attachments"][0]["content"]["body"]
            out.append([b.get("text") for b in body if b.get("type") == "TextBlock"])
        return out


def free_port() -> int:
    """A loopback port with nothing listening (connection refused)."""
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port
