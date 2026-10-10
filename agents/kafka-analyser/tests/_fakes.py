"""In-process fake SMTP server and fake Teams webhook for the outbox tests.
Both run in daemon threads on 127.0.0.1 and record what they receive. No
network beyond the loopback interface is used."""
import email
import json
import socket
import socketserver
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer


class FakeSMTP:
    """Minimal ESMTP server for smtplib: no STARTTLS, no AUTH. messages holds
    (recipients, parsed message). down() closes the listener so connections
    are refused; up() reopens it on the same port."""

    def __init__(self):
        self.messages = []
        self._server = None
        self.port = None
        self.up()

    def up(self):
        owner = self

        class Handler(socketserver.StreamRequestHandler):
            def handle(self):
                rcpts, w = [], self.wfile
                w.write(b"220 fake ESMTP\r\n")
                while True:
                    line = self.rfile.readline()
                    if not line:
                        return
                    cmd = line.decode("utf-8", "replace").strip()
                    upper = cmd.upper()
                    if upper.startswith(("EHLO", "HELO")):
                        w.write(b"250-fake\r\n250 8BITMIME\r\n")
                    elif upper.startswith("MAIL FROM"):
                        rcpts = []
                        w.write(b"250 ok\r\n")
                    elif upper.startswith("RCPT TO"):
                        rcpts.append(cmd.split(":", 1)[1].strip().strip("<>"))
                        w.write(b"250 ok\r\n")
                    elif upper == "DATA":
                        w.write(b"354 go\r\n")
                        data = []
                        while True:
                            chunk = self.rfile.readline()
                            if chunk in (b".\r\n", b".\n", b""):
                                break
                            data.append(chunk)
                        owner.messages.append((list(rcpts), email.message_from_bytes(b"".join(data))))
                        w.write(b"250 queued\r\n")
                    elif upper == "QUIT":
                        w.write(b"221 bye\r\n")
                        return
                    else:
                        w.write(b"250 ok\r\n")

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


class FakeTeams:
    """Webhook stub: records each posted card; answers status (default 202)."""

    def __init__(self, status=202):
        self.cards = []
        self.status = status
        owner = self

        class Handler(BaseHTTPRequestHandler):
            def do_POST(self):
                body = self.rfile.read(int(self.headers.get("Content-Length") or 0))
                owner.cards.append(json.loads(body or b"{}"))
                self.send_response(owner.status)
                self.end_headers()

            def log_message(self, *args):
                pass

        self._server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.port = self._server.server_address[1]
        self.url = f"http://127.0.0.1:{self.port}/webhook/secret-signature-abc123"
        threading.Thread(target=self._server.serve_forever, daemon=True).start()


def free_port() -> int:
    """A loopback port with nothing listening (connection refused)."""
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port
