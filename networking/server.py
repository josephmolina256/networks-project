import socket
import threading
import time

from networking.connection_manager import ConnectionManager


class TCPServer:

    def __init__(
            self,
            peer_id: int,
            host: str,
            port: int,
            connection_manager: ConnectionManager
    ):
        self.peer_id = peer_id
        self.host = host
        self.port = port
        self.connection_manager = connection_manager

    def start(self):
        server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        
        # Retry binding in case port is temporarily unavailable
        max_retries = 5
        for attempt in range(max_retries):
            try:
                server.bind((self.host, self.port))
                break
            except OSError as e:
                if attempt == max_retries - 1:
                    raise
                print(f"Bind attempt {attempt + 1} failed, retrying... ({e})")
                time.sleep(1)
        
        server.listen()

        print(f"Peer {self.peer_id} listening on port {self.port}")

        while True:
            conn, addr = server.accept()

            thread = threading.Thread(
                target=self.handle_connection,
                args=(conn, addr)
            )
            thread.daemon = True
            thread.start()

    def handle_connection(self, conn, addr):
        # handshake, bitfield exchange, and starting the receive loop all
        # happen inside register_incoming_connection now
        self.connection_manager.register_incoming_connection(conn)
