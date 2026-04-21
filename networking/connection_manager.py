from networking.client import connect_to_peer
from protocol import handshake
from protocol.encoder import encode_bitfield, encode_interested, encode_not_interested, encode_choke, encode_unchoke
from protocol.decoder import recv_message, decode_bitfield_payload
from protocol.message_types import MessageType

from p2p.neighbor_state import NeighborState
from p2p.peer_connection import PeerConnection


class ConnectionManager:

    def __init__(self, peer_id, peer_info_list, piece_manager, logger):

        self.peer_id = peer_id
        self.peer_info_list = peer_info_list
        self.piece_manager = piece_manager
        self.logger = logger

        # peer_id -> NeighborState
        self.neighbors = {}

        # peer_id -> PeerConnection (receive loop)
        self.peer_connections = {}

    def start_outgoing_connections(self):

        for peer in self.peer_info_list:

            # only connect to peers with smaller ID so there's just one connection per pair

            if peer.peer_id < self.peer_id:

                sock = connect_to_peer(peer.hostname, peer.port)

                if sock:
                    # send our id, then read theirs to confirm who answered
                    handshake.send(sock, self.peer_id)
                    remote_id = handshake.receive(sock)

                    if remote_id != peer.peer_id:
                        print(f"Handshake failed: expected {peer.peer_id}, got {remote_id}")
                        sock.close()
                        continue

                    print(f"Peer {self.peer_id} makes a connection to Peer {remote_id}")
                    self.logger.tcp_log_connect(self.peer_id, remote_id)

                    self._setup_neighbor(sock, remote_id)

    def register_incoming_connection(self, conn):
        # read their handshake first, then reply with ours
        remote_id = handshake.receive(conn)
        handshake.send(conn, self.peer_id)

        print(f"Peer {self.peer_id} is connected from Peer {remote_id}")
        self.logger.tcp_log_connected_from(self.peer_id, remote_id)

        self._setup_neighbor(conn, remote_id)

        return remote_id

    # Called on both sides right after handshake. Creates the NeighborState,
    # does the bitfield + initial interested exchange, then starts the
    # persistent receive loop.
    def _setup_neighbor(self, sock, remote_id):

        neighbor = NeighborState(remote_id, sock, self.piece_manager.num_pieces)
        self.neighbors[remote_id] = neighbor

        # send our bitfield
        sock.sendall(encode_bitfield(self.piece_manager.bitfield))
        print(
            f"Peer {self.peer_id} sent bitfield to {remote_id} "
            f"({self.piece_manager.piece_count()} pieces)"
        )

        # receive theirs (spec says BITFIELD is always the first message after
        # handshake, so read it synchronously before starting the loop)
        msg = recv_message(sock)
        if msg.msg_type == MessageType.BITFIELD:
            remote_bitfield = decode_bitfield_payload(
                msg.payload, self.piece_manager.num_pieces
            )
            neighbor.bitfield = remote_bitfield
            print(
                f"Peer {self.peer_id} received bitfield from {remote_id} "
                f"({remote_bitfield.piece_count()} pieces)"
            )
        else:
            print(f"Peer {self.peer_id}: expected BITFIELD from {remote_id}, got {msg.msg_type}")

        # send INTERESTED or NOT_INTERESTED based on what they have
        has_something_we_need = False
        for i in range(self.piece_manager.num_pieces):
            if neighbor.bitfield.has_piece(i) and not self.piece_manager.bitfield.has_piece(i):
                has_something_we_need = True
                break

        if has_something_we_need:
            neighbor.am_interested = True
            sock.sendall(encode_interested())
            print(f"Peer {self.peer_id} sending INTERESTED to {remote_id}")
            self.logger.rec_interested_message_log(self.peer_id, remote_id)  # Wait, this is sending, not receiving. Maybe keep as print or add a send method.

        else:
            neighbor.am_interested = False
            sock.sendall(encode_not_interested())
            print(f"Peer {self.peer_id} sending NOT_INTERESTED to {remote_id}")
            self.logger.rec_not_interested_message_log(self.peer_id, remote_id)  # Similarly, this is sending.

        # kick off the receive loop for the rest of this peer's lifetime
        pc = PeerConnection(neighbor, self.piece_manager, self.peer_id, self, self.logger)
        self.peer_connections[remote_id] = pc
        pc.start()

    def get_neighbor(self, peer_id):
        return self.neighbors.get(peer_id)

    def get_all_neighbors(self):
        return list(self.neighbors.values())

    def remove_connection(self, peer_id):
        if peer_id in self.peer_connections:
            self.peer_connections[peer_id].stop()
            del self.peer_connections[peer_id]

        if peer_id in self.neighbors:
            self.neighbors[peer_id].close()
            del self.neighbors[peer_id]

    def choking_manager(self):
        # Simple choking: unchoke up to 3 interested peers with highest download rates
        interested_neighbors = [
            n for n in self.neighbors.values() 
            if n.peer_interested
        ]
        
        # Sort by bytes_downloaded descending
        interested_neighbors.sort(key=lambda n: n.bytes_downloaded, reverse=True)
        
        # Unchoke top 3, choke the rest
        for i, neighbor in enumerate(self.neighbors.values()):
            should_choke = i >= 3 or neighbor not in interested_neighbors[:3]
            if should_choke and not neighbor.am_choking:
                neighbor.am_choking = True
                neighbor.sock.sendall(encode_choke())
            elif not should_choke and neighbor.am_choking:
                neighbor.am_choking = False
                neighbor.sock.sendall(encode_unchoke())
        
        # Reset download counters for next interval
        for neighbor in self.neighbors.values():
            neighbor.bytes_downloaded = 0
