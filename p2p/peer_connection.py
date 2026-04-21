import threading
import random

from protocol.decoder import (
    recv_message,
    decode_bitfield_payload,
    decode_have_payload,
    decode_request_payload,
    decode_piece_payload,
)
from protocol.encoder import encode_interested, encode_not_interested, encode_request, encode_piece, encode_have
from protocol.message_types import MessageType


# Runs one receive loop per neighbor, reading length-prefixed messages off
# the socket and updating NeighborState / piece_manager. Sending choke/unchoke,
# request, piece, and have is done by other modules that own those flows.
class PeerConnection:

    def __init__(self, neighbor, piece_manager, peer_id, connection_manager, logger):
        self.neighbor = neighbor
        self.piece_manager = piece_manager
        self.peer_id = peer_id  # our id, just for prints
        self.connection_manager = connection_manager
        self.logger = logger

        self.running = False
        self.thread = None

    def start(self):
        self.running = True
        self.thread = threading.Thread(target=self._run)
        self.thread.daemon = True
        self.thread.start()

    def stop(self):
        self.running = False
        self.neighbor.close()

    def _run(self):
        remote_id = self.neighbor.peer_id

        try:
            while self.running:
                msg = recv_message(self.neighbor.sock)
                self._dispatch(msg)

        except (ConnectionError, OSError) as e:
            if self.running:
                print(f"Peer {self.peer_id}: connection to {remote_id} closed ({e})")

        finally:
            self.running = False

    def _dispatch(self, msg):
        mt = msg.msg_type

        if mt == MessageType.CHOKE:
            self._on_choke()

        elif mt == MessageType.UNCHOKE:
            self._on_unchoke()

        elif mt == MessageType.INTERESTED:
            self._on_interested()

        elif mt == MessageType.NOT_INTERESTED:
            self._on_not_interested()

        elif mt == MessageType.HAVE:
            piece_index = decode_have_payload(msg.payload)
            self._on_have(piece_index)

        elif mt == MessageType.BITFIELD:
            remote_bitfield = decode_bitfield_payload(
                msg.payload, self.piece_manager.num_pieces
            )
            self._on_bitfield(remote_bitfield)

        elif mt == MessageType.REQUEST:
            piece_index = decode_request_payload(msg.payload)
            self._on_request(piece_index)

        elif mt == MessageType.PIECE:
            piece_index, data = decode_piece_payload(msg.payload)
            self._on_piece(piece_index, data)

        else:
            print(f"Peer {self.peer_id}: unknown message type {mt} from {self.neighbor.peer_id}")

    # ---- handlers ----
    # Most handlers just update state. Choking manager and request/piece flow
    # are separate modules and will plug in where the TODOs are below.

    def _on_choke(self):
        self.neighbor.peer_choking = True
        print(f"Peer {self.peer_id} is choked by {self.neighbor.peer_id}")
        self.logger.choking_log(self.peer_id, self.neighbor.peer_id)


    def _on_unchoke(self):
        self.neighbor.peer_choking = False
        print(f"Peer {self.peer_id} is unchoked by {self.neighbor.peer_id}")
        self.logger.unchoking_log(self.peer_id, self.neighbor.peer_id)
        # Pick a random missing piece this peer has and send REQUEST
        if self.neighbor.am_interested and not self.neighbor.peer_choking:
            missing_pieces = [
                i for i in range(self.piece_manager.num_pieces)
                if self.neighbor.bitfield.has_piece(i) and not self.piece_manager.bitfield.has_piece(i)
            ]
            if missing_pieces:
                piece_index = random.choice(missing_pieces)
                self.neighbor.sock.sendall(encode_request(piece_index))
                print(f"Peer {self.peer_id} requesting piece {piece_index} from {self.neighbor.peer_id}")

    def _on_interested(self):
        self.neighbor.peer_interested = True
        print(f"Peer {self.peer_id} received INTERESTED from {self.neighbor.peer_id}")
        self.logger.rec_interested_message_log(self.peer_id, self.neighbor.peer_id)

    def _on_not_interested(self):
        self.neighbor.peer_interested = False
        print(f"Peer {self.peer_id} received NOT_INTERESTED from {self.neighbor.peer_id}")
        self.logger.rec_not_interested_message_log(self.peer_id, self.neighbor.peer_id)


    def _on_have(self, piece_index):
        self.neighbor.bitfield.set_piece(piece_index)
        print(f"Peer {self.peer_id} received HAVE({piece_index}) from {self.neighbor.peer_id}")
        self.logger.rec_have_message_log(self.peer_id, self.neighbor.peer_id, piece_index)

        # if this is a piece we don't have, we might newly be interested
        if not self.piece_manager.bitfield.has_piece(piece_index):
            if not self.neighbor.am_interested:
                self.neighbor.am_interested = True
                self.neighbor.sock.sendall(encode_interested())

    def _on_bitfield(self, remote_bitfield):
        # normally BITFIELD only comes right after handshake, but if we get
        # another one just overwrite so state stays consistent
        self.neighbor.bitfield = remote_bitfield
        print(f"Peer {self.peer_id} received BITFIELD from {self.neighbor.peer_id}")
        self._reevaluate_interest()

    def _on_request(self, piece_index):
        print(f"Peer {self.peer_id} received REQUEST({piece_index}) from {self.neighbor.peer_id}")
        # If we are NOT choking this peer, send them the piece
        if not self.neighbor.am_choking and self.piece_manager.bitfield.has_piece(piece_index):
            data = self.piece_manager.get_piece(piece_index)
            self.neighbor.sock.sendall(encode_piece(piece_index, data))
            print(f"Peer {self.peer_id} sending piece {piece_index} to {self.neighbor.peer_id}")

    def _on_piece(self, piece_index, data):
        self.neighbor.bytes_downloaded += len(data)
        self.piece_manager.store_piece(piece_index, data)

        print(
            f"Peer {self.peer_id} downloaded piece {piece_index} from "
            f"{self.neighbor.peer_id} (now has {self.piece_manager.piece_count()})"
        )
        self.logger.downloading_piece_log(self.peer_id, self.neighbor.peer_id, piece_index, self.piece_manager.piece_count())


        # Broadcast HAVE to all neighbors, then send the next REQUEST
        for neighbor in self.connection_manager.neighbors.values():
            neighbor.sock.sendall(encode_have(piece_index))
        
        # Send next REQUEST to this neighbor (similar to _on_unchoke)
        if self.neighbor.am_interested and not self.neighbor.peer_choking:
            missing_pieces = [
                i for i in range(self.piece_manager.num_pieces)
                if self.neighbor.bitfield.has_piece(i) and not self.piece_manager.bitfield.has_piece(i)
            ]
            if missing_pieces:
                next_piece = random.choice(missing_pieces)
                self.neighbor.sock.sendall(encode_request(next_piece))
                print(f"Peer {self.peer_id} requesting piece {next_piece} from {self.neighbor.peer_id}")
    def _reevaluate_interest(self):
        # are any of their pieces ones we don't have?
        wants_something = False
        for i in range(self.piece_manager.num_pieces):
            if self.neighbor.bitfield.has_piece(i) and not self.piece_manager.bitfield.has_piece(i):
                wants_something = True
                break

        if wants_something and not self.neighbor.am_interested:
            self.neighbor.am_interested = True
            self.neighbor.sock.sendall(encode_interested())

        elif not wants_something and self.neighbor.am_interested:
            self.neighbor.am_interested = False
            self.neighbor.sock.sendall(encode_not_interested())
