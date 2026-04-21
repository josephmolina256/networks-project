import sys
import json
import os
import threading
import time

from config.config_loader import load_common_config, load_peer_info
from file_manager.logger import Logger
from file_manager.piece_manager import PieceManager
from networking.connection_manager import ConnectionManager
from networking.server  import TCPServer


def main():

    if len(sys.argv) != 2:
        print("Usage: python peerProcess.py <peer_id>")
        sys.exit(1)

    peer_id = int(sys.argv[1])

    print(f"Starting peer {peer_id}")

    # load configs
    common_cfg = load_common_config("local_testing/Common.cfg")
    peer_info_list = load_peer_info("local_testing/PeerInfo.cfg")

    print(f"Common config: {json.dumps(vars(common_cfg), indent=2)}")
    print(f"Peer info: {json.dumps([vars(p) for p in peer_info_list], indent=2)}")


    # find our peer entry
    this_peer = None
    for p in peer_info_list:
        if p.peer_id == peer_id:
            this_peer = p
            break

    if this_peer is None:
        print("Peer ID not found in PeerInfo.cfg")
        sys.exit(1)

    _logger = Logger(f"log_peer_{peer_id}.log")

    # Create peer directory
    peer_dir = f"peer_{peer_id}"

    if not os.path.exists(peer_dir):
        os.makedirs(peer_dir)

    if this_peer.has_file:
        print("Peer has the file, loading pieces...")
        os.system(f"cp {common_cfg.file_name} {peer_dir}/")

    piece_manager = PieceManager(
        peer_id,
        common_cfg,
        this_peer.has_file
    )

    print(vars(piece_manager.bitfield))
    print("Pieces owned:", piece_manager.piece_count())
    print("Missing pieces:", piece_manager.bitfield.missing_pieces())

    connection_manager = ConnectionManager(
        peer_id,
        peer_info_list,
        piece_manager,
        _logger
    )

    server = TCPServer(
        peer_id,
        this_peer.hostname,
        this_peer.port,
        connection_manager
    )

    server_thread = threading.Thread(target=server.start)
    server_thread.daemon = True
    server_thread.start()

    connection_manager.start_outgoing_connections()

    # Initial choking decision
    connection_manager.choking_manager()

    # Start periodic choking manager
    def choking_loop():
        while True:
            time.sleep(common_cfg.unchoking_interval)
            connection_manager.choking_manager()

    choking_thread = threading.Thread(target=choking_loop)
    choking_thread.daemon = True
    choking_thread.start()

    # TODO (termination): exit when all peers report complete file (spec)
    # for now, block the main thread so daemon threads keep running
    try:
        while True:
            server_thread.join(timeout=1.0)
            if piece_manager.completed() and not this_peer.has_file:
                print(f"Peer {peer_id} has downloaded the complete file")
                _logger.complete_download_log(peer_id)
                break
    except KeyboardInterrupt:
        print(f"Peer {peer_id} shutting down")

if __name__ == "__main__":
    main()