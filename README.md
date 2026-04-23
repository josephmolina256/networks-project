# Networks Project

## Video Link 
https://youtube.com/shorts/TG22AyrvlhY

## Video in Drive
https://drive.google.com/file/d/1V83BzfDvXl43wlvA4gQptNJjhEAUGQYs/view?usp=sharing

Simple peer-to-peer file sharing project for the networks class.

## Quick Start

1. Create a virtual environment:

```bash
python -m venv .venv
```

2. Activate it:

Windows:

```bash
.venv\Scripts\activate
```

macOS / Linux:

```bash
source .venv/bin/activate
```

3. Install dependencies:

```bash
pip install -r requirements.txt
```

4. Run each peer in a separate terminal from the repo root:

```bash
python peerProcess.py 1001
```

```bash
python peerProcess.py 1002
```

```bash
python peerProcess.py 1003
```

## Local Test Setup

Current local config:

- `1001` starts with the file
- `1002` starts without the file
- `1003` starts without the file

Ports:

- `1001` -> `6001`
- `1002` -> `6002`
- `1003` -> `6003`

File settings from `local_testing/Common.cfg`:

- preferred neighbors: `1`
- unchoking interval: `5`
- optimistic unchoking interval: `10`
- piece size: `16384`

## Status

Working now:

- peers start and read config files
- peers connect to each other with the handshake
- bitfields are exchanged
- interest / not interested messages are sent
- each peer keeps state for every neighbor
- peers request pieces at random from peers that unchoke them
- peers serve pieces in response to `REQUEST` when not choking the requester
- peers broadcast `HAVE` after getting a new piece
- preferred neighbors are chosen every `UnchokingInterval` seconds (random tie-break, random pick when seeder has the full file)
- one optimistically unchoked neighbor is chosen every `OptimisticUnchokingInterval` seconds
- choke / unchoke messages are sent accordingly
- completed file is reassembled and written to `peer_<id>/<file_name>`
- logs are written to `log_peer_<id>.log`
- peers stop automatically once everyone has the full file

Still not fully done:

- extra testing on larger peer counts

## Logs

Each peer writes to its own log file:

- `log_peer_1001.log`
- `log_peer_1002.log`
- `log_peer_1003.log`

You should see events like:

- connection made
- connected from
- interested / not interested
- choke / unchoke
- have
- piece downloaded
- complete file

## Stopping and Rerunning

To stop peers cleanly, use `Ctrl+C`.

If a rerun says a port is already in use, an old Python process is probably still running. On PowerShell:

```powershell
Get-Process -Name python | Stop-Process -Force
```

Then start the peers again.

## Main Files

- `peerProcess.py` - starts a peer
- `networking/connection_manager.py` - handles peer connections
- `networking/server.py` - TCP server
- `p2p/peer_connection.py` - receive loop and message handling
- `p2p/choking_manager.py` - preferred neighbor choke logic
- `file_manager/piece_manager.py` - piece storage
- `file_manager/logger.py` - log writing
