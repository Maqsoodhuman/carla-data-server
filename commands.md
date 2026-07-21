# Commands Reference

## Lab PC

### Start CARLA
```bash
# CARLA A — headless (no window)
cd ~/Documents/carla
./CarlaUE4.sh -quality-level=Low -prefernvidia -RenderOffScreen

# CARLA A — visible window
cd ~/Documents/carla
./CarlaUE4.sh -quality-level=Low -prefernvidia

# CARLA B — visible, different port (for two-CARLA mirror on same machine)
./CarlaUE4.sh -quality-level=Low -prefernvidia -carla-rpc-port=3000
```

### Load Map
```bash
source ~/Documents/carla-data-server/carla-data-server/venv/bin/activate
cd ~/Documents/carla

# Into CARLA A (port 2000)
python3 PythonAPI/util/config.py --host localhost --port 2000 --map UBAutonomousProvingGrounds

# Into CARLA B (port 3000)
python3 PythonAPI/util/config.py --host localhost --port 3000 --map UBAutonomousProvingGrounds
```

### Data Server
```bash
cd ~/Documents/carla-data-server/carla-data-server && source venv/bin/activate
python3 server/server.py --carla-host localhost --carla-port 2000 --tick-rate 20
```

### Generate Traffic
```bash
source ~/Documents/carla-data-server/carla-data-server/venv/bin/activate
cd ~/Documents/carla/PythonAPI/examples
python3 generate_traffic.py -n 50 -w 0 --host localhost --port 2000
```

> **NOTE — First run on UBAutonomousProvingGrounds:** CARLA builds a nav-mesh/local-map
> cache the first time this map is loaded. `generate_traffic.py` will appear hung for
> 5-15 minutes. This is normal — do NOT kill it. Subsequent runs are instant.

> **NOTE — Spawn point cap:** `UBAutonomousProvingGrounds` has ~125 usable spawn points.
> Requesting more than that (e.g. `-n 500`) silently under-spawns without error.
> Check the actual count first:
> ```bash
> python3 -c "
> import carla
> c = carla.Client('localhost', 2000); c.set_timeout(5)
> pts = c.get_world().get_map().get_spawn_points()
> print(f'Available spawn points: {len(pts)}')
> "
> ```
> Then use that number (or less) as your `-n` value.

### Manual Control (pygame on lab PC)
```bash
source ~/Documents/carla-data-server/carla-data-server/venv/bin/activate
cd ~/Documents/carla/PythonAPI/examples
python3 manual_control.py
```

### Mirror Bridge (two CARLAs on same machine)
```bash
cd ~/Documents/carla-data-server/carla-data-server && source venv/bin/activate
python3 bridges/carla_mirror_client.py --server ws://localhost:8765 --shadow-host localhost --shadow-port 3000
```

### Snap Spectator Camera to First Vehicle (lab PC, CARLA B)
```bash
python3 -c "
import carla
c = carla.Client('localhost', 3000)
c.set_timeout(5)
w = c.get_world()
vs = w.get_actors().filter('vehicle.*')
v = list(vs)[0]
t = v.get_transform()
w.get_spectator().set_transform(carla.Transform(
    carla.Location(t.location.x, t.location.y, t.location.z + 20),
    carla.Rotation(pitch=-45)
))
print(f'Snapped to ({t.location.x:.0f},{t.location.y:.0f})')
"
```

### Kill Everything (Lab PC)
```bash
pkill -9 -f server.py; pkill -9 -f client.py; pkill -9 -f carla_mirror_client.py
pkill -9 -f generate_traffic.py; pkill -9 -f interactive_driver.py
pkill -9 -f metrics_client.py; pkill -9 -f manual_control.py; pkill -9 -f CarlaUE4
sleep 3
pgrep -af "server.py|client.py|CarlaUE4|generate_traffic|manual_control|metrics|interactive"
```

---

## Laptop

### Test Connectivity to Lab PC
```bash
ping 128.205.222.211
nc -zv 128.205.222.211 8765
time nc -zv 128.205.222.211 8765
```

### Start Laptop CARLA
```bash
cd ~/Documents/Cavas_Lab/CARLA_ff009c8a3-dirty
./CarlaUE4.sh -quality-level=Low -prefernvidia
```

### Load Map into Laptop CARLA
```bash
source ~/Documents/carla-data-server/carla-data-server/venv/bin/activate
cd ~/Documents/Cavas_Lab/CARLA_ff009c8a3-dirty
python3 PythonAPI/util/config.py --host localhost --port 2000 --map UBAutonomousProvingGrounds
```

### Spectator Client (no CARLA needed)
```bash
cd ~/Documents/carla-data-server/carla-data-server && source venv/bin/activate
python3 client/client.py --server ws://128.205.222.211:8765 --role spectator
```

### Mirror Bridge (laptop CARLA ← lab server)
```bash
cd ~/Documents/carla-data-server/carla-data-server && source venv/bin/activate
python3 bridges/carla_mirror_client.py \
    --server ws://128.205.222.211:8765 \
    --shadow-host localhost --shadow-port 2000
```

### Interactive Driver (pygame keyboard control + camera)
```bash
cd ~/Documents/carla-data-server/carla-data-server && source venv/bin/activate
python3 scripts/interactive_driver.py --server ws://128.205.222.211:8765 --spawn-index 5
```

### Metrics Client
```bash
cd ~/Documents/carla-data-server/carla-data-server && source venv/bin/activate
python3 scripts/metrics_client.py --server ws://128.205.222.211:8765
```

### Snap Spectator Camera to First Vehicle (laptop CARLA)
```bash
python3 -c "
import carla
c = carla.Client('localhost', 2000)
c.set_timeout(5)
w = c.get_world()
vs = w.get_actors().filter('vehicle.*')
v = list(vs)[0]
t = v.get_transform()
w.get_spectator().set_transform(carla.Transform(
    carla.Location(t.location.x, t.location.y, t.location.z + 20),
    carla.Rotation(pitch=-45)
))
print(f'Snapped to ({t.location.x:.0f},{t.location.y:.0f})')
"
```

### Clock Sync (for accurate latency measurement)
```bash
sudo apt install -y ntpdate
sudo ntpdate -s pool.ntp.org
```

### Kill Everything (Laptop)
```bash
pkill -9 -f client.py; pkill -9 -f carla_mirror_client.py
pkill -9 -f interactive_driver.py; pkill -9 -f metrics_client.py
pkill -9 -f ws_to_udp_bridge.py; pkill -9 -f UB-mmr; pkill -9 -f CarlaUE4
sleep 3
pgrep -af "client.py|CarlaUE4|metrics|interactive|mirror|UB-mmr|bridge"
```

---

## UB-MR Unity Integration (Laptop)

### GitHub SSH Key Setup
```bash
ssh-keygen -t ed25519 -C "your-email@example.com" -f ~/.ssh/id_ed25519 -N ""
cat ~/.ssh/id_ed25519.pub
# Add to GitHub: https://github.com/settings/keys
ssh -T git@github.com
```

### Clone UB-MR (redis_networking branch)
```bash
cd ~/Documents
rm -rf UB-MR
sudo apt install -y git-lfs
git lfs install
git clone --recurse-submodules --branch redis_networking git@github.com:ub-cavas/UB-MR.git
cd UB-MR
git lfs pull
```

### Install Unity Hub + Editor
```bash
wget -qO - https://hub.unity3d.com/linux/keys/public | sudo gpg --dearmor -o /usr/share/keyrings/unity.gpg
echo "deb [signed-by=/usr/share/keyrings/unity.gpg] https://hub.unity3d.com/linux/repos/deb stable main" | sudo tee /etc/apt/sources.list.d/unityhub.list
sudo apt update
sudo apt install -y unityhub
unityhub --headless install --version 6000.0.36f1 --changeset 9fe3b5f71dbb
```

### Open UB-MR in Unity Editor
```bash
/home/maqsood/Unity/Hub/Editor/6000.0.36f1/Editor/Unity -projectPath ~/Documents/UB-MR &
# Then: File → Build Settings → Linux → Build
```

### Run UB-MR Player
```bash
chmod +x ~/Documents/UB-MR/UB-mmr.x86_64
~/Documents/UB-MR/UB-mmr.x86_64
# Select: START HOST → Simple Traffic

# With log output
~/Documents/UB-MR/UB-mmr.x86_64 -logFile /dev/stdout 2>&1 | tee ~/unity_log.txt
```

### WebSocket → UDP Bridge (CARLA → Unity)
```bash
cd ~/Documents/carla-data-server/carla-data-server && source venv/bin/activate
python3 bridges/ws_to_udp_bridge.py --server ws://128.205.222.211:8765 --udp-host localhost --udp-port 12345
```

### Debug Unity / UDP
```bash
# Check if Unity is listening on UDP 12345
ss -ulnp | grep 12345

# Send a test UDP packet manually
python3 -c "
import socket, json
sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
msg = json.dumps({
    'vehicles': [{'id': '999', 'blueprint': 'vehicle.tesla.model3',
                  'color': '255,0,0', 'location': {'x': 0, 'y': 0, 'z': 1}, 'yaw': 0}],
    'timestamp': 12345.0
})
sock.sendto(msg.encode(), ('127.0.0.1', 12345))
print('Sent')
sock.close()
"

# Check Unity logs for spawn/prefab errors
grep -i "spawn\|prefab\|null\|error\|exception\|instantiate" ~/unity_log.txt | head -20
grep -i "NullReference\|vehiclePrefab\|HandleSpawn\|HandleUpdate\|TrafficRenderer" ~/unity_log.txt | head -20
```

---

## Setup from Scratch (Laptop, fresh OS)

```bash
sudo apt install -y python3.10-venv python3-pip
cd ~/Documents/carla-data-server/carla-data-server
python3.10 -m venv venv
source venv/bin/activate
pip install --upgrade pip setuptools wheel
pip install "websockets>=12.0" numpy pygame Pillow
pip install ~/Documents/Cavas_Lab/CARLA_ff009c8a3-dirty/PythonAPI/carla/dist/carla-0.9.16-cp310-cp310-linux_x86_64.whl
python3 -c "import carla; import websockets; print('ok')"
```

---

## Protobuf (regenerate if .proto changes)

```bash
pip install protobuf grpcio-tools
cd ~/Documents/carla-data-server/carla-data-server
python3 -m grpc_tools.protoc --proto_path=proto --python_out=server proto/world_state.proto
```

---

## Full Demo Sequence

### Lab PC (4 terminals)
```bash
# Terminal 1 — CARLA
cd ~/Documents/carla && ./CarlaUE4.sh -quality-level=Low -prefernvidia

# Terminal 2 — Load map
source ~/Documents/carla-data-server/carla-data-server/venv/bin/activate
cd ~/Documents/carla
python3 PythonAPI/util/config.py --map UBAutonomousProvingGrounds

# Terminal 3 — Data server
cd ~/Documents/carla-data-server/carla-data-server && source venv/bin/activate
python3 server/server.py --carla-host localhost --carla-port 2000 --tick-rate 20

# Terminal 4 — Traffic
source ~/Documents/carla-data-server/carla-data-server/venv/bin/activate
cd ~/Documents/carla/PythonAPI/examples
python3 generate_traffic.py -n 50 -w 0 --host localhost --port 2000
```

### Laptop (run any combination)
```bash
# Mirror bridge → laptop CARLA
cd ~/Documents/carla-data-server/carla-data-server && source venv/bin/activate
python3 bridges/carla_mirror_client.py --server ws://128.205.222.211:8765 --shadow-host localhost --shadow-port 2000

# Interactive driver (pygame)
python3 scripts/interactive_driver.py --server ws://128.205.222.211:8765 --spawn-index 5

# UB-MR Unity player (Terminal 1)
~/Documents/UB-MR/UB-mmr.x86_64
# Select: START HOST → Simple Traffic

# WebSocket → UDP bridge (Terminal 2)
python3 bridges/ws_to_udp_bridge.py --server ws://128.205.222.211:8765 --udp-host localhost --udp-port 12345

# Metrics
python3 scripts/metrics_client.py --server ws://128.205.222.211:8765
```
