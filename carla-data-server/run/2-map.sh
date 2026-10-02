#!/usr/bin/env bash
# 2. Load the UB map and report whether it has ground.
set -u
VENV="${VENV:-/home/maqsood/Documents/carla-data-server/carla-data-server/venv/bin/python}"
MAP="${MAP:-UBAutonomousProvingGrounds}"
PORT="${PORT:-2000}"

"$VENV" - "$MAP" "$PORT" <<'PY'
import sys, carla
name, port = sys.argv[1], int(sys.argv[2])
c = carla.Client('localhost', port); c.set_timeout(180.0)
w = c.load_world(name)
print('map         :', w.get_map().name)
print('env objects :', len(w.get_environment_objects()))
sp = w.get_map().get_spawn_points()
print('spawn points:', len(sp))
p = sp[0].location
hits = w.cast_ray(carla.Location(p.x, p.y, 200), carla.Location(p.x, p.y, -200))
print('ground      :', 'YES' if hits else 'NO - cars will fall through')
PY
