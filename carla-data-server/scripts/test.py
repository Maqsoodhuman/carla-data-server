import carla, time

c = carla.Client('localhost', 2000)
c.set_timeout(10)
w = c.get_world()

s = w.get_settings()
s.synchronous_mode = True
s.fixed_delta_seconds = 0.05
w.apply_settings(s)

bp = w.get_blueprint_library().find('vehicle.tesla.model3')
sp = w.get_map().get_spawn_points()[5]
car = w.spawn_actor(bp, sp)

# Spawn camera like our interactive driver does
cam_bp = w.get_blueprint_library().find('sensor.camera.rgb')
cam_bp.set_attribute('image_size_x', '800')
cam_bp.set_attribute('image_size_y', '600')
cam = w.spawn_actor(cam_bp, carla.Transform(carla.Location(x=-5.5,z=2.8),carla.Rotation(pitch=-12)), attach_to=car)
cam.listen(lambda img: None)
w.tick()
print(f'Spawned car={car.id} cam={cam.id}')

print('Throttle ON 2s...')
for i in range(40):
	car.apply_control(carla.VehicleControl(throttle=1.0))
	w.tick()
v = car.get_velocity()
print(f'  speed={(v.x**2+v.y**2+v.z**2)**0.5*3.6:.1f} km/h')

print('Throttle OFF 3s...')
for i in range(60):
	car.apply_control(carla.VehicleControl(throttle=0.0))
	w.tick()
	if i % 10 == 0:
		v = car.get_velocity()
		print(f'  speed={(v.x**2+v.y**2+v.z**2)**0.5*3.6:.1f} km/h')

cam.destroy()
car.destroy()
s = w.get_settings()
s.synchronous_mode = False
w.apply_settings(s)
print('Done')
