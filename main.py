# main.py – EnerGreen ESP32 Firmware (Unified Multi-Appliance + Low-Wattage + Heavy-Load Support)
# ------------------------------------------------------------------------------
#  ✅ Detects multiple appliances concurrently
#  ✅ Detects OFF events by matching power drops
#  ✅ Supports low-wattage devices (≥ 2 W)
#  ✅ Keeps heavy-load stability and transient analysis
# ------------------------------------------------------------------------------

import time, json, utime, random, network, urequests, ntptime
from machine import UART

# ------------------------------------------------------------------------------
# Configuration
# ------------------------------------------------------------------------------
USE_SIMULATED_DATA = False
POWER_CHANGE_THRESHOLD_WATT = 1.2     # sensitivity for ON/OFF event candidates
POWER_MIN_VALID_WATT = 0.5            # discard noise
POWER_CHANGE_DEBOUNCE_SECONDS = 3
REGULAR_READING_INTERVAL_SECONDS = 60

APPLIANCE_STABILIZATION_WINDOW = 8
STABILIZATION_VARIATION_WATT = 60.0   # heavy load tolerance
TRANSIENT_CAPTURE_WINDOW = 5
MIN_RESIDUAL_WATT = 0.5
MIN_RESIDUAL_RELATIVE = 0.02

# ------------------------------------------------------------------------------
# Cloud configuration
# ------------------------------------------------------------------------------
try:
    from config import (
        CLOUD_FUNCTION_URL_READING,
        CLOUD_FUNCTION_URL_SIGNATURE,
        WIFI_SSID,
        WIFI_PASSWORD
    )
except ImportError:
    print("⚠️ config.py missing; using defaults.")
    CLOUD_FUNCTION_URL_READING = "http://default_reading_url"
    CLOUD_FUNCTION_URL_SIGNATURE = "http://default_signature_url"
    WIFI_SSID, WIFI_PASSWORD = "ssid", "password"

# ------------------------------------------------------------------------------
# Globals
# ------------------------------------------------------------------------------
device_id = "energreen_esp32"
last_power_reading = 0.0
last_regular_reading_time = 0
debounce_active = False
debounce_start_time = 0
debounce_event_type = None

active_events = []         # pending ON/OFF transitions
connected_devices = {}     # applianceID → info dict

power_window = []
POWER_WINDOW_SIZE = 8
low_on_timer = None
low_off_timer = None

# UART for PZEM
pzem_uart = UART(2, baudrate=9600, tx=17, rx=16, timeout=1000)

# ------------------------------------------------------------------------------
# Utility Functions
# ------------------------------------------------------------------------------
def smoothed_power(new_reading):
    power_window.append(new_reading)
    if len(power_window) > POWER_WINDOW_SIZE:
        power_window.pop(0)
    return sum(power_window) / len(power_window)

def modbus_crc16(data):
    crc = 0xFFFF
    for b in data:
        crc ^= b
        for _ in range(8):
            if crc & 1:
                crc = (crc >> 1) ^ 0xA001
            else:
                crc >>= 1
    return bytes([crc & 0xFF, (crc >> 8) & 0xFF])

def sync_time_with_ntp():
    for i in range(5):
        try:
            ntptime.settime()
            print("NTP synced:", utime.localtime())
            return True
        except Exception:
            time.sleep(2)
    print("⚠️ NTP failed.")
    return False

def print_connected_devices():
    if connected_devices:
        print("➡️ Connected devices:",
              [f"{v['name']} ({k}) - {v['powerWatt']}W" for k, v in connected_devices.items()])
    else:
        print("➡️ No devices connected.")

# ------------------------------------------------------------------------------
# PZEM reading
# ------------------------------------------------------------------------------
def pzem_data():
    try:
        _ = pzem_uart.read()
        time.sleep_ms(50)
        cmd = bytes([0x01, 0x04, 0x00, 0x00, 0x00, 0x0A])
        cmd += modbus_crc16(cmd)
        pzem_uart.write(cmd)
        time.sleep_ms(400)
        res = pzem_uart.read()
        if not res or len(res) < 21: return None
        data = res[3:23]
        def u32(payload, offset):
            low = int.from_bytes(payload[offset:offset+2],'big')
            high = int.from_bytes(payload[offset+2:offset+4],'big')
            return (high<<16)|low
        voltage = int.from_bytes(data[0:2],'big')/10
        current = u32(data,2)/1000
        power = u32(data,6)/10
        pf = int.from_bytes(data[16:18],'big')/100
        energy = u32(data,10)/1000
        return {
            "deviceId":device_id,
            "timestamp":utime.time(),
            "voltageVolt":round(voltage,1),
            "currentAmp":round(current,3),
            "powerWatt":round(power,1),
            "kwhConsumed":round(energy,3),
            "powerFactor":round(pf,2),
        }
    except Exception as e:
        print("Error PZEM:", e)
        return None

# ------------------------------------------------------------------------------
# Cloud helper
# ------------------------------------------------------------------------------
def send_signature_to_cloud(sig):
    try:
        print("📡 Sending signature to cloud...")
        resp = urequests.post(CLOUD_FUNCTION_URL_SIGNATURE,
                              data=json.dumps(sig),
                              headers={"Content-Type": "application/json"})
        raw = resp.text
        print("☁️ Cloud response:", raw)
        try:
            data = json.loads(raw)
        except Exception:
            data = {}
        resp.close()
        return data
    except Exception as e:
        print("❌ Cloud send error:", e)
        return {}

# ------------------------------------------------------------------------------
# Event detection
# ------------------------------------------------------------------------------
def find_best_match_for_drop(drop_watt):
    """Find the connected device whose power roughly matches the observed drop."""
    if not connected_devices:
        return None
    best_id, best_diff = None, 1e9
    for aid, info in connected_devices.items():
        diff = abs(info.get("powerWatt",0)-drop_watt)
        if diff < best_diff:
            best_diff, best_id = diff, aid
    if best_id and best_diff <= max(10,0.4*connected_devices[best_id]['powerWatt']):
        return best_id
    return None

def detect_appliance_event(reading):
    global last_power_reading, debounce_active, debounce_start_time, debounce_event_type
    global active_events, connected_devices, low_on_timer, low_off_timer

    current_power = smoothed_power(reading["powerWatt"])
    now = utime.time()
    delta = current_power - last_power_reading

    # --- Candidate event ---
    if abs(delta) > POWER_CHANGE_THRESHOLD_WATT and not debounce_active:
        debounce_active = True
        debounce_start_time = now
        debounce_event_type = "ON" if delta > 0 else "OFF"
        pre_snapshot = last_power_reading
        print(f"⚡ Candidate {debounce_event_type} ΔP={delta:.1f}W")

    # --- Confirm event ---
    if debounce_active and (now - debounce_start_time) >= POWER_CHANGE_DEBOUNCE_SECONDS:
        event = {
            "event_type": debounce_event_type,
            "start_time": now,
            "pre_total": last_power_reading,
            "stabilization_time": now + APPLIANCE_STABILIZATION_WINDOW,
            "finalized": False
        }
        active_events.append(event)
        print(f"✅ Confirmed {debounce_event_type} pre_total={last_power_reading:.1f}W")
        debounce_active = False
        debounce_event_type = None

    # --- Low-power ON watcher (e.g. clip fan) ---
    if last_power_reading < 1.0 and current_power > 2.0:
        if low_on_timer is None:
            low_on_timer = now
        elif now - low_on_timer > 3:
            print(f"🌿 Low-power ON detected ({current_power:.1f}W)")
            sig = {
                "dataType": "ApplianceSignature",
                "deviceId": device_id,
                "event_type": "ON",
                "signature_data": [{
                    "timestamp": reading["timestamp"],
                    "powerWatt": round(current_power,1),
                    "powerFactor": reading["powerFactor"]
                }],
                "features": {
                    "steady_avg_power": round(current_power,1),
                    "powerFactor": reading["powerFactor"]
                }
            }
            res = send_signature_to_cloud(sig)
            if res.get("applianceID"):
                aid = res["applianceID"]
                connected_devices[aid] = {
                    "name": res.get("applianceName","Unknown"),
                    "powerWatt": sig["features"]["steady_avg_power"],
                    "powerFactor": sig["features"]["powerFactor"],
                    "last_seen": now
                }
                print("🔌 Device connected:", connected_devices[aid]["name"])
            low_on_timer = None
    else:
        low_on_timer = None

    # --- Process active events (multi-appliance logic) ---
    for ev in list(active_events):
        if now >= ev["stabilization_time"] and not ev["finalized"]:
            residual = current_power - ev["pre_total"]
            if ev["event_type"] == "ON" and residual >= MIN_RESIDUAL_WATT:
                sig = {
                    "dataType": "ApplianceSignature",
                    "deviceId": device_id,
                    "event_type": "ON",
                    "signature_data": [{
                        "timestamp": reading["timestamp"],
                        "powerWatt": round(residual,1),
                        "powerFactor": reading["powerFactor"]
                    }],
                    "features": {
                        "steady_avg_power": round(residual,1),
                        "powerFactor": reading["powerFactor"]
                    }
                }
                res = send_signature_to_cloud(sig)
                if res.get("applianceID"):
                    aid = res["applianceID"]
                    connected_devices[aid] = {
                        "name": res.get("applianceName","Unknown"),
                        "powerWatt": sig["features"]["steady_avg_power"],
                        "powerFactor": sig["features"]["powerFactor"],
                        "last_seen": now
                    }
                    print(f"🔌 Device connected: {connected_devices[aid]['name']} ({aid})")
                    print_connected_devices()

            elif ev["event_type"] == "OFF":
                drop = ev["pre_total"] - current_power
                if drop > 0:
                    match = find_best_match_for_drop(drop)
                    if match:
                        info = connected_devices.pop(match)
                        print(f"❌ Device disconnected: {info['name']} ({match})")
                        print_connected_devices()
            ev["finalized"] = True
            active_events.remove(ev)

    last_power_reading = current_power

# ------------------------------------------------------------------------------
# Wi-Fi connection
# ------------------------------------------------------------------------------
sta = network.WLAN(network.STA_IF)
print("Connecting to Wi-Fi...")
sta.active(True)
sta.connect(WIFI_SSID, WIFI_PASSWORD)
for _ in range(30):
    if sta.isconnected(): break
    print(".", end=""); time.sleep(1)
if sta.isconnected():
    print("\n✅ Wi-Fi connected:", sta.ifconfig())
    sync_time_with_ntp()
else:
    print("\n❌ Wi-Fi failed.")

# ------------------------------------------------------------------------------
# Main loop
# ------------------------------------------------------------------------------
print("EnerGreen ready.")
while True:
    try:
        reading = pzem_data() if not USE_SIMULATED_DATA else None
        if reading:
            print(
                f"[Reading]\n"
                f"[V]{reading['voltageVolt']:.1f} V || [C]{reading['currentAmp']:.3f} A || [P]{reading['powerWatt']:.1f} W || [E]{reading['kwhConsumed']:.3f} kWh || [PF]{reading['powerFactor']:.2f}\n"
                "----------------------------------------------------------------"
            )
            detect_appliance_event(reading)

            now = utime.time()
            if now - last_regular_reading_time >= REGULAR_READING_INTERVAL_SECONDS:
                reading["dataType"] = "RegularReading"
                resp = urequests.post(CLOUD_FUNCTION_URL_READING,
                                      data=json.dumps(reading),
                                      headers={"Content-Type": "application/json"})
                print("☁️ Readings response:", resp.text)
                resp.close()
                last_regular_reading_time = now
        else:
            print("No reading.")
        time.sleep(1)
    except Exception as e:
        print("Loop error:", e)
        time.sleep(5)

