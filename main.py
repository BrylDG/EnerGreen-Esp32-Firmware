# main.py - EnerGreen ESP32 MicroPython Code with Appliance Signature Detection + Residual Disaggregation
# PATCHED: use pre-event snapshot to compute residuals so small devices after large ones are detected.

import time
import random
import network
import urequests
import json
import utime
import ntptime
from machine import UART

# --- Configuration ---
USE_SIMULATED_DATA = False
POWER_CHANGE_THRESHOLD_WATT = 1.5   # detect step changes above this
POWER_MIN_VALID_WATT = 2.0          # ignore trivial ON events
POWER_CHANGE_DEBOUNCE_SECONDS = 3
REGULAR_READING_INTERVAL_SECONDS = 60

APPLIANCE_STABILIZATION_WINDOW = 8   # seconds before finalizing event
STABILIZATION_VARIATION_WATT = 5.0   # max steady variation allowed
TRANSIENT_CAPTURE_WINDOW = 5         # seconds for transient capture
MIN_RESIDUAL_WATT = 0.5              # min residual to consider a new device
MIN_RESIDUAL_RELATIVE = 0.03         # or 3% relative increase (helps detect small devices on large baseline)

# --- Cloud & Wi-Fi config (in config.py) ---
try:
    from config import (
        CLOUD_FUNCTION_URL_READING,
        CLOUD_FUNCTION_URL_SIGNATURE,
        WIFI_SSID,
        WIFI_PASSWORD
    )
except ImportError:
    print("Error: config.py missing. Using defaults.")
    CLOUD_FUNCTION_URL_READING = 'http://default_readings_url'
    CLOUD_FUNCTION_URL_SIGNATURE = 'http://default_signatures_url'
    WIFI_SSID = 'default_ssid'
    WIFI_PASSWORD = 'default_password'

# --- Globals ---
device_id = "energreen_esp32_002"
last_power_reading = 0.0
last_regular_reading_time = 0
debounce_active = False
debounce_start_time = 0
debounce_event_type = None

active_events = []         # list of pending events (each has buffers)
connected_devices = {}     # applianceID -> { name, powerWatt, powerFactor, last_seen }

# smoothing buffer
power_window = []
POWER_WINDOW_SIZE = 5

# UART to PZEM
pzem_uart = UART(2, baudrate=9600, tx=17, rx=16, timeout=1000)


# ----------------- Helpers -----------------
def smoothed_power(new_reading):
    global power_window
    power_window.append(new_reading)
    if len(power_window) > POWER_WINDOW_SIZE:
        power_window.pop(0)
    return sum(power_window) / len(power_window)


def modbus_crc16(data):
    crc = 0xFFFF
    for b in data:
        crc ^= b
        for _ in range(8):
            if crc & 0x0001:
                crc >>= 1
                crc ^= 0xA001
            else:
                crc >>= 1
    return bytes([crc & 0xFF, (crc >> 8) & 0xFF])


def sync_time_with_ntp(max_retries=10, timeout=5):
    ntptime.host = 'pool.ntp.org'
    for i in range(max_retries):
        try:
            ntptime.settime()
            print("NTP synced:", utime.localtime())
            return True
        except Exception as e:
            print("NTP attempt", i+1, "failed:", e)
            time.sleep(timeout)
    print("NTP failed after retries")
    return False


def print_reading(reading):
    print("[Reading] {v:.1f} V | {c:.3f} A | {p:.1f} W | PF {pf:.2f} | kWh {k:.3f}".format(
        v=reading["voltageVolt"],
        c=reading["currentAmp"],
        p=reading["powerWatt"],
        pf=reading["powerFactor"],
        k=reading["kwhConsumed"]
    ))
    print_connected_devices()   # Always show connected devices after reading


def list_connected_devices():
    """Return a list of connected device names + IDs."""
    if not connected_devices:
        return []
    return [f"{info.get('name', 'Unknown')} ({aid}) - {info.get('powerWatt', 0)}W" for aid, info in connected_devices.items()]


def print_connected_devices():
    """Print all connected devices in a list form."""
    devices = list_connected_devices()
    if devices:
        print("➡️ Connected devices:", devices)
    else:
        print("➡️ No devices connected.")


# ----------------- PZEM read -----------------
def pzem_data():
    try:
        _ = pzem_uart.read()
        time.sleep_ms(50)

        base_cmd = bytes([0x01, 0x04, 0x00, 0x00, 0x00, 0x0A])
        crc = modbus_crc16(base_cmd)
        cmd = base_cmd + crc
        pzem_uart.write(cmd)
        time.sleep_ms(500)

        response = pzem_uart.read()
        if not response or len(response) < 21:
            return None

        data = response[3:3+20]
        if len(data) < 20:
            return None

        def u32_from_low_high(payload, offset):
            low16 = int.from_bytes(payload[offset:offset+2], 'big')
            high16 = int.from_bytes(payload[offset+2:offset+4], 'big')
            return (high16 << 16) | low16

        voltage = int.from_bytes(data[0:2], 'big') / 10.0
        current_raw = u32_from_low_high(data, 2)
        power_raw   = u32_from_low_high(data, 6)
        energy_raw  = u32_from_low_high(data, 10)
        frequency   = int.from_bytes(data[14:16], 'big') / 10.0
        pf          = int.from_bytes(data[16:18], 'big') / 100.0

        current = current_raw / 1000.0
        power   = power_raw / 10.0
        energy_kwh = energy_raw / 1000.0

        if not (0.0 <= voltage <= 300.0): return None
        if not (0.0 <= current <= 200.0): return None
        if not (-10000.0 <= power <= 10000.0): return None

        return {
            "deviceId": device_id,
            "timestamp": utime.time(),
            "voltageVolt": round(voltage, 1),
            "currentAmp": round(current, 3),
            "powerWatt": round(power, 1),
            "kwhConsumed": round(energy_kwh, 3),
            "frequencyHz": round(frequency, 1),
            "powerFactor": round(pf, 2),
            "energySource": "Grid"
        }

    except Exception as e:
        print("Error in pzem_data:", e)
        return None


def simulate_pzem_data():
    voltage = round(random.uniform(220.0, 240.0), 1)
    current = round(random.uniform(0.1, 5.0), 3)
    power = round(voltage * current * random.uniform(0.8, 1.0), 1)
    return {
        "deviceId": device_id,
        "voltageVolt": voltage,
        "currentAmp": current,
        "powerWatt": power,
        "kwhConsumed": round(random.uniform(100.0, 500.0), 3),
        "frequencyHz": round(random.uniform(59.8, 60.2), 1),
        "powerFactor": round(random.uniform(0.7, 0.99), 2),
        "energySource": "Grid",
        "timestamp": utime.time()
    }


# ----------------- Disaggregation utilities -----------------
def baseline_power_sum():
    return sum(v.get("powerWatt", 0.0) for v in connected_devices.values())


def find_best_match_for_drop(drop_watt):
    if not connected_devices:
        return None
    best_id = None
    best_diff = None
    for aid, info in connected_devices.items():
        p = info.get("powerWatt", 0.0)
        diff = abs(p - drop_watt)
        if best_diff is None or diff < best_diff:
            best_diff = diff
            best_id = aid
    if best_id:
        best_val = connected_devices[best_id].get("powerWatt", 0.0)
        if abs(best_val - drop_watt) <= max(0.4 * best_val, 10.0):
            return best_id
    return None


# ----------------- Event detection & finalization -----------------
def detect_appliance_event(reading):
    global last_power_reading, debounce_active, debounce_start_time, debounce_event_type, active_events

    current_power = smoothed_power(reading["powerWatt"])
    now = utime.time()
    delta_power = current_power - last_power_reading

    # Candidate detection
    if (abs(delta_power) > POWER_CHANGE_THRESHOLD_WATT) and (not debounce_active):
        debounce_active = True
        debounce_start_time = now
        debounce_event_type = "ON" if delta_power > 0 else "OFF"
        # store pre-event snapshot at candidate moment (helps compute residual later)
        pre_total_snapshot = last_power_reading
        print("⚡ Power change candidate:", debounce_event_type, "(ΔP={:.1f} W)".format(delta_power))

    # confirm after debounce
    if debounce_active and (now - debounce_start_time) >= POWER_CHANGE_DEBOUNCE_SECONDS:
        if debounce_event_type == "ON" and current_power < POWER_MIN_VALID_WATT:
            print("❌ Ignored false ON ({:.1f} W)".format(current_power))
        else:
            # create event with pre_total snapshot recorded
            # NOTE: Use last_power_reading (smoothed) as pre_total
            event = {
                "event_type": debounce_event_type,
                "start_time": now,
                "pre_total": last_power_reading,   # <<--- important snapshot
                "transient_buffer": [],
                "buffer": [],
                "stabilization_time": now + APPLIANCE_STABILIZATION_WINDOW,
                "finalized": False
            }
            active_events.append(event)
            print("✅ Confirmed event:", debounce_event_type, "pre_total={:.1f}W".format(event["pre_total"]))

        debounce_active = False
        debounce_event_type = None

    # update pending events
    for event in list(active_events):
        # capture transient (first seconds)
        if now - event["start_time"] <= TRANSIENT_CAPTURE_WINDOW:
            event["transient_buffer"].append({
                "timestamp": reading["timestamp"],
                "voltageVolt": reading["voltageVolt"],
                "currentAmp": reading["currentAmp"],
                "powerWatt": current_power,
                "powerFactor": reading["powerFactor"]
            })
        # always capture steady buffer
        event["buffer"].append({
            "timestamp": reading["timestamp"],
            "voltageVolt": reading["voltageVolt"],
            "currentAmp": reading["currentAmp"],
            "powerWatt": current_power,
            "powerFactor": reading["powerFactor"]
        })

        # finalize after stabilization window
        if not event["finalized"] and now >= event["stabilization_time"]:
            if len(event["buffer"]) == 0:
                active_events.remove(event)
                continue

            stable_power = sum(x["powerWatt"] for x in event["buffer"]) / len(event["buffer"])
            variation = max(x["powerWatt"] for x in event["buffer"]) - min(x["powerWatt"] for x in event["buffer"])

            if variation <= STABILIZATION_VARIATION_WATT:
                # --- Use pre_total snapshot to compute residual for ON events ---
                if event["event_type"] == "ON":
                    pre = event.get("pre_total", last_power_reading)
                    residual = stable_power - pre  # incremental increase due to new device
                    # Accept residual if absolute OR relative threshold exceeded
                    rel = (residual / pre) if pre > 0 else 0.0
                    if -1.0 < residual < 0.0:
                        residual = 0.0
                        
                    if residual >= MIN_RESIDUAL_WATT or (pre > 0 and residual >= pre * MIN_RESIDUAL_RELATIVE):
                        # build signature feature including transient-derived features
                        transient_powers = [x["powerWatt"] for x in event["transient_buffer"]]
                        rise_time = None
                        overshoot = None
                        if transient_powers:
                            target = round(residual, 1)
                            for i, val in enumerate(transient_powers):
                                if val >= 0.9 * target:
                                    rise_time = i
                                    break
                            overshoot = max(transient_powers) - target

                        sig_point = {
                            "timestamp": event["buffer"][-1]["timestamp"],
                            "voltageVolt": event["buffer"][-1]["voltageVolt"],
                            "currentAmp": event["buffer"][-1]["currentAmp"],
                            "powerWatt": round(residual, 1),
                            "powerFactor": event["buffer"][-1]["powerFactor"]
                        }
                        event_signature = {
                            "dataType": "ApplianceSignature",
                            "deviceId": device_id,
                            "event_type": "ON",
                            "signature_data": [sig_point],
                            "transient_data": event["transient_buffer"],
                            "steady_state_data": event["buffer"],
                            "timestamp": now,
                            "features": {
                                "steady_avg_power": round(residual, 1),
                                "powerFactor": sig_point["powerFactor"],
                                "rise_time": rise_time if rise_time is not None else -1,
                                "overshoot": round(overshoot, 1) if overshoot is not None else 0.0
                            }
                        }
                        # send signature
                        try:
                            resp = urequests.post(CLOUD_FUNCTION_URL_SIGNATURE, data=json.dumps(event_signature),
                                                  headers={"Content-Type": "application/json"})
                            result = {}
                            try:
                                result = resp.json()
                            except Exception:
                                print("⚠️ Signature response not JSON:", resp.text)
                            resp.close()

                            appliance_id = result.get("applianceID")
                            appliance_name = result.get("applianceName", "Unknown")
                            if appliance_id:
                                connected_devices[appliance_id] = {
                                    "name": appliance_name,
                                    "powerWatt": sig_point["powerWatt"],
                                    "powerFactor": sig_point["powerFactor"],
                                    "last_seen": now
                                }
                                print("🔌 Device connected:", appliance_name, "(", appliance_id, ")")
                                print("📊 Devices connected:", len(connected_devices))
                                print_connected_devices()
                            else:
                                print("⚠️ Signature stored but no applianceID returned.")
                        except Exception as e:
                            print("❌ Failed to send signature:", e)
                    else:
                        print("⚠️ Residual {:.2f}W (rel {:.2f}) below threshold; possible small device not recorded.".format(residual, rel))

                # OFF handling: compute drop relative to pre_total as well
                elif event["event_type"] == "OFF":
                    # Use pre_total snapshot to compute drop estimate
                    # pre_total was stored when the event was created (approx system power before OFF)
                    pre = event.get("pre_total", None)
                    if pre is None:
                        # fallback: use first transient (if available)
                        first_pt = event["transient_buffer"][0] if event["transient_buffer"] else None
                        pre = first_pt["powerWatt"] if first_pt else 0.0

                    drop = pre - stable_power
                    if drop > 0:
                        matched = find_best_match_for_drop(drop)
                        if matched:
                            info = connected_devices.pop(matched, None)
                            if info:
                                print("❌ Device disconnected:", info.get("name", "Unknown"), "(", matched, ")")
                                print("📊 Devices connected:", len(connected_devices))
                                print_connected_devices()
                        else:
                            # Failsafe: if measured total is very small, clear list
                            if stable_power < MIN_RESIDUAL_WATT and connected_devices:
                                print("⚠️ Power near zero after OFF. Resetting connected devices.")
                                connected_devices.clear()
                            else:
                                print("⚠️ OFF drop {:.2f}W didn't match known devices.".format(drop))

                else:
                    print("⚠️ Unknown event type:", event["event_type"])

            else:
                print("⚠️ Event discarded: unstable variation {:.1f} W".format(variation))

            event["finalized"] = True
            try:
                active_events.remove(event)
            except ValueError:
                pass

    last_power_reading = current_power


# ----------------- Wi-Fi connect -----------------
sta_if = network.WLAN(network.STA_IF)
print("Connecting to Wi-Fi...")
sta_if.active(True)
sta_if.connect(WIFI_SSID, WIFI_PASSWORD)
timeout = 0
while not sta_if.isconnected() and timeout < 30:
    print('.', end='')
    time.sleep(1)
    timeout += 1

if sta_if.isconnected():
    print("\nWi-Fi connected:", sta_if.ifconfig())
    sync_time_with_ntp()
else:
    print("\nWi-Fi connection failed. Check credentials.")

# ----------------- Main loop -----------------
print("EnerGreen ESP32 Ready. Starting loop...")
while True:
    try:
        reading = simulate_pzem_data() if USE_SIMULATED_DATA else pzem_data()

        if reading:
            print_reading(reading)   # prints reading + device list
            detect_appliance_event(reading)   # process detection

            now = utime.time()
            if now - last_regular_reading_time >= REGULAR_READING_INTERVAL_SECONDS:
                payload = reading.copy()
                payload["dataType"] = "RegularReading"
                if "deviceId" not in payload:
                    payload["deviceId"] = device_id
                try:
                    resp = urequests.post(CLOUD_FUNCTION_URL_READING, data=json.dumps(payload),
                                          headers={"Content-Type": "application/json"})
                    print("☁️ Readings response:", resp.text)
                    resp.close()
                    last_regular_reading_time = now
                except Exception as e:
                    print("Failed to send reading:", e)
        else:
            print("No readings")

        time.sleep(1)

    except Exception as loop_err:
        print("Loop error:", loop_err)
        time.sleep(5)
