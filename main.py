# main.py - EnerGreen ESP32 MicroPython Code with Appliance Signature Detection + Residual Disaggregation
# PATCHED (Low-Wattage Fix + Heavy Load Fix + Timer Scope Fix)
#  • Detects low-wattage devices (≥ 5 W)
#  • Handles heavy loads robustly
#  • Uses global timers for low-power detection (fixes '_low_on_timer' errors)

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
POWER_CHANGE_THRESHOLD_WATT = 1.5
POWER_MIN_VALID_WATT = 0.5          # Detects small fans, chargers, etc.
POWER_CHANGE_DEBOUNCE_SECONDS = 3
REGULAR_READING_INTERVAL_SECONDS = 60

APPLIANCE_STABILIZATION_WINDOW = 8
STABILIZATION_VARIATION_WATT = 80.0
TRANSIENT_CAPTURE_WINDOW = 5
MIN_RESIDUAL_WATT = 0.5
MIN_RESIDUAL_RELATIVE = 0.02

# --- Cloud & Wi-Fi config ---
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

active_events = []
connected_devices = {}

power_window = []
POWER_WINDOW_SIZE = 8  # widened smoothing buffer

# NEW: low-power detection timers
low_on_timer = None
low_off_timer = None

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
            print("NTP attempt", i + 1, "failed:", e)
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
    print_connected_devices()


def list_connected_devices():
    if not connected_devices:
        return []
    return [f"{info.get('name', 'Unknown')} ({aid}) - {info.get('powerWatt', 0)}W"
            for aid, info in connected_devices.items()]


def print_connected_devices():
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

        data = response[3:3 + 20]
        if len(data) < 20:
            return None

        def u32_from_low_high(payload, offset):
            low16 = int.from_bytes(payload[offset:offset + 2], 'big')
            high16 = int.from_bytes(payload[offset + 2:offset + 4], 'big')
            return (high16 << 16) | low16

        voltage = int.from_bytes(data[0:2], 'big') / 10.0
        current_raw = u32_from_low_high(data, 2)
        power_raw = u32_from_low_high(data, 6)
        energy_raw = u32_from_low_high(data, 10)
        frequency = int.from_bytes(data[14:16], 'big') / 10.0
        pf = int.from_bytes(data[16:18], 'big') / 100.0

        current = current_raw / 1000.0
        power = power_raw / 10.0
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


# ----------------- Event Detection -----------------
def detect_appliance_event(reading):
    global last_power_reading, debounce_active, debounce_start_time, debounce_event_type
    global active_events, connected_devices, low_on_timer, low_off_timer

    current_power = smoothed_power(reading["powerWatt"])
    now = utime.time()
    delta_power = current_power - last_power_reading

    # --- High-load detection ---
    if abs(delta_power) > 0.3 and not debounce_active:
        debounce_active = True
        debounce_start_time = now
        debounce_event_type = "ON" if delta_power > 0 else "OFF"
        print("⚡ Power change candidate:", debounce_event_type, "(ΔP={:.2f} W)".format(delta_power))

    if debounce_active and (now - debounce_start_time) >= POWER_CHANGE_DEBOUNCE_SECONDS:
        if debounce_event_type == "ON" and current_power < 0.5:
            debounce_active = False
            debounce_event_type = None
        else:
            pre_total = last_power_reading
            active_events.append({
                "event_type": debounce_event_type,
                "start_time": now,
                "pre_total": pre_total,
                "transient_buffer": [],
                "buffer": [],
                "stabilization_time": now + APPLIANCE_STABILIZATION_WINDOW,
                "finalized": False
            })
            print("✅ Confirmed event:", debounce_event_type, "pre_total={:.2f}W".format(pre_total))
            debounce_active = False
            debounce_event_type = None

    # --- Low-power ON watcher ---
    LOW_POWER_ON_THRESHOLD = 2.0   # watts
    LOW_POWER_SUSTAIN_SECONDS = 3  # seconds

    if last_power_reading < 0.8 and current_power >= LOW_POWER_ON_THRESHOLD:
        if low_on_timer is None:
            low_on_timer = now
        elif now - low_on_timer >= LOW_POWER_SUSTAIN_SECONDS:
            print("🌿 Detected low-power appliance ON (~{:.1f} W)".format(current_power))
            sig_point = {
                "timestamp": reading["timestamp"],
                "voltageVolt": reading["voltageVolt"],
                "currentAmp": reading["currentAmp"],
                "powerWatt": round(current_power, 1),
                "powerFactor": reading["powerFactor"]
            }
            event_signature = {
                "dataType": "ApplianceSignature",
                "deviceId": device_id,
                "event_type": "ON",
                "signature_data": [sig_point],
                "timestamp": now,
                "features": {
                    "steady_avg_power": sig_point["powerWatt"],
                    "powerFactor": sig_point["powerFactor"]
                }
            }

            result = send_signature_to_cloud(event_signature, low_power=True)
            if result:
                appliance_id = result.get("applianceID")
                appliance_name = result.get("applianceName", "Unknown")
                if appliance_id:
                    connected_devices[appliance_id] = {
                        "name": appliance_name,
                        "powerWatt": sig_point["powerWatt"],
                        "powerFactor": sig_point["powerFactor"],
                        "last_seen": now
                    }
                    print("🔌 Low-power device connected:", appliance_name, f"({appliance_id})")
                    print_connected_devices()
                else:
                    print("⚠️ No appliance ID returned; cloud may have rejected signature.")
            else:
                print("⚠️ Signature send failed or invalid cloud response.")

            low_on_timer = None
    else:
        low_on_timer = None

    # --- Low-power OFF watcher ---
    total_connected_power = sum(d.get("powerWatt", 0) for d in connected_devices.values())
    if total_connected_power > 0 and current_power < 1.0:
        if low_off_timer is None:
            low_off_timer = now
        elif now - low_off_timer > 5:
            print("🍃 Low-power devices turned off (current {:.1f}W) → clearing registry".format(current_power))
            connected_devices.clear()
            print_connected_devices()
            low_off_timer = None
    else:
        low_off_timer = None

    # --- Finalize active events ---
    for event in list(active_events):
        if now >= event["stabilization_time"] and not event["finalized"]:
            # Compute average stable power
            stable_power = current_power
            residual = stable_power - event["pre_total"]

            if residual >= MIN_RESIDUAL_WATT:
                sig_point = {
                    "timestamp": reading["timestamp"],
                    "voltageVolt": reading["voltageVolt"],
                    "currentAmp": reading["currentAmp"],
                    "powerWatt": round(residual, 1),
                    "powerFactor": reading["powerFactor"]
                }

                event_signature = {
                    "dataType": "ApplianceSignature",
                    "deviceId": device_id,
                    "event_type": event["event_type"],
                    "signature_data": [sig_point],
                    "timestamp": now,
                    "features": {
                        "steady_avg_power": sig_point["powerWatt"],
                        "powerFactor": sig_point["powerFactor"]
                    }
                }

                try:
                    print("📡 Sending signature to:", CLOUD_FUNCTION_URL_SIGNATURE)
                    resp = urequests.post(
                        CLOUD_FUNCTION_URL_SIGNATURE,
                        data=json.dumps(event_signature),
                        headers={"Content-Type": "application/json"}
                    )
                    raw_resp = resp.text
                    print("☁️ Raw cloud response:", raw_resp)
                    try:
                        result = json.loads(raw_resp)
                    except Exception:
                        print("⚠️ Cloud returned invalid JSON")
                        result = {}
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
                        print("🔌 Device connected:", appliance_name, f"({appliance_id})")
                        print_connected_devices()
                    else:
                        print("⚠️ No appliance ID returned; cloud may have rejected signature.")
                except Exception as e:
                    print("❌ Failed to send signature:", e)

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
        reading = pzem_data() if not USE_SIMULATED_DATA else simulate_pzem_data()

        if reading:
            print_reading(reading)
            detect_appliance_event(reading)

            now = utime.time()
            if now - last_regular_reading_time >= REGULAR_READING_INTERVAL_SECONDS:
                payload = reading.copy()
                payload["dataType"] = "RegularReading"
                if "deviceId" not in payload:
                    payload["deviceId"] = device_id
                try:
                    resp = urequests.post(CLOUD_FUNCTION_URL_READING,
                                          data=json.dumps(payload),
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
    
