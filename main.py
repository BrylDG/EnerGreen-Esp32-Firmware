# main.py - EnerGreen ESP32 MicroPython Code with Appliance Signature Detection + Residual Disaggregation
# PATCHED (Heavy Load Fix): improved pre-event snapshot, relaxed stabilization tolerance,
# added forced finalize for long transients, and widened smoothing buffer.

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
POWER_MIN_VALID_WATT = 2.0
POWER_CHANGE_DEBOUNCE_SECONDS = 3
REGULAR_READING_INTERVAL_SECONDS = 60

# PATCH: relaxed and tuned thresholds
APPLIANCE_STABILIZATION_WINDOW = 8
STABILIZATION_VARIATION_WATT = 80.0     # <-- was 5.0, now allows heavy-load variation
TRANSIENT_CAPTURE_WINDOW = 5
MIN_RESIDUAL_WATT = 0.5
MIN_RESIDUAL_RELATIVE = 0.03

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

# PATCH: expanded smoothing buffer
power_window = []
POWER_WINDOW_SIZE = 8  # was 5

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
    print_connected_devices()


def list_connected_devices():
    if not connected_devices:
        return []
    return [f"{info.get('name', 'Unknown')} ({aid}) - {info.get('powerWatt', 0)}W" for aid, info in connected_devices.items()]


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


# ----------------- Event Detection -----------------
def detect_appliance_event(reading):
    global last_power_reading, debounce_active, debounce_start_time, debounce_event_type, active_events

    current_power = smoothed_power(reading["powerWatt"])
    now = utime.time()
    delta_power = current_power - last_power_reading

    # --- Detect Candidate ---
    if (abs(delta_power) > POWER_CHANGE_THRESHOLD_WATT) and (not debounce_active):
        debounce_active = True
        debounce_start_time = now
        debounce_event_type = "ON" if delta_power > 0 else "OFF"
        print("⚡ Power change candidate:", debounce_event_type, "(ΔP={:.1f} W)".format(delta_power))

    # --- Confirm Event ---
    if debounce_active and (now - debounce_start_time) >= POWER_CHANGE_DEBOUNCE_SECONDS:
        if debounce_event_type == "ON" and current_power < POWER_MIN_VALID_WATT:
            print("❌ Ignored false ON ({:.1f} W)".format(current_power))
        else:
            # PATCH: better baseline snapshot (pre-event average)
            if len(power_window) >= 3:
                pre_total = sum(power_window[:-2]) / max(len(power_window[:-2]), 1)
            else:
                pre_total = last_power_reading

            event = {
                "event_type": debounce_event_type,
                "start_time": now,
                "pre_total": pre_total,  # more stable baseline
                "transient_buffer": [],
                "buffer": [],
                "stabilization_time": now + APPLIANCE_STABILIZATION_WINDOW,
                "finalized": False
            }
            active_events.append(event)
            print("✅ Confirmed event:", debounce_event_type, "pre_total={:.1f}W".format(pre_total))

        debounce_active = False
        debounce_event_type = None

    # --- Process Active Events ---
    for event in list(active_events):
        if now - event["start_time"] <= TRANSIENT_CAPTURE_WINDOW:
            event["transient_buffer"].append(reading)
        event["buffer"].append(reading)

        if not event["finalized"] and now >= event["stabilization_time"]:
            if not event["buffer"]:
                active_events.remove(event)
                continue

            stable_power = sum(x["powerWatt"] for x in event["buffer"]) / len(event["buffer"])
            variation = max(x["powerWatt"] for x in event["buffer"]) - min(x["powerWatt"] for x in event["buffer"])

            # PATCH: allow large transient loads to finalize
            if variation > STABILIZATION_VARIATION_WATT and len(event["buffer"]) > APPLIANCE_STABILIZATION_WINDOW * 2:
                print("⚠️ Large variation persisted; forcing finalize (likely heavy load).")
                variation = STABILIZATION_VARIATION_WATT / 2

            if variation <= STABILIZATION_VARIATION_WATT:
                pre = event.get("pre_total", last_power_reading)
                residual = stable_power - pre
                rel = (residual / pre) if pre > 0 else 0.0

                if residual >= MIN_RESIDUAL_WATT or rel >= MIN_RESIDUAL_RELATIVE:
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
                        "event_type": event["event_type"],
                        "signature_data": [sig_point],
                        "timestamp": now,
                        "features": {
                            "steady_avg_power": sig_point["powerWatt"],
                            "powerFactor": sig_point["powerFactor"]
                        }
                    }

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
                    print("⚠️ Residual {:.2f}W (rel {:.2f}) below threshold; ignored.".format(residual, rel))

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
