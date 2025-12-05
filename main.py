# main.py - EnerGreen merged firmware (MQTT + NILM + Multi-appliance + Low-watt + Heavy-load)
# Option A: firmware stabilizes signature, publishes to MQTT TOPIC_SIGNATURE,
# waits for cloud match on TOPIC_MATCH, registers matched appliance locally and publishes CONNECT to TOPIC_CONNECTED.
# -------------------------------------------------------------------------

import time
import json
import utime
import gc
import random
import network
import urequests
import ssl
import usocket as socket
from machine import UART
from umqtt.simple import MQTTClient
import os

# -------------------------------------------------------------------------
# Configuration (from config.py)
# -------------------------------------------------------------------------
try:
    from config import (
        WIFI_SSID,
        WIFI_PASSWORD,
        MQTT_BROKER,
        MQTT_PORT,
        MQTT_USER,
        MQTT_PASS,
        MQTT_CLIENT_ID,
        TOPIC_REGULAR,
        TOPIC_SIGNATURE,
        TOPIC_CONNECTED,
        TOPIC_MATCH
    )
except ImportError:
    # Provide safe defaults to avoid crashes during early testing
    print("⚠️ config.py missing or incomplete — using placeholders.")
    WIFI_SSID = "ssid"
    WIFI_PASSWORD = "password"
    MQTT_BROKER = "broker.hivemq.com"
    MQTT_PORT = 8883
    MQTT_USER = "energreen_user"
    MQTT_PASS = ""
    MQTT_CLIENT_ID = "energreen_client"
    TOPIC_REGULAR = b"/energy/003/readings"
    TOPIC_SIGNATURE = b"/energy/003/signatures"
    TOPIC_CONNECTED = b"/energy/003/connected"
    TOPIC_MATCH = b"/energy/003/matches"

# -------------------------------------------------------------------------
# Tuning parameters
# -------------------------------------------------------------------------
USE_SIMULATED_DATA = False

POWER_CHANGE_THRESHOLD_WATT = 1.2
POWER_MIN_VALID_WATT = 0.5
POWER_CHANGE_DEBOUNCE_SECONDS = 3
REGULAR_READING_INTERVAL_SECONDS = 60

APPLIANCE_STABILIZATION_WINDOW = 8
STABILIZATION_VARIATION_WATT = 60.0
TRANSIENT_CAPTURE_WINDOW = 5
MIN_RESIDUAL_WATT = 0.5
MIN_RESIDUAL_RELATIVE = 0.02

POWER_WINDOW_SIZE = 8  # smoothing

EVENT_COOLDOWN_SECONDS = 6
event_cooldown_expiry = 0

# -------------------------------------------------------------------------
# Globals
# -------------------------------------------------------------------------
device_id = MQTT_USER                # confirmed by you
last_power_reading = 0.0
last_regular_reading_time = 0

# Debounce & multi-event buffers
debounce_active = False
debounce_start_time = 0
debounce_event_type = None
debounce_buffer = []
cumulative_delta = 0.0

active_events = []         # pending ON/OFF transitions with capture buffers
connected_devices = {}     # applianceID -> { name, powerWatt, powerFactor, last_seen }

# smoothing buffer
power_window = []

# low-watt timers
low_on_timer = None
low_off_timer = None

# MQTT client state
mqtt_client = None
mqtt_connected = False

# pending matches store: signature_nonce -> dict (filled by callback)
pending_matches = {}

# UART to PZEM
pzem_uart = UART(2, baudrate=9600, tx=17, rx=16, timeout=1000)

# -------------------------------------------------------------------------
# Helpers
# -------------------------------------------------------------------------
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

def compute_variation(buffer):
    if not buffer:
        return 0.0
    vals = [b["powerWatt"] for b in buffer]
    return max(vals) - min(vals)

def compute_slope(buffer):
    if not buffer or len(buffer) < 2:
        return 0.0
    total_slope = 0.0
    count = 0
    for i in range(1, len(buffer)):
        dt = buffer[i]["timestamp"] - buffer[i-1]["timestamp"]
        if dt <= 0:
            continue
        dp = buffer[i]["powerWatt"] - buffer[i-1]["powerWatt"]
        total_slope += abs(dp / dt)
        count += 1
    return (total_slope / count) if count else 0.0

def print_connected_devices():
    if connected_devices:
        print("➡️ Connected devices:", [f"{v['name']} ({k}) - {v['powerWatt']}W" for k, v in connected_devices.items()])
    else:
        print("➡️ No devices connected.")

# -------------------------------------------------------------------------
# PZEM reading
# -------------------------------------------------------------------------
def pzem_data():
    try:
        # flush UART
        try:
            _ = pzem_uart.read()
        except Exception:
            pass
        time.sleep_ms(30)
        base_cmd = bytes([0x01, 0x04, 0x00, 0x00, 0x00, 0x0A])
        crc = modbus_crc16(base_cmd)
        cmd = base_cmd + crc
        pzem_uart.write(cmd)
        time.sleep_ms(300)
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
        power_raw = u32_from_low_high(data, 6)
        energy_raw = u32_from_low_high(data, 10)
        frequency = int.from_bytes(data[14:16], 'big') / 10.0
        pf = int.from_bytes(data[16:18], 'big') / 100.0

        current = current_raw / 1000.0
        power = power_raw / 10.0
        energy_kwh = energy_raw / 1000.0

        # basic sanity checks
        if not (0.0 <= voltage <= 300.0):
            return None
        if not (0.0 <= current <= 200.0):
            return None
        if not (-10000.0 <= power <= 10000.0):
            return None

        return {
            "deviceId": device_id,
            "timestamp": utime.time(),
            "voltageVolt": round(voltage,1),
            "currentAmp": round(current,3),
            "powerWatt": round(power,1),
            "kwhConsumed": round(energy_kwh,3),
            "frequencyHz": round(frequency,1),
            "powerFactor": round(pf,2)
        }

    except Exception as e:
        print("Error reading PZEM:", e)
        return None

def simulate_pzem_data():
    voltage = round(random.uniform(220.0,240.0),1)
    current = round(random.uniform(0.01,2.0),3)
    power = round(voltage * current * random.uniform(0.8, 1.0), 1)
    return {
        "deviceId": device_id,
        "timestamp": utime.time(),
        "voltageVolt": voltage,
        "currentAmp": current,
        "powerWatt": power,
        "kwhConsumed": round(random.uniform(100.0,500.0),3),
        "frequencyHz": round(random.uniform(59.8,60.2),1),
        "powerFactor": round(random.uniform(0.6,0.99),2)
    }

# -------------------------------------------------------------------------
# MQTT helpers
# -------------------------------------------------------------------------
def mqtt_connect():
    global mqtt_client, mqtt_connected
    try:
        # Use SSL if port is 8883, otherwise plain
        use_ssl = True if MQTT_PORT == 8883 else False
        mqtt_client = MQTTClient(client_id=MQTT_CLIENT_ID,
                                 server=MQTT_BROKER,
                                 port=MQTT_PORT,
                                 user=MQTT_USER,
                                 password=MQTT_PASS,
                                 ssl=use_ssl,
                                 ssl_params={"server_hostname": MQTT_BROKER} if use_ssl else None)
        # set callback BEFORE connect so subscriptions are handled
        mqtt_client.set_callback(mqtt_callback)
        mqtt_client.connect()
        mqtt_connected = True
        print("✅ MQTT connected")
        # subscribe to MATCH topic (cloud -> device)
        try:
            mqtt_client.subscribe(TOPIC_MATCH)
            print("✅ Subscribed to match topic:", TOPIC_MATCH)
        except Exception as e:
            print("⚠️ Subscribe failed:", e)
        return True
    except Exception as e:
        mqtt_connected = False
        print("❌ MQTT connect failed:", e)
        return False

def mqtt_publish(topic, payload):
    global mqtt_connected, mqtt_client
    if not mqtt_connected:
        ok = mqtt_connect()
        if not ok:
            print("⚠️ MQTT not connected; skipping publish.")
            return False
    try:
        if not isinstance(payload, (bytes, bytearray)):
            payload_bytes = json.dumps(payload)
        else:
            payload_bytes = payload
        mqtt_client.publish(topic, payload_bytes)
        print("📤 Published to", topic)
        return True
    except Exception as e:
        mqtt_connected = False
        print("❌ MQTT publish failed:", e)
        return False

# -------------------------------------------------------------------------
# MQTT callback - receives match responses from cloud
# Payload expected: {"signature_nonce":"abc123","applianceID":"...","applianceName":"...","matchScore":0.9}
# -------------------------------------------------------------------------
def mqtt_callback(topic, msg):
    global pending_matches
    try:
        # msg may be bytes
        payload = msg.decode() if isinstance(msg, (bytes, bytearray)) else str(msg)
        data = json.loads(payload)
        if not data:
            return
        nonce = data.get("signature_nonce") or data.get("signatureNonce") or data.get("nonce")
        if nonce:
            pending_matches[nonce] = {
                "applianceID": data.get("applianceID"),
                "applianceName": data.get("applianceName"),
                "matchScore": data.get("matchScore", 0.0),
                "raw": data
            }
            print("☁️ Received match for nonce", nonce, "->", pending_matches[nonce])
    except Exception as e:
        print("⚠️ mqtt_callback error:", e)

# -------------------------------------------------------------------------
# Publish signature and wait for cloud match
# -------------------------------------------------------------------------
def publish_signature_and_wait_for_match(signature_msg, wait_seconds=6):
    """
    Publishes signature_msg (dict) to TOPIC_SIGNATURE with generated signature_nonce.
    Then waits up to wait_seconds for the cloud to publish a response to TOPIC_MATCH
    that contains the same signature_nonce. Returns matched dict or None.
    """
    # ensure signature_nonce
    nonce = signature_msg.get("signature_nonce")
    if not nonce:
        # simple nonce: device + timestamp + random int
        nonce = "{}-{}-{}".format(device_id, int(utime.time()), random.getrandbits(16))
        signature_msg["signature_nonce"] = nonce

    # clear any stale pending
    if nonce in pending_matches:
        try:
            del pending_matches[nonce]
        except:
            pass

    # publish
    ok = mqtt_publish(TOPIC_SIGNATURE, signature_msg)
    if not ok:
        print("❌ Failed to publish signature")
        return None

    # wait loop
    deadline = utime.time() + wait_seconds
    while utime.time() < deadline:
        # mqtt_client.check_msg will invoke callback and fill pending_matches
        try:
            # non-blocking check for messages
            mqtt_client.check_msg()
        except Exception:
            # try reconnect once
            if not mqtt_connect():
                time.sleep(0.5)
        if nonce in pending_matches:
            match = pending_matches.pop(nonce)
            return match
        # small sleep
        time.sleep_ms(200)
    print("⚠️ No match response for nonce", nonce)
    return None

# -------------------------------------------------------------------------
# Send connected (connect/disconnect) event to cloud
# -------------------------------------------------------------------------
def publish_connected_event(device_info, action="connect"):
    payload = {
        "deviceId": device_id,
        "action": action,
        "applianceID": device_info.get("applianceID"),
        "applianceName": device_info.get("name"),
        "powerWatt": device_info.get("powerWatt"),
        "powerFactor": device_info.get("powerFactor"),
        "timestamp": utime.time()
    }
    mqtt_publish(TOPIC_CONNECTED, payload)

# -------------------------------------------------------------------------
# NILM detection & event handling (main logic)
# Uses debounce + transient + steady buffers similar to your NILM firmware.
# -------------------------------------------------------------------------
def find_best_match_for_drop(drop_watt):
    if not connected_devices:
        return None
    best_id, best_diff = None, 1e9
    for aid, info in connected_devices.items():
        diff = abs(info.get("powerWatt", 0.0) - drop_watt)
        if diff < best_diff:
            best_diff, best_id = diff, aid
    if best_id and best_diff <= max(10, 0.4 * connected_devices[best_id].get("powerWatt", 0.0)):
        return best_id
    return None

def detect_appliance_event(reading):
    """
    This function is called every reading. It:
    - smooths power
    - detects candidate events (step or cumulative)
    - manages debounce and active_events buffers
    - finalizes events after APPLIANCE_STABILIZATION_WINDOW by stabilizing and (for ON) publishing signature
    """
    global last_power_reading, debounce_active, debounce_start_time, debounce_event_type
    global debounce_buffer, cumulative_delta, active_events, connected_devices
    global low_on_timer, low_off_timer, event_cooldown_expiry

    current_power = smoothed_power(reading["powerWatt"])
    now = utime.time()
    
    if now < event_cooldown_expiry:
        last_power_reading = current_power 
        return
    # initialize baseline
    if last_power_reading == 0.0:
        last_power_reading = current_power

    step_delta = current_power - last_power_reading
    cumulative_delta += step_delta

    # Candidate detection triggers (step or cumulative)
    trigger_now = False
    trigger_value = 0.0
    trigger_reason = None
    if (abs(step_delta) >= POWER_CHANGE_THRESHOLD_WATT) and (not debounce_active):
        trigger_now = True
        trigger_value = step_delta
        trigger_reason = "step"
    elif (abs(cumulative_delta) >= POWER_CHANGE_THRESHOLD_WATT) and (not debounce_active):
        trigger_now = True
        trigger_value = cumulative_delta
        trigger_reason = "cumulative"

    if trigger_now:
        debounce_active = True
        debounce_start_time = now
        debounce_event_type = "ON" if trigger_value > 0 else "OFF"
        debounce_buffer = [current_power]
        print("⚡ Candidate", debounce_event_type, "reason=", trigger_reason, "val={:.2f}W".format(trigger_value))
    elif debounce_active:
        debounce_buffer.append(current_power)

    # Confirm after debounce window
    if debounce_active and (now - debounce_start_time) >= POWER_CHANGE_DEBOUNCE_SECONDS:
        avg_power = sum(debounce_buffer) / len(debounce_buffer) if debounce_buffer else current_power
        variation = max(debounce_buffer) - min(debounce_buffer) if debounce_buffer else 0.0
        STABILITY_THRESHOLD_W = max(1.5, STABILIZATION_VARIATION_WATT / 6.0)
        if variation <= STABILITY_THRESHOLD_W:
            # confirm event and capture with pre_total snapshot
            active_events.append({
                "event_type": debounce_event_type,
                "start_time": now,
                "pre_total": last_power_reading,
                "buffer": [],            # steady buffer (used for stabilization)
                "transient_buffer": [],  # initial transient buffer
                "finalized": False
            })
            print("✅ Confirmed event:", debounce_event_type, "pre_total={:.1f}W".format(last_power_reading))
            event_cooldown_expiry = now + EVENT_COOLDOWN_SECONDS
            print("⏳ Cooldown started for {}s".format(EVENT_COOLDOWN_SECONDS))
        else:
            print("⚠️ Debounce rejected: unstable (variation={:.1f}W)".format(variation))

        # reset debounce
        debounce_active = False
        debounce_buffer = []
        cumulative_delta = 0.0
        debounce_event_type = None

    # Low-power ON watcher (e.g., clip fan) - queue a synthetic ON event for stabilization
    LOW_POWER_ON_THRESHOLD = 2.0
    LOW_POWER_SUSTAIN_SECONDS = 3
    if last_power_reading < 0.8 and current_power >= LOW_POWER_ON_THRESHOLD:
        if low_on_timer is None:
            low_on_timer = now
        elif now - low_on_timer >= LOW_POWER_SUSTAIN_SECONDS:
            ev = {
                "event_type": "ON",
                "start_time": now,
                "pre_total": last_power_reading,
                "buffer": [],
                "transient_buffer": [],
                "finalized": False,
                "low_power": True
            }
            active_events.append(ev)
            print("🌿 Low-power ON suspected ({:.1f}W) — queued for stabilization".format(current_power))
            low_on_timer = None
    else:
        low_on_timer = None

    # Low-power OFF clearing logic (if total connected power > 0 and reading drops very low)
    total_connected_power = sum(v.get("powerWatt", 0) for v in connected_devices.values())
    if total_connected_power > 0 and current_power < 1.0:
        if low_off_timer is None:
            low_off_timer = now
        elif now - low_off_timer > 5:
            print("🍃 Low-power devices appear OFF -> clearing local registry")
            connected_devices.clear()
            print_connected_devices()
            low_off_timer = None
    else:
        low_off_timer = None

    # Update active_events buffers and attempt finalization
    for ev in list(active_events):
        # record transient (first few seconds)
        if (now - ev["start_time"]) <= TRANSIENT_CAPTURE_WINDOW:
            ev["transient_buffer"].append({
                "timestamp": reading["timestamp"],
                "powerWatt": current_power,
                "powerFactor": reading["powerFactor"],
                "voltageVolt": reading["voltageVolt"],
                "currentAmp": reading["currentAmp"]
            })

        # always append to steady buffer
        ev["buffer"].append({
            "timestamp": reading["timestamp"],
            "powerWatt": current_power,
            "powerFactor": reading["powerFactor"],
            "voltageVolt": reading["voltageVolt"],
            "currentAmp": reading["currentAmp"]
        })

        # finalize after stabilization window
        if not ev["finalized"] and (now - ev["start_time"]) >= APPLIANCE_STABILIZATION_WINDOW:
            # compute stable average and variation
            stable_avg = sum(x["powerWatt"] for x in ev["buffer"]) / max(len(ev["buffer"]),1)
            variation = max(x["powerWatt"] for x in ev["buffer"]) - min(x["powerWatt"] for x in ev["buffer"])
            if variation > STABILIZATION_VARIATION_WATT:
                print("⚠️ Large variation (heavy load). Forcing finalize with tolerance.")
                # keep going but accept higher variation
                variation = STABILIZATION_VARIATION_WATT / 2.0

            # compute slope to detect drifting loads
            slope = compute_slope(ev["buffer"])

            print("🔍 Finalize check — avg={:.2f}W var={:.2f}W slope={:.2f}W/s".format(stable_avg, variation, slope))

            # Finalization behavior
            if ev["event_type"] == "ON":
                pre = ev.get("pre_total", last_power_reading)
                residual = stable_avg - pre
                rel = (residual / pre) if pre > 0 else 0.0
                if -1.0 < residual < 0.0:
                    residual = 0.0

                if residual >= MIN_RESIDUAL_WATT or (pre > 0 and residual >= pre * MIN_RESIDUAL_RELATIVE):
                    # Build signature message similar to NILM firmware
                    sig_point = {
                        "timestamp": ev["buffer"][-1]["timestamp"],
                        "voltageVolt": ev["buffer"][-1]["voltageVolt"],
                        "currentAmp": ev["buffer"][-1]["currentAmp"],
                        "powerWatt": round(residual,1),
                        "powerFactor": ev["buffer"][-1]["powerFactor"]
                    }

                    signature_msg = {
                        "dataType": "ApplianceSignature",
                        "deviceId": device_id,
                        "event_type": "ON",
                        "signature_data": [sig_point],
                        "transient_data": ev["transient_buffer"],
                        "steady_state_data": ev["buffer"],
                        "timestamp": now,
                        "features": {
                            "steady_avg_power": round(residual,1),
                            "powerFactor": sig_point["powerFactor"]
                        }
                    }

                    print("📡 Publishing stabilized signature (residual {:.1f}W) & waiting for cloud match...".format(residual))

                    match = publish_signature_and_wait_for_match(signature_msg, wait_seconds=6)
                    if match and match.get("applianceID"):
                        aid = match["applianceID"]
                        aname = match.get("applianceName", "Unknown")
                        # register locally
                        connected_devices[aid] = {
                            "name": aname,
                            "powerWatt": signature_msg["features"]["steady_avg_power"],
                            "powerFactor": signature_msg["features"]["powerFactor"],
                            "last_seen": now
                        }
                        print("🔌 Device connected (matched):", aname, "(", aid, ")")
                        print_connected_devices()
                        # publish confirmed connect event
                        publish_connected_event({
                            "applianceID": aid,
                            "name": aname,
                            "powerWatt": signature_msg["features"]["steady_avg_power"],
                            "powerFactor": signature_msg["features"]["powerFactor"]
                        }, action="connect")
                    else:
                        # cloud didn't respond with match -> still send connect request WITHOUT ID
                        print("⚠️ No matching appliance returned by cloud. Sending connect request for cloud to create record.")
                        # allow cloud to create an appliance (cloud should reply with created applianceID on TOPIC_MATCH later)
                        publish_connected_event({
                            "applianceID": None,
                            "name": "Unknown",
                            "powerWatt": signature_msg["features"]["steady_avg_power"],
                            "powerFactor": signature_msg["features"]["powerFactor"]
                        }, action="connect")
                else:
                    print("⚠️ Residual {:.2f}W too small → not creating signature.".format(residual))

            elif ev["event_type"] == "OFF":
                pre = ev.get("pre_total", ev["buffer"][0]["powerWatt"] if ev["buffer"] else last_power_reading)
                drop = pre - stable_avg
                if drop > 0:
                    matched = find_best_match_for_drop(drop)
                    if matched:
                        info = connected_devices.pop(matched, None)
                        if info:
                            print("❌ Device disconnected (local match):", info.get("name","Unknown"), "(", matched, ")")
                            # notify cloud (disconnect)
                            publish_connected_event({
                                "applianceID": matched,
                                "name": info.get("name","Unknown"),
                                "powerWatt": info.get("powerWatt",0),
                                "powerFactor": info.get("powerFactor",0)
                            }, action="disconnect")
                            print_connected_devices()
                    else:
                        print("⚠️ OFF drop {:.1f}W didn't match any tracked device.".format(drop))
                else:
                    print("⚠️ OFF event detected but drop {:.1f}W not positive.".format(drop if 'drop' in locals() else 0.0))

            else:
                print("⚠️ Unknown event type:", ev["event_type"])

            ev["finalized"] = True
            try:
                active_events.remove(ev)
            except ValueError:
                pass

    last_power_reading = current_power

# -------------------------------------------------------------------------
# WiFi connect
# -------------------------------------------------------------------------
def wifi_connect():
    sta = network.WLAN(network.STA_IF)
    print("Connecting to Wi-Fi...")
    sta.active(True)
    sta.connect(WIFI_SSID, WIFI_PASSWORD)
    timeout = 0
    while not sta.isconnected() and timeout < 30:
        print(".", end=""); time.sleep(1); timeout += 1
    if sta.isconnected():
        print("\n✅ Wi-Fi connected:", sta.ifconfig())
        # try to sync time
        try:
            import ntptime
            ntptime.settime()
            print("✅ NTP synced")
        except Exception as e:
            print("⚠️ NTP sync failed:", e)
        return True
    else:
        print("\n❌ Wi-Fi connection failed.")
        return False

# -------------------------------------------------------------------------
# Main loop
# -------------------------------------------------------------------------
def main():
    global mqtt_connected, mqtt_client
    print("EnerGreen merged firmware starting...")
    # connect wifi
    wifi_ok = wifi_connect()
    if not wifi_ok:
        print("Continuing without Wi-Fi; will retry in main loop.")

    # connect MQTT
    if wifi_ok:
        mqtt_connect()

    # ensure MQTT client is set up and subscribed
    if mqtt_client is None or not mqtt_connected:
        mqtt_connect()

    last_regular = 0

    while True:
        try:
            reading = simulate_pzem_data() if USE_SIMULATED_DATA else pzem_data()
            if reading:
                # print short reading for debug
                print("[Reading] {v:.1f} V | {c:.3f} A | {p:.1f} W | PF {pf:.2f}".format(
                    v=reading["voltageVolt"], c=reading["currentAmp"],
                    p=reading["powerWatt"], pf=reading["powerFactor"]))
                detect_appliance_event(reading)

                # periodically publish regular reading
                now = utime.time()
                if now - last_regular >= REGULAR_READING_INTERVAL_SECONDS:
                    payload = reading.copy()
                    payload["dataType"] = "RegularReading"
                    mqtt_publish(TOPIC_REGULAR, payload)
                    last_regular = now
            else:
                print("No reading")

            # periodically check for incoming MQTT messages (non-blocking)
            try:
                if mqtt_client is not None:
                    mqtt_client.check_msg()
            except Exception as e:
                # reconnect if necessary
                print("⚠️ check_msg error:", e)
                mqtt_connect()

            time.sleep(1)

        except Exception as e:
            print("Loop error:", e)
            # try reconnects
            try:
                if not mqtt_connected:
                    mqtt_connect()
            except Exception:
                pass
            time.sleep(5)

# -------------------------------------------------------------------------
if __name__ == "__main__":
    main()

