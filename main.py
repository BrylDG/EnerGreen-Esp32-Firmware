# main.py - EnerGreen merged firmware (MQTT + NILM + Stabilized signature flow)
# NOTE: Expects config.py to define MQTT_* constants and TOPIC_* topics.

import time
import json
import utime
import ntptime
import gc
import random
import network
import ssl
import usocket as socket
from machine import UART
from umqtt.simple import MQTTClient

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
        TOPIC_CONNECTED
    )
except ImportError:
    print("Error: config.py missing or incomplete — using placeholders.")
    WIFI_SSID = "ssid"
    WIFI_PASSWORD = "pass"
    MQTT_BROKER = "broker"
    MQTT_PORT = 8883
    MQTT_USER = "user"
    MQTT_PASS = "pass"
    MQTT_CLIENT_ID = "energreen_esp"
    TOPIC_REGULAR = "energreen/reading"
    TOPIC_SIGNATURE = "energreen/signature"
    TOPIC_CONNECTED = "energreen/connected"

# Response topic pattern (cloud should publish to this topic with correlation)
TOPIC_SIGNATURE_RESPONSE = TOPIC_SIGNATURE + "/response/" + MQTT_USER

# -------------------------------------------------------------------------
# Tuning parameters (NILM & timing)
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

# smoothing
POWER_WINDOW_SIZE = 8

# -------------------------------------------------------------------------
# Globals (State Management)
# -------------------------------------------------------------------------
device_id = MQTT_USER
last_power_reading = 0.0
last_regular_reading_time = 0

# Debounce & multi-event buffers
debounce_active = False
debounce_start_time = 0
debounce_event_type = None
debounce_buffer = []
cumulative_delta = 0.0

active_events = []          # List of events currently being captured or stabilized (ON/OFF)
connected_devices = {}      # Local cache of appliances known to be ON {applianceID: info}

# smoothing buffer
power_window = []

# low-watt timers (global scope)
low_on_timer = None
low_off_timer = None

# MQTT client state
mqtt_client = None
mqtt_connected = False

# Holds pending signature results keyed by signature_id
pending_signature_results = {}

# UART to PZEM
pzem_uart = UART(2, baudrate=9600, tx=17, rx=16, timeout=1000)

# -------------------------------------------------------------------------
# I. Utility Functions
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

def sync_time_with_ntp(max_retries=5, timeout=3):
    ntptime.host = "pool.ntp.org"
    for i in range(max_retries):
        try:
            ntptime.settime()
            print("NTP synced:", utime.localtime())
            return True
        except Exception as e:
            print("⚠️ Attempt", i+1, "failed with", e)
            time.sleep(timeout)
    print("❌ All NTP attempts failed.")
    return False

def print_connected_devices():
    if connected_devices:
        print("➡️ Connected devices:", [f"{v['name']} ({k}) - {v['powerWatt']}W" for k,v in connected_devices.items()])
    else:
        print("➡️ No devices connected.")

# -------------------------------------------------------------------------
# II. Data Acquisition (PZEM)
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

        # Basic range validation
        if not (0.0 <= voltage <= 300.0): return None
        if not (0.0 <= current <= 200.0): return None
        if not (-10000.0 <= power <= 10000.0): return None

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
# III. MQTT Helpers (Secure Communication)
# -------------------------------------------------------------------------
def mqtt_callback(topic, msg):
    """
    Expected payload from cloud:
    {
      "signature_id": "...",
      "result": "match"|"new",
      "applianceID": "...",
      "applianceName": "...",
      "powerWatt": 123.4
    }
    """
    global pending_signature_results
    try:
        data = json.loads(msg)
        sig_id = data.get("signature_id") or data.get("signatureId") or data.get("sig_id")
        if sig_id:
            pending_signature_results[sig_id] = data
            print("☁️ Received signature response for", sig_id, "->", data.get("result"))
        else:
            # If cloud sends appliance updates without signature_id, handle them too (best-effort)
            aid = data.get("applianceID") or data.get("applianceId")
            if aid:
                connected_devices[aid] = {
                    "name": data.get("applianceName", "Unknown"),
                    "powerWatt": data.get("powerWatt", 0),
                    "powerFactor": data.get("powerFactor", 0),
                    "last_seen": utime.time()
                }
                print("☁️ Cloud update: registered appliance", aid)
    except Exception as e:
        print("⚠️ mqtt_callback parse error:", e)

def mqtt_connect():
    """
    Connects and subscribes to response topic so cloud can reply with appliance matches.
    """
    global mqtt_client, mqtt_connected
    try:
        if mqtt_client is None:
            mqtt_client = MQTTClient(client_id=MQTT_CLIENT_ID,
                                     server=MQTT_BROKER,
                                     port=MQTT_PORT,
                                     user=MQTT_USER,
                                     password=MQTT_PASS,
                                     ssl=True,
                                     ssl_params={"server_hostname": MQTT_BROKER})
            mqtt_client.set_callback(mqtt_callback)
        mqtt_client.connect()
        # subscribe to signature response topic (cloud must publish here)
        mqtt_client.subscribe(TOPIC_SIGNATURE_RESPONSE)
        mqtt_connected = True
        print("✅ MQTT connected and subscribed to", TOPIC_SIGNATURE_RESPONSE)
    except Exception as e:
        mqtt_connected = False
        print("❌ MQTT connect/subscribe failed:", e)
    return mqtt_connected

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
# IV. Cloud Synchronization Functions
# -------------------------------------------------------------------------
def send_signature_and_wait_for_match(signature_msg, timeout_seconds=6):
    """
    Publishes signature_msg (must include 'signature_id') to TOPIC_SIGNATURE, then waits
    briefly for the cloud to respond on TOPIC_SIGNATURE_RESPONSE with the same signature_id.

    Returns: dict (cloud response) or None if no response
    """
    global pending_signature_results, mqtt_client, mqtt_connected

    sig_id = signature_msg.get("signature_id")
    if not sig_id:
        # ensure signature_id exists
        sig_id = str(int(utime.time())) + "_" + str(random.getrandbits(16))
        signature_msg["signature_id"] = sig_id

    # Attempt publish
    ok = mqtt_publish(TOPIC_SIGNATURE, signature_msg)
    if not ok:
        return None

    # Wait up to timeout_seconds for pending_signature_results[sig_id] to arrive
    deadline = utime.time() + timeout_seconds
    while utime.time() < deadline:
        # allow incoming messages to be processed
        try:
            mqtt_client.check_msg()
        except Exception:
            # if check_msg errors, try reconnect later
            pass

        if sig_id in pending_signature_results:
            resp = pending_signature_results.pop(sig_id)
            return resp
        # small delay to avoid busy-looping
        time.sleep_ms(200)

    # no response within timeout
    return None

def send_connected_device_to_cloud(device_info, action="connect"):
    """
    Publishes compact connect/disconnect event to TOPIC_CONNECTED.
    """
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
# V. NILM Detection Logic (stabilize signatures before sending)
# -------------------------------------------------------------------------
def find_best_match_for_drop(drop_watt):
    if not connected_devices:
        return None
    best_id, best_diff = None, 1e9
    for aid, info in connected_devices.items():
        diff = abs(info.get("powerWatt", 0.0) - drop_watt)
        if diff < best_diff:
            best_diff, best_id = diff, aid
    # Confirmation: difference < 10W OR < 40% of device power
    if best_id and best_diff <= max(10, 0.4 * connected_devices[best_id].get("powerWatt", 0.0)):
        return best_id
    return None

def detect_appliance_event(reading):
    """
    Primary NILM engine: detect candidate step -> debounce -> capture buffers -> stabilize -> send signature.
    Uses send_signature_and_wait_for_match() to publish stabilized signature and wait for cloud ID.
    """
    global last_power_reading, debounce_active, debounce_start_time, debounce_event_type
    global debounce_buffer, cumulative_delta, active_events, connected_devices
    global low_on_timer, low_off_timer

    current_power = smoothed_power(reading["powerWatt"])
    now = utime.time()

    # initialize baseline
    if last_power_reading == 0.0:
        last_power_reading = current_power

    step_delta = current_power - last_power_reading
    cumulative_delta += step_delta

    # --- Candidate detection triggers ---
    trigger_now = False
    trigger_value = 0.0
    if (abs(step_delta) >= POWER_CHANGE_THRESHOLD_WATT) and (not debounce_active):
        trigger_now = True
        trigger_value = step_delta
        trigger_reason = "step"
    elif (abs(cumulative_delta) >= POWER_CHANGE_THRESHOLD_WATT) and (not debounce_active):
        trigger_now = True
        trigger_value = cumulative_delta
        trigger_reason = "cumulative"
    else:
        trigger_reason = None

    if trigger_now:
        # Start debounce timer
        debounce_active = True
        debounce_start_time = now
        debounce_event_type = "ON" if trigger_value > 0 else "OFF"
        debounce_buffer = [current_power]
        print("⚡ Candidate", debounce_event_type, "reason=", trigger_reason, "val={:.2f}W".format(trigger_value))

    elif debounce_active:
        # Collect samples during debounce
        debounce_buffer.append(current_power)

    # --- Confirm after debounce window ---
    if debounce_active and (now - debounce_start_time) >= POWER_CHANGE_DEBOUNCE_SECONDS:
        # Calculate stability (noise) during the debounce period
        variation = max(debounce_buffer) - min(debounce_buffer)
        STABILITY_THRESHOLD_W = max(1.5, STABILIZATION_VARIATION_WATT / 6.0)

        if variation <= STABILITY_THRESHOLD_W:
            # Event is stable enough; confirm and prepare for signature capture
            event = {
                "event_type": debounce_event_type,
                "start_time": now,
                "pre_total": last_power_reading, # The baseline before the event
                "buffer": [],                   # Will collect steady-state samples
                "transient_buffer": [],         # Will collect transient samples
                "finalized": False,
                "stabilization_deadline": now + APPLIANCE_STABILIZATION_WINDOW
            }
            active_events.append(event)
            print("✅ Confirmed event:", debounce_event_type, "pre_total={:.1f}W".format(last_power_reading))
        else:
            print("⚠️ Debounce rejected: unstable (variation={:.1f}W)".format(variation))

        # Reset debounce state
        debounce_active = False
        debounce_buffer = []
        cumulative_delta = 0.0
        debounce_event_type = None

    # --- Low-power ON watcher (Dedicated path for small loads like a clip fan) ---
    LOW_POWER_ON_THRESHOLD = 2.0
    LOW_POWER_SUSTAIN_SECONDS = 3
    if last_power_reading < 0.8 and current_power >= LOW_POWER_ON_THRESHOLD:
        if low_on_timer is None:
            low_on_timer = now
        elif now - low_on_timer >= LOW_POWER_SUSTAIN_SECONDS:
            print("🌿 Low-power ON detected ({:.1f}W) - sending signature".format(current_power))
            sig_point = {
                "timestamp": reading["timestamp"],
                "powerWatt": round(current_power,1),
                "powerFactor": reading["powerFactor"],
                "voltageVolt": reading["voltageVolt"],
                "currentAmp": reading["currentAmp"]
            }
            signature_msg = {
                "dataType": "ApplianceSignature",
                "deviceId": device_id,
                "event_type": "ON",
                "signature_data": [sig_point],
                "transient_data": [sig_point],
                "steady_state_data": [sig_point],
                "timestamp": now,
                "features": {"steady_avg_power": sig_point["powerWatt"], "powerFactor": sig_point["powerFactor"]}
            }

            # Add a signature_id for correlation and send
            signature_msg["signature_id"] = str(int(now)) + "_" + str(random.getrandbits(16))
            resp = send_signature_and_wait_for_match(signature_msg, timeout_seconds=6)
            if resp:
                # Cloud returned a match/new id
                aid = resp.get("applianceID")
                if aid:
                    connected_devices[aid] = {
                        "name": resp.get("applianceName", "Unknown"),
                        "powerWatt": resp.get("powerWatt", signature_msg["features"]["steady_avg_power"]),
                        "powerFactor": signature_msg["features"]["powerFactor"],
                        "last_seen": now
                    }
                    print("🔌 Low-power device registered:", connected_devices[aid]["name"], "(", aid, ")")
                    send_connected_device_to_cloud({ "applianceID": aid, "name": connected_devices[aid]["name"],
                                                     "powerWatt": connected_devices[aid]["powerWatt"],
                                                     "powerFactor": connected_devices[aid]["powerFactor"] }, "connect")
                else:
                    print("⚠️ Low-power cloud response had no applianceID.")
            else:
                print("⚠️ No cloud response for low-power signature.")
            low_on_timer = None
    else:
        low_on_timer = None

    # --- Low-power OFF watcher ---
    total_connected_power = sum(v.get("powerWatt",0) for v in connected_devices.values())
    if total_connected_power > 0 and current_power < 1.0:
        if low_off_timer is None:
            low_off_timer = now
        elif now - low_off_timer > 5: # If power is low for 5 seconds, assume small loads turned off.
            print("🍃 Low-power devices appear OFF -> clearing local registry")
            connected_devices.clear()
            print_connected_devices()
            low_off_timer = None
    else:
        low_off_timer = None

    # --- Update active_events capturing buffers and finalize events ---
    for ev in list(active_events):
        # 1. Record transient data (only for the first few seconds)
        if (now - ev["start_time"]) <= TRANSIENT_CAPTURE_WINDOW:
            ev["transient_buffer"].append({
                "timestamp": reading["timestamp"], "powerWatt": current_power, "powerFactor": reading["powerFactor"],
                "voltageVolt": reading["voltageVolt"], "currentAmp": reading["currentAmp"]
            })

        # 2. Always record steady state data
        ev["buffer"].append({
            "timestamp": reading["timestamp"], "powerWatt": current_power, "powerFactor": reading["powerFactor"],
            "voltageVolt": reading["voltageVolt"], "currentAmp": reading["currentAmp"]
        })

        # 3. Finalize event after stabilization window
        if not ev["finalized"] and now >= ev.get("stabilization_deadline", 0):
            # compute stable average and variation
            stable_avg = sum(x["powerWatt"] for x in ev["buffer"]) / max(len(ev["buffer"]),1)
            variation = max(x["powerWatt"] for x in ev["buffer"]) - min(x["powerWatt"] for x in ev["buffer"])

            if variation > STABILIZATION_VARIATION_WATT:
                # heavy-load noisy device - allow but note
                print("⚠️ Large variation (heavy load). Variation:", variation)
                # we still attempt to stabilize but signal it to the cloud via features

            if ev["event_type"] == "ON":
                pre = ev.get("pre_total", last_power_reading)
                residual = stable_avg - pre
                rel = (residual / pre) if pre > 0 else 0.0
                if -1.0 < residual < 0.0:
                    residual = 0.0

                if residual >= MIN_RESIDUAL_WATT or (pre > 0 and residual >= pre * MIN_RESIDUAL_RELATIVE):
                    # Build stabilized signature payload
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
                            "powerFactor": ev["buffer"][-1]["powerFactor"],
                            "variation": variation
                        }
                    }
                    # correlate responses with signature_id
                    signature_msg["signature_id"] = str(int(now)) + "_" + str(random.getrandbits(16))

                    print("📡 Stabilized signature ready (residual {:.1f}W). Sending to cloud...".format(sig_point["powerWatt"]))
                    resp = send_signature_and_wait_for_match(signature_msg, timeout_seconds=6)

                    if resp:
                        # Cloud returned match/new id - register locally and send connect event
                        aid = resp.get("applianceID")
                        if aid:
                            connected_devices[aid] = {
                                "name": resp.get("applianceName", "Unknown"),
                                "powerWatt": resp.get("powerWatt", signature_msg["features"]["steady_avg_power"]),
                                "powerFactor": signature_msg["features"]["powerFactor"],
                                "last_seen": now
                            }
                            # ensure applianceID is stored
                            connected_devices[aid]["applianceID"] = aid
                            print("🔌 Device connected:", connected_devices[aid]["name"], "(", aid, ")")
                            # announce to cloud that it's connected (cloud may already have registered it, but keep realtime inventory updated)
                            send_connected_device_to_cloud(connected_devices[aid], "connect")
                            print_connected_devices()
                        else:
                            print("⚠️ Cloud response didn't include applianceID:", resp)
                    else:
                        print("⚠️ No cloud response for stabilized signature (signature_id {}).".format(signature_msg["signature_id"]))
                else:
                    print("⚠️ Residual {:.2f}W too small — no signature created.".format(residual))

            elif ev["event_type"] == "OFF":
                pre = ev.get("pre_total", last_power_reading)
                drop = pre - stable_avg
                if drop > 0:
                    match = find_best_match_for_drop(drop)
                    if match:
                        info = connected_devices.pop(match, None)
                        if info:
                            print("❌ Device disconnected:", info.get("name","Unknown"), "(", match, ")")
                            # send disconnect event to cloud
                            send_connected_device_to_cloud(info, "disconnect")
                            print_connected_devices()
                    else:
                        print("⚠️ OFF event finalized, but no matching device found for drop {:.1f}W.".format(drop))
                else:
                    print("⚠️ OFF event but drop {:.1f}W not positive.".format(drop if 'drop' in locals() else 0.0))
            else:
                print("⚠️ Unknown event_type", ev["event_type"])

            ev["finalized"] = True
            try:
                active_events.remove(ev)
            except ValueError:
                pass

    last_power_reading = current_power

# -------------------------------------------------------------------------
# VI. Initialization and Main Loop
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
        sync_time_with_ntp()
        return True
    else:
        print("\n❌ Wi-Fi connection failed.")
        return False

# --- Main execution block ---
print("EnerGreen merged firmware starting...")
if not wifi_connect():
    print("Continuing without Wi-Fi; will retry in main loop.")

# connect MQTT if possible
if network.WLAN(network.STA_IF).isconnected():
    mqtt_connect()

while True:
    try:
        # 1. Check for incoming MQTT messages
        if mqtt_connected:
            try:
                mqtt_client.check_msg()
            except Exception:
                # ignore check errors; connection health handled later
                pass

        reading = simulate_pzem_data() if USE_SIMULATED_DATA else pzem_data()

        if reading:
            # debug print
            print("[Reading] {v:.1f} V | {c:.3f} A | {p:.1f} W | PF {pf:.2f}".format(
                v=reading["voltageVolt"], c=reading["currentAmp"],
                p=reading["powerWatt"], pf=reading["powerFactor"]))
            detect_appliance_event(reading) # main NILM processing

            # periodically publish regular reading
            now = utime.time()
            if now - last_regular_reading_time >= REGULAR_READING_INTERVAL_SECONDS:
                payload = reading.copy()
                payload["dataType"] = "RegularReading"
                mqtt_publish(TOPIC_REGULAR, payload)
                last_regular_reading_time = now

        else:
            print("No reading")

        time.sleep(1)

    except Exception as e:
        print("Loop error:", e)
        # recover: try reconnects
        try:
            if not network.WLAN(network.STA_IF).isconnected():
                wifi_connect()
            if not mqtt_connected:
                mqtt_connect()
        except Exception:
            pass
        time.sleep(5)

