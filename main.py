# main.py - EnerGreen ESP32 Unified Firmware
# Merges High-Res Transient Detection (Code 1) with Multi-Appliance Tracking (Code 2)
# **NOW INCLUDES DEDICATED LOW-POWER WATCHER**
# PROPERTY OF ENERGREEN

import time
import random
import network
import json
import utime
import ntptime
from machine import UART
import gc  # free up memory
from umqtt.simple import MQTTClient
import ssl
import usocket as socket

# --- Configuration ---
USE_SIMULATED_DATA = False
POWER_CHANGE_THRESHOLD_WATT = 1.0   # Threshold for event triggering
POWER_MIN_VALID_WATT = 1.0          # Minimum load to consider valid
POWER_CHANGE_DEBOUNCE_SECONDS = 3   # Time to wait for signal to stabilize
APPLIANCE_EVENT_DURATION_SECONDS = 15 # Duration to capture transient + steady state
REGULAR_READING_INTERVAL_SECONDS = 60


# --- New Matching Tolerance Constant ---
MAX_ABSOLUTE_DIFFERENCE = 1.0 # Max difference (in W) allowed for a positive match
MIN_RELATIVE_TOLERANCE = 0.1  # Min difference (10%) allowed for matching large loads
# --- Low-Power Watcher Configuration ---
LOW_POWER_BASELINE_WATT = 2.0       # If power is below this, system is considered 'low'
LOW_POWER_TRIGGER_WATT = 5.0        # Power jump required to trigger low-power event
LOW_POWER_DEBOUNCE_SECONDS = 3      # Time to confirm low-power change

# --- Config Import ---
try:
    from config import (WIFI_SSID, WIFI_PASSWORD, MQTT_BROKER, MQTT_PORT, 
                        MQTT_USER, MQTT_PASS, MQTT_CLIENT_ID, 
                        TOPIC_REGULAR, TOPIC_SIGNATURE, TOPIC_DEVICES)
except ImportError:
    print("Error: config.py not found. Using defaults.")
    WIFI_SSID = 'default_ssid'
    WIFI_PASSWORD = 'default_password'
    TOPIC_REGULAR = b"energreen/readings"
    TOPIC_SIGNATURE = b"energreen/signatures"
    TOPIC_DEVICES = b"energreen/devices" 

# --- Global Variables for State Management ---
last_power_reading = 0.0
last_regular_reading_time = 0
debounce_active = False
debounce_start_time = 0
device_id = "energreen_esp32_002"

# Buffers and cumulative tracking
debounce_buffer = []
cumulative_delta = 0.0

# Multi-label support & Device Tracking
active_events = []
debounce_event_type = None
connected_devices = {} # Dictionary to store currently running appliances: {id: {info}}

# **NEW Low-Power Watcher Globals**
low_on_timer = None 

# Sliding average buffer
power_window = []
POWER_WINDOW_SIZE = 8 

# --- MQTT Client Setup ---
mqtt_client = None
mqtt_connected = False

def mqtt_connect():
    global mqtt_client, mqtt_connected
    try:
        # NOTE: Removed manual socket creation and wrapping.
        # umqtt.simple will handle this internally because ssl=True is set.
        
        mqtt_client = MQTTClient(
            client_id=MQTT_CLIENT_ID,
            server=MQTT_BROKER,
            port=MQTT_PORT,
            user=MQTT_USER,
            password=MQTT_PASS,
            ssl=True,  # This tells the library to use SSL/TLS
            ssl_params={"server_hostname": MQTT_BROKER}
        )

        mqtt_client.connect()
        print("✅ Connected securely to MQTT broker")
        mqtt_connected = True
        return True
    except Exception as e:
        print("❌ Failed to connect to MQTT broker:", e)
        mqtt_connected = False
        return False

# In your mqtt_publish function:
def mqtt_publish(topic, payload):
    global mqtt_client, mqtt_connected
    if not mqtt_connected:
        if not mqtt_connect():
            return False
    try:
        # Check if the socket is alive before publishing
        mqtt_client.ping() 
        mqtt_client.publish(topic, json.dumps(payload))
        print(f"Sent to {topic}: {json.dumps(payload)}")
        return True
    except Exception as e:
        print("Failed to publish, attempting reconnect:", e)
        mqtt_connected = False  # Mark as disconnected
        try:
            if mqtt_connect():
                mqtt_client.publish(topic, json.dumps(payload))
                return True
        except Exception as e_retry:
            print("Retry failed:", e_retry)
            return False

def smoothed_power(new_reading):
    """Maintain a sliding average of power to reduce fluctuations."""
    global power_window
    power_window.append(new_reading)
    if len(power_window) > POWER_WINDOW_SIZE:
        power_window.pop(0)
    return sum(power_window) / len(power_window)

def find_best_match_for_drop(drop_watt):
    """
    (From Code 2) Find the connected device whose power roughly matches the observed drop.
    Returns the device ID if found, else None.
    """
    if not connected_devices:
        return None
    
    best_id, best_diff = None, 1e9
    
    # Iterate through connected devices to find the closest wattage match
    for aid, info in connected_devices.items():
        stored_watt = info.get("powerWatt", 0)
        diff = abs(stored_watt - drop_watt)
        
        # Tolerance logic: Allow difference of 10W or 40% of the device's power (whichever is larger)
        tolerance = max(MAX_ABSOLUTE_DIFFERENCE, MIN_RELATIVE_TOLERANCE * stored_watt)
        
        # Update best match only if the difference is within tolerance AND is the closest match so far.
        if diff <= tolerance and diff < best_diff:
            best_diff = diff
            best_id = aid
            
    return best_id

# --- PZEM-004T V3.0 Setup ---
pzem_uart = UART(2, baudrate=9600, tx=17, rx=16, timeout=500)

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

def sync_time_with_ntp(max_retries=5):
    print("Attempting to sync time via NTP...")
    ntptime.host = 'pool.ntp.org'
    for i in range(max_retries):
        try:
            ntptime.settime()
            print("Time synced:", utime.localtime())
            return True
        except Exception:
            time.sleep(1)
    return False

def pzem_data():
    try:
        # Clear buffer
        while pzem_uart.any(): pzem_uart.read()
        
        base_cmd = bytes([0x01, 0x04, 0x00, 0x00, 0x00, 0x0A])
        cmd = base_cmd + modbus_crc16(base_cmd)
        pzem_uart.write(cmd)
        time.sleep_ms(50)
        
        response = pzem_uart.read()
        if not response or len(response) < 25: return None
        
        data = response[3:23]
        
        def u32(payload, offset):
            return (int.from_bytes(payload[offset+2:offset+4], 'big') << 16) | \
                   int.from_bytes(payload[offset:offset+2], 'big')

        voltage = int.from_bytes(data[0:2], 'big') / 10.0
        current = u32(data, 2) / 1000.0
        power   = u32(data, 6) / 10.0
        energy  = u32(data, 10) / 1000.0
        freq    = int.from_bytes(data[14:16], 'big') / 10.0
        pf      = int.from_bytes(data[16:18], 'big') / 100.0

        return {
            "deviceId": device_id,
            "timestamp": utime.time(),
            "voltageVolt": round(voltage, 1),
            "currentAmp": round(current, 3),
            "powerWatt": round(power, 1),
            "kwhConsumed": round(energy, 3),
            "frequencyHz": round(freq, 1),
            "powerFactor": round(pf, 2),
            "energySource": "Grid"
        }
    except Exception as e:
        print("PZEM Read Error:", e)
        return None

def simulate_pzem_data():
    # Simplified simulation for testing
    return {
        "deviceId": device_id,
        "voltageVolt": 220.0,
        "currentAmp": 0.5,
        "powerWatt": 100.0 + random.uniform(-2, 2),
        "kwhConsumed": 10.0,
        "frequencyHz": 60.0,
        "powerFactor": 0.9,
        "energySource": "Grid",
        "timestamp": utime.time()
    }

# --------------------------------------------------------------------------------
# CORE LOGIC: Appliance Event Detection & Tracking (Modified to include Low-Power Watcher)
# --------------------------------------------------------------------------------
def detect_appliance_event(reading):
    global last_power_reading, debounce_active, debounce_start_time
    global active_events, debounce_event_type, debounce_buffer, cumulative_delta
    global connected_devices, low_on_timer 

    current_power = smoothed_power(reading["powerWatt"])
    now = utime.time()

    if last_power_reading == 0.0: last_power_reading = current_power

    # Calculate Delta for Main Detection (Dual Trigger)
    step_delta = current_power - last_power_reading
    cumulative_delta += step_delta
    
    # 1. Main Detection - Check for Step or Gradual Change
    trigger_now = False
    event_delta = 0.0

    if (abs(step_delta) >= POWER_CHANGE_THRESHOLD_WATT) and (not debounce_active):
        trigger_now = True
        event_delta = step_delta
    elif (abs(cumulative_delta) >= POWER_CHANGE_THRESHOLD_WATT) and (not debounce_active):
        trigger_now = True
        event_delta = cumulative_delta

    if trigger_now:
        debounce_active = True
        debounce_start_time = now
        debounce_event_type = "ON" if event_delta > 0 else "OFF"
        debounce_buffer = [current_power]
        print(f"⚡ Candidate {debounce_event_type} detected (main logic)...")

    elif debounce_active:
        debounce_buffer.append(current_power)

    # 2. **DEDICATED LOW-POWER WATCHER (NEW BLOCK)**
    # This watches for a small, stable ON event from a very low baseline.
    if current_power > LOW_POWER_TRIGGER_WATT and last_power_reading < LOW_POWER_BASELINE_WATT and not debounce_active:
        if low_on_timer is None:
            low_on_timer = now
        elif (now - low_on_timer) >= LOW_POWER_DEBOUNCE_SECONDS:
            # Low-power event confirmed, process it immediately as a signature event
            approx_load_watt = current_power - last_power_reading
            print(f"🌿 Low-Power ON confirmed ({approx_load_watt:.1f}W). Processing signature...")
            
            # Create a simplified event structure for immediate processing
            active_events.append({
                "event_type": "ON",
                "start_time": now,
                "buffer": [{ # Simplified buffer with current reading
                    "timestamp": reading["timestamp"],
                    "powerWatt": current_power,
                    "powerFactor": reading["powerFactor"],
                    "voltageVolt": reading["voltageVolt"],
                    "currentAmp": reading["currentAmp"]
                }],
                "delta_power": approx_load_watt,
                "low_power_event": True # Flag to skip main debounce checks
            })
            low_on_timer = None
    else:
        low_on_timer = None # Reset if condition is broken
        
    # 3. Confirm Main Event (Debounce and Stability Check)
    if debounce_active and (now - debounce_start_time) >= POWER_CHANGE_DEBOUNCE_SECONDS:
        avg_power = sum(debounce_buffer) / len(debounce_buffer)
        stability = max(debounce_buffer) - min(debounce_buffer)
        STABILITY_THRESHOLD_W = 1.5

        if stability <= STABILITY_THRESHOLD_W:
            # Determine the actual power change magnitude based on buffer vs previous baseline
            estimated_delta = avg_power - (last_power_reading - cumulative_delta) 
            
            active_events.append({
                "event_type": debounce_event_type,
                "start_time": now,
                "buffer": [], # Start capturing the full signature now
                "delta_power": estimated_delta if abs(estimated_delta) > 0 else event_delta
            })
            print(f"✅ Confirmed {debounce_event_type} event (Delta: {active_events[-1]['delta_power']:.1f}W).")
        else:
            print(f"❌ Event rejected (unstable: {stability:.2f}W).")

        debounce_active = False
        debounce_buffer = []
        cumulative_delta = 0.0

    # 4. Process Active Events (Capture & Finalize)
    for event in list(active_events):
        
        # Capture full signature for long-running events only
        if not event.get("low_power_event", False):
            event["buffer"].append({
                "timestamp": reading["timestamp"],
                "powerWatt": current_power,
                "powerFactor": reading["powerFactor"],
                "voltageVolt": reading["voltageVolt"],
                "currentAmp": reading["currentAmp"]
            })

        # Check if Event Duration is Complete or if it's a Low-Power Event (which processes immediately)
        if (now - event["start_time"]) >= APPLIANCE_EVENT_DURATION_SECONDS or event.get("low_power_event", False):
            
            buf = event["buffer"]
            
            if event.get("low_power_event", False):
                # For low-power, the signature is the instantaneous reading
                transient_data = buf
                steady_data = buf
                approx_load_watt = event["delta_power"]
            else:
                # For main logic, calculate signature from buffer
                transient_data = buf[:3] if len(buf) >= 3 else buf
                steady_data = buf[-3:] if len(buf) >= 3 else buf
                approx_load_watt = abs(event["delta_power"])
            
            # --- A. Send Signature to MQTT ---
            event_signature = {
                "dataType": "ApplianceSignature",
                "deviceId": device_id,
                "event_type": event["event_type"],
                "transient_data": transient_data,
                "steady_state_data": steady_data,
                "delta_watt": event["delta_power"],
                "timestamp": now
            }
            mqtt_publish(TOPIC_SIGNATURE, event_signature)

            # --- B. Update Connected Devices List ---
            if event["event_type"] == "ON":
                appliance_id = str(int(now)) # Use timestamp as a temporary unique ID
                
                new_device = {
                    "applianceID": appliance_id,
                    "name": f"Device_{appliance_id}",
                    "powerWatt": approx_load_watt,
                    "powerFactor": reading["powerFactor"],
                    "on_timestamp": now
                }
                
                connected_devices[appliance_id] = new_device
                print(f" >>> DEVICE CONNECTED: {new_device['name']} ({new_device['powerWatt']:.1f}W)")
                mqtt_publish(TOPIC_DEVICES, {"action": "connect", "device": new_device})

            elif event["event_type"] == "OFF":
                drop_watt = abs(event["delta_power"])
                matched_id = find_best_match_for_drop(drop_watt)
                
                if matched_id:
                    removed_device = connected_devices.pop(matched_id)
                    print(f" <<< DEVICE DISCONNECTED: {removed_device['name']} (Expected ~{drop_watt:.1f}W)")
                    mqtt_publish(TOPIC_DEVICES, {"action": "disconnect", "device": removed_device})
                else:
                    print(f" <<< UNKNOWN DEVICE DISCONNECTED (Drop: {drop_watt:.1f}W)")

            # Remove processed event
            active_events.remove(event)

    # Update baseline
    last_power_reading = current_power

# --- Initialization (omitted for brevity) ---
# ... (Wi-Fi and MQTT setup remain the same) ...
sta_if = network.WLAN(network.STA_IF)
sta_if.active(True)
sta_if.connect(WIFI_SSID, WIFI_PASSWORD)

print("Connecting to Wi-Fi...")
timeout = 0
while not sta_if.isconnected() and timeout < 30:
    time.sleep(1)
    timeout += 1

if sta_if.isconnected():
    print("Wi-Fi Connected!", sta_if.ifconfig())
    sync_time_with_ntp()
    mqtt_connect()
else:
    print("Wi-Fi Failed.")

# --- Main Loop (remains the same) ---
# ... (while True: loop remains the same) ...
while True:
    try:
        reading = simulate_pzem_data() if USE_SIMULATED_DATA else pzem_data()

        if reading:
            detect_appliance_event(reading)

            # Periodic Regular Reading
            now = utime.time()
            if now - last_regular_reading_time >= REGULAR_READING_INTERVAL_SECONDS:
                reading["dataType"] = "RegularReading"
                reading["connected_count"] = len(connected_devices)
                mqtt_publish(TOPIC_REGULAR, reading)
                last_regular_reading_time = now
                
                print("--- Active Devices ---")
                for uid, dev in connected_devices.items():
                    print(f"ID: {uid} | Power: {dev['powerWatt']:.1f}W")
                print("----------------------")

        utime.sleep(1)

    except Exception as e:
        print("Loop Error:", e)
        try:
            mqtt_connect()
        except:
            pass
        utime.sleep(5)
