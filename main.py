# main.py - EnerGreen Autonomous Firmware
# ------------------------------------------------------------------------------
# 1. MQTT Protocol (Outbound only)
# 2. Local State Tracking (Immediate registration of loads)
# 3. Cloud Identification (Passive - Cloud knows, ESP32 tracks Watts only)
# ------------------------------------------------------------------------------

import time, json, utime, network
from machine import UART
from umqtt.simple import MQTTClient
import ssl
import random

# --- Import Configuration ---
try:
    from config import *
except ImportError:
    print("Error: config.py not found.")

# --- System Constants ---
POWER_CHANGE_THRESHOLD_WATT = 1.2
POWER_CHANGE_DEBOUNCE_SECONDS = 3
REGULAR_READING_INTERVAL_SECONDS = 60
APPLIANCE_STABILIZATION_WINDOW = 8
MIN_RESIDUAL_WATT = 0.5

# --- Global State Memory ---
device_id = MQTT_USER
# Dictionary: { "session_id": {name, watts, start_time} }
connected_devices = {} 
active_events = [] 
last_power_reading = 0.0
last_regular_reading_time = 0
debounce_active = False
debounce_start_time = 0
debounce_event_type = None

# Sliding Window for Smoothing
power_window = []
POWER_WINDOW_SIZE = 6

# --- Hardware Setup ---
pzem_uart = UART(2, baudrate=9600, tx=17, rx=16)
mqtt_client = None

# ------------------------------------------------------------------------------
# 1. Signal Processing Functions
# ------------------------------------------------------------------------------

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
            if crc & 1:
                crc = (crc >> 1) ^ 0xA001
            else:
                crc >>= 1
    return bytes([crc & 0xFF, (crc >> 8) & 0xFF])

# ------------------------------------------------------------------------------
# 2. Hardware Driver (PZEM-004T)
# ------------------------------------------------------------------------------

def pzem_data():
    try:
        if pzem_uart.any(): pzem_uart.read()
        
        cmd = bytes([0x01, 0x04, 0x00, 0x00, 0x00, 0x0A])
        cmd += modbus_crc16(cmd)
        
        pzem_uart.write(cmd)
        time.sleep_ms(200)
        
        res = pzem_uart.read()
        if not res or len(res) < 21: return None
        
        def u32(payload, offset):
            low = int.from_bytes(payload[offset:offset+2],'big')
            high = int.from_bytes(payload[offset+2:offset+4],'big')
            return (high<<16)|low
            
        data = res[3:23]
        voltage = int.from_bytes(data[0:2],'big') / 10.0
        current = u32(data, 2) / 1000.0
        power = u32(data, 6) / 10.0
        energy = u32(data, 10) / 1000.0
        pf = int.from_bytes(data[16:18],'big') / 100.0
        
        return {
            "deviceId": device_id,
            "timestamp": utime.time(),
            "voltageVolt": round(voltage, 1),
            "currentAmp": round(current, 3),
            "powerWatt": round(power, 1),
            "kwhConsumed": round(energy, 3),
            "powerFactor": round(pf, 2)
        }
    except Exception as e:
        print("PZEM Read Error:", e)
        return None

# ------------------------------------------------------------------------------
# 3. Network & MQTT Setup (Outbound Only)
# ------------------------------------------------------------------------------

def sync_time_with_ntp(max_retries=5):
    import ntptime
    servers = ['time.google.com', 'time.cloudflare.com', 'pool.ntp.org']
    print("🕒 Syncing Time...")
    for i in range(max_retries):
        for host in servers:
            try:
                ntptime.host = host
                ntptime.settime()
                print(f"✅ NTP synced via {host}")
                return True
            except Exception as e:
                print(f"⚠️ NTP Error ({host}): {e}")
                time.sleep(1)
    print("❌ NTP Failed.")
    return False

def setup_wifi():
    sta = network.WLAN(network.STA_IF)
    sta.active(True)
    sta.connect(WIFI_SSID, WIFI_PASSWORD)
    print("Connecting to WiFi...")
    timeout = 0
    while not sta.isconnected() and timeout < 20:
        time.sleep(1)
        timeout += 1
        print(".", end="")
    if sta.isconnected():
        print("\nWiFi Connected:", sta.ifconfig())
        sync_time_with_ntp()
    else:
        print("\nWiFi Failed.")

def mqtt_connect():
    global mqtt_client
    try:
        mqtt_client = MQTTClient(
            MQTT_CLIENT_ID, 
            MQTT_BROKER, 
            port=MQTT_PORT, 
            user=MQTT_USER, 
            password=MQTT_PASS, 
            keepalive=60,
            ssl=True,
            ssl_params={"server_hostname": MQTT_BROKER} 
        )
        # Note: No set_callback here because we don't listen anymore
        mqtt_client.connect()
        print("✅ MQTT Connected (Transmitter Mode).")
        return True
    except Exception as e:
        print("❌ MQTT Connection Error:", e)
        return False

def mqtt_pub(topic, payload):
    if not mqtt_client: return
    try:
        mqtt_client.publish(topic, json.dumps(payload))
        print(f"📤 Sent to {topic}")
    except Exception as e:
        print("Publish Error:", e)

# ------------------------------------------------------------------------------
# 4. Logic Core: Matching & State Management
# ------------------------------------------------------------------------------

def print_connected_devices():
    if not connected_devices:
        print("State: [Empty]")
    else:
        # We might not know the name, but we know the watts
        print("State: ", [f"{v['name']} ({v['powerWatt']}W)" for k,v in connected_devices.items()])

def find_best_match_for_drop(drop_watt):
    """
    Finds the device in memory that matches the power drop.
    """
    if not connected_devices: return None
    
    best_id = None
    best_diff = 100000.0
    
    for aid, info in connected_devices.items():
        stored_watt = info.get("powerWatt", 0)
        diff = abs(stored_watt - drop_watt)
        
        # Match tolerance: 10W or 20%
        tolerance = max(10, stored_watt * 0.2) 
        
        if diff < tolerance and diff < best_diff:
            best_diff = diff
            best_id = aid
            
    return best_id

def detect_appliance_event(reading):
    global last_power_reading, debounce_active, debounce_start_time, debounce_event_type
    global active_events, connected_devices

    current_power = smoothed_power(reading["powerWatt"])
    now = utime.time()
    delta = current_power - last_power_reading

    # --- A. Candidate Detection ---
    if abs(delta) > POWER_CHANGE_THRESHOLD_WATT and not debounce_active:
        debounce_active = True
        debounce_start_time = now
        debounce_event_type = "ON" if delta > 0 else "OFF"
        pre_event_baseline = last_power_reading 
        print(f"⚡ Candidate {debounce_event_type} ΔP={delta:.1f}W")

    # --- B. Event Confirmation ---
    if debounce_active and (now - debounce_start_time) >= POWER_CHANGE_DEBOUNCE_SECONDS:
        event = {
            "event_type": debounce_event_type,
            "start_time": now,
            "pre_total": last_power_reading,
            "stabilization_time": now + APPLIANCE_STABILIZATION_WINDOW,
            "finalized": False,
            "buffer": [] # Added buffer for data signature
        }
        active_events.append(event)
        debounce_active = False 

    # --- C. Processing ---
    for ev in list(active_events):
        
        # Collect data for signature analysis
        if not ev["finalized"]:
            ev["buffer"].append({
                "timestamp": reading["timestamp"],
                "voltageVolt": reading["voltageVolt"],
                "currentAmp": reading["currentAmp"],
                "powerWatt": current_power,
                "powerFactor": reading["powerFactor"]
            })

        if now >= ev["stabilization_time"] and not ev["finalized"]:
            
            # 1. ON Event: Auto-Register & Publish
            if ev["event_type"] == "ON":
                residual = current_power - ev["pre_total"]
                
                if residual >= MIN_RESIDUAL_WATT:
                    print(f"🚀 Detected ON: {residual:.1f}W")
                    
                    # Generate a unique ID for this specific session
                    # The Cloud will map this ID to "Fan" or "TV" on its end
                    session_id = f"dev_{int(utime.time())}_{random.randint(100,999)}"
                    
                    # Update Memory IMMEDIATELY (Don't wait for cloud)
                    connected_devices[session_id] = {
                        "name": f"Unknown_{residual:.0f}W", # Placeholder name
                        "powerWatt": residual,
                        "last_seen": utime.time()
                    }
                    print_connected_devices()
                    
                    # Prepare Signature Data (Transient + Steady State)
                    buf = ev["buffer"]
                    transient_data = buf[:4] if len(buf) >= 4 else buf
                    steady_data = buf[-4:] if len(buf) >= 4 else buf

                    # Send Signature to Cloud (Compatible with Old Protocol)
                    sig = {
                        "dataType": "ApplianceSignature",
                        "deviceId": device_id,
                        "session_id": session_id,
                        "event_type": "ON",
                        "timestamp": reading["timestamp"],
                        # Include raw arrays for backend compatibility
                        "transient_data": transient_data,
                        "steady_state_data": steady_data,
                        "features": {
                            "steady_avg_power": round(residual, 1),
                            "powerFactor": reading["powerFactor"]
                        }
                    }
                    mqtt_pub(TOPIC_SIGNATURE, sig)
                else:
                    print("Ignored noise.")

            # 2. OFF Event: Local Match & Publish
            elif ev["event_type"] == "OFF":
                drop = ev["pre_total"] - current_power
                
                if drop > 0:
                    print(f"📉 Detected OFF: {drop:.1f}W")
                    
                    match_id = find_best_match_for_drop(drop)
                    
                    if match_id:
                        device = connected_devices.pop(match_id)
                        print(f"✅ Matched to {device['name']}. Removing.")
                        print_connected_devices()
                        
                        # Send Disconnect to Cloud
                        payload = {
                            "dataType": "DeviceUpdate",
                            "action": "disconnect",
                            "session_id": match_id, 
                            "applianceName": device["name"],
                            "timestamp": utime.time()
                        }
                        mqtt_pub(TOPIC_DEVICES, payload)
                    else:
                        print("Unknown device turned off.")

            ev["finalized"] = True
            active_events.remove(ev)

    last_power_reading = current_power

# ------------------------------------------------------------------------------
# 5. Main Loop
# ------------------------------------------------------------------------------

setup_wifi()
mqtt_connect()

print("🚀 System Ready (Autonomous Mode).")

while True:
    try:
        reading = pzem_data()
        
        if reading:
            # Print status to console
            print(f"[Status] {reading['voltageVolt']}V | {reading['currentAmp']}A | {reading['powerWatt']}W | PF: {reading['powerFactor']}")
            
            detect_appliance_event(reading)
            
            now = utime.time()
            if now - last_regular_reading_time >= REGULAR_READING_INTERVAL_SECONDS:
                reading["dataType"] = "RegularReading"
                mqtt_pub(TOPIC_REGULAR, reading) # Changed from TOPIC_READINGS
                last_regular_reading_time = now
        
        time.sleep(0.5)
        
    except KeyboardInterrupt:
        break
    except Exception as e:
        print("Error:", e)
        try:
            mqtt_connect()
        except:
            pass
        time.sleep(5)
