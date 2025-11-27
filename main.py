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
USE_SIMULATED_DATA = False # Flag: set to True for testing without physical PZEM hardware
POWER_CHANGE_THRESHOLD_WATT = 1.2    # Threshold (in Watts) for detecting a sudden start or stop event (NILM sensitivity)
POWER_MIN_VALID_WATT = 0.5            # Minimum absolute power (in Watts) required for a change to be considered a real appliance event (filters noise/standby power)
POWER_CHANGE_DEBOUNCE_SECONDS = 3     # Duration (in seconds) to wait after detecting a change, ensuring the power has stabilized before confirming the event
REGULAR_READING_INTERVAL_SECONDS = 60 # How often (in seconds) to send a full set of meter readings to the cloud

APPLIANCE_STABILIZATION_WINDOW = 8    # Time (in seconds) to wait after an ON event for the appliance to reach a stable state (e.g., an AC compressor stabilizing)
STABILIZATION_VARIATION_WATT = 60.0   # Maximum allowed power fluctuation (in Watts) during the stabilization window for heavy loads
TRANSIENT_CAPTURE_WINDOW = 5          # Number of samples captured at the start of an event to analyze the 'inrush current' or transient signature
MIN_RESIDUAL_WATT = 0.5               # Minimum residual power change (in Watts) required to classify a confirmed ON event
MIN_RESIDUAL_RELATIVE = 0.02          # Minimum residual power change required relative to the new total power

# ------------------------------------------------------------------------------
# Cloud configuration
# ------------------------------------------------------------------------------
try:
    # Attempts to import sensitive cloud endpoint URLs and Wi-Fi credentials from a separate file (config.py)
    from config import (
        CLOUD_FUNCTION_URL_READING,       # Endpoint for sending periodic power readings
        CLOUD_FUNCTION_URL_SIGNATURE,     # Endpoint for sending detected appliance signatures (ON events)
        CLOUD_FUNCTION_URL_CONNECTEDDEVICES, # Endpoint for notifying the cloud about connected/disconnected devices
        WIFI_SSID,
        WIFI_PASSWORD
    )
except ImportError:
    print("⚠️ config.py missing; using defaults.")
    # Default values for error handling if config.py is missing
    CLOUD_FUNCTION_URL_READING = "http://default_reading_url"
    CLOUD_FUNCTION_URL_SIGNATURE = "http://default_signature_url"
    WIFI_SSID, WIFI_PASSWORD = "ssid", "password"

# ------------------------------------------------------------------------------
# Globals (State Management)
# ------------------------------------------------------------------------------
device_id = "energreen_esp32" # Unique identifier for the hardware unit
last_power_reading = 0.0      # Baseline power from the previous cycle, used to calculate the power delta (change)
last_regular_reading_time = 0 # Timestamp of the last time a full periodic reading was sent

debounce_active = False       # Flag: True when a power change is detected and the system is waiting for stability
debounce_start_time = 0       # Timestamp when the debounce period started
debounce_event_type = None    # Stores "ON" or "OFF" during the debounce period

active_events = []          # List of events (ON/OFF) that have been confirmed and are currently being processed or stabilized
connected_devices = {}      # Dictionary storing confirmed active devices (key: applianceID, value: power/metadata)

power_window = []             # Buffer for the sliding average calculation
POWER_WINDOW_SIZE = 8         # Number of readings used for the sliding average (smoothing)
low_on_timer = None           # Timer to track low-power ON events (used to debounce small loads)
low_off_timer = None          # Timer to track low-power OFF events

# UART for PZEM (Hardware setup for communicating with the PZEM-004T)
pzem_uart = UART(2, baudrate=9600, tx=17, rx=16, timeout=1000)

# ------------------------------------------------------------------------------
# Utility Functions
# ------------------------------------------------------------------------------
def smoothed_power(new_reading):
    """Calculates a moving average of recent power readings to filter out measurement noise and spikes."""
    power_window.append(new_reading)
    if len(power_window) > POWER_WINDOW_SIZE:
        power_window.pop(0) # Remove the oldest reading
    return sum(power_window) / len(power_window)

def modbus_crc16(data):
    """Calculates the CRC-16 checksum required for the Modbus RTU communication protocol used by the PZEM-004T."""
    crc = 0xFFFF
    for b in data:
        crc ^= b
        for _ in range(8):
            if crc & 1:
                crc = (crc >> 1) ^ 0xA001
            else:
                crc >>= 1
    # Returns the 16-bit CRC as two separate bytes
    return bytes([crc & 0xFF, (crc >> 8) & 0xFF])

def sync_time_with_ntp(max_retries=5):
    """Connects to multiple Network Time Protocol (NTP) servers to synchronize the ESP32's internal clock."""
    import ntptime, time
    servers = ['time.google.com', 'time.cloudflare.com', 'pool.ntp.org']
    for i in range(max_retries):
        for host in servers:
            try:
                ntptime.host = host
                ntptime.settime()
                print("✅ NTP synced via", host)
                return True
            except Exception as e:
                print(f"⚠️ Attempt {i+1} failed with {host}: {e}")
                time.sleep(2)
    print("❌ All NTP attempts failed.")
    return False

def print_connected_devices():
    """Prints a formatted list of currently known active appliances for debugging and monitoring."""
    if connected_devices:
        print("➡️ Connected devices:",
              [f"{v['name']} ({k}) - {v['powerWatt']}W" for k, v in connected_devices.items()])
    else:
        print("➡️ No devices connected.")

# ------------------------------------------------------------------------------
# PZEM reading
# ------------------------------------------------------------------------------
def pzem_data():
    """Sends a Modbus command and parses the response from the PZEM-004T sensor to get power metrics."""
    try:
        # Clears the UART buffer of any residual data
        _ = pzem_uart.read()
        time.sleep_ms(50)
        # Constructs the Modbus command to read input registers (voltage, current, power, etc.)
        cmd = bytes([0x01, 0x04, 0x00, 0x00, 0x00, 0x0A])
        cmd += modbus_crc16(cmd)
        pzem_uart.write(cmd)
        time.sleep_ms(400) # Wait for the PZEM to respond
        res = pzem_uart.read()
        if not res or len(res) < 21: return None # Basic check for minimum response length

        data = res[3:23]
        def u32(payload, offset):
            """Helper function to combine 16-bit registers into 32-bit values (PZEM format)."""
            low = int.from_bytes(payload[offset:offset+2],'big')
            high = int.from_bytes(payload[offset+2:offset+4],'big')
            return (high<<16)|low
        
        # Extracts and scales the raw data registers
        voltage = int.from_bytes(data[0:2],'big')/10
        current = u32(data,2)/1000
        power = u32(data,6)/10
        pf = int.from_bytes(data[16:18],'big')/100
        energy = u32(data,10)/1000
        
        # Returns the cleaned data dictionary
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
    """Sends the captured appliance power signature data to the cloud for NILM analysis and device identification."""
    try:
        print("📡 Sending signature to cloud...")
        # Uses urequests to send an HTTP POST request to the CLOUD_FUNCTION_URL_SIGNATURE endpoint
        resp = urequests.post(CLOUD_FUNCTION_URL_SIGNATURE,
                              data=json.dumps(sig),
                              headers={"Content-Type": "application/json"})
        raw = resp.text
        print("☁️ Cloud response:", raw)
        try:
            data = json.loads(raw)
        except Exception:
            data = {}
        resp.close() # Close the connection to free up resources
        return data # Returns the cloud's response (which usually contains the identified appliance ID and Name)
    except Exception as e:
        print("❌ Cloud send error:", e)
        return {}

# ------------------------------------------------------------------------------
# Event detection (Core NILM Logic)
# ------------------------------------------------------------------------------
def find_best_match_for_drop(drop_watt):
    """
    Identifies which currently 'connected_device' likely turned OFF by matching the observed power drop.
    This is critical for the multi-appliance logic.
    """
    if not connected_devices:
        return None
    best_id, best_diff = None, 1e9
    # Iterates through all currently known active devices
    for aid, info in connected_devices.items():
        # Calculates the difference between the device's known power and the observed power drop
        diff = abs(info.get("powerWatt",0)-drop_watt)
        if diff < best_diff:
            best_diff, best_id = diff, aid
    # Checks if the best match is within an acceptable margin of error (10W or 40% of the device's known power)
    if best_id and best_diff <= max(10,0.4*connected_devices[best_id]['powerWatt']):
        return best_id # Returns the unique ID of the device that turned OFF
    return None

def detect_appliance_event(reading):
    """Main NILM function: Manages the lifecycle of appliance events (detection, debounce, stabilization, identification)."""
    global last_power_reading, debounce_active, debounce_start_time, debounce_event_type
    global active_events, connected_devices, low_on_timer, low_off_timer

    current_power = smoothed_power(reading["powerWatt"])
    now = utime.time()
    delta = current_power - last_power_reading # The instantaneous change in power

    # --- Candidate event detection ---
    # Triggers if the power delta is large enough AND no other event is currently being debounced
    if abs(delta) > POWER_CHANGE_THRESHOLD_WATT and not debounce_active:
        debounce_active = True
        debounce_start_time = now
        debounce_event_type = "ON" if delta > 0 else "OFF"
        pre_snapshot = last_power_reading # The power level before the change
        print(f"⚡ Candidate {debounce_event_type} ΔP={delta:.1f}W")

    # --- Debounce complete and event confirmation ---
    if debounce_active and (now - debounce_start_time) >= POWER_CHANGE_DEBOUNCE_SECONDS:
        # Create a new active event record
        event = {
            "event_type": debounce_event_type,
            "start_time": now,
            "pre_total": last_power_reading,
            "stabilization_time": now + APPLIANCE_STABILIZATION_WINDOW,
            "finalized": False
        }
        active_events.append(event)
        print(f"✅ Confirmed {debounce_event_type} pre_total={last_power_reading:.1f}W")
        # Reset debounce state to allow detection of new, simultaneous events
        debounce_active = False
        debounce_event_type = None

    # --- Low-power ON watcher (for very small loads that might be missed by the main threshold) ---
    if last_power_reading < 1.0 and current_power > 2.0:
        if low_on_timer is None:
            low_on_timer = now
        elif now - low_on_timer > 3: # If the low-power state persists for > 3 seconds
            print(f"🌿 Low-power ON detected ({current_power:.1f}W)")
            # Constructs a minimal signature payload for the cloud
            sig = {
                "dataType": "ApplianceSignature",
                "deviceId": device_id,
                "event_type": "ON",
                "signature_data": [{"timestamp": reading["timestamp"], "powerWatt": round(current_power,1), "powerFactor": reading["powerFactor"]}],
                "features": {"steady_avg_power": round(current_power,1), "powerFactor": reading["powerFactor"]}
            }
            res = send_signature_to_cloud(sig)
            # If the cloud successfully identifies the appliance, store it locally
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

    # --- Process active events (Multi-appliance logic) ---
    for ev in list(active_events):
        # 1. Check if the stabilization window is over
        if now >= ev["stabilization_time"] and not ev["finalized"]:
            residual = current_power - ev["pre_total"] # Final power change (residual power of the new appliance)

            if ev["event_type"] == "ON" and residual >= MIN_RESIDUAL_WATT:
                # FINALIZING ON EVENT: Send signature to cloud for matching/learning
                sig = {
                    # ... constructs the signature payload using the residual power ...
                    "dataType": "ApplianceSignature",
                    "deviceId": device_id,
                    "event_type": "ON",
                    "signature_data": [{"timestamp": reading["timestamp"], "powerWatt": round(residual,1), "powerFactor": reading["powerFactor"]}],
                    "features": {"steady_avg_power": round(residual,1), "powerFactor": reading["powerFactor"]}
                }
                res = send_signature_to_cloud(sig)
                if res.get("applianceID"):
                    aid = res["applianceID"]
                    # Store the new active device in the local connected_devices dictionary
                    connected_devices[aid] = {
                        "name": res.get("applianceName","Unknown"),
                        "powerWatt": sig["features"]["steady_avg_power"],
                        "powerFactor": reading["powerFactor"],
                        "last_seen": now,
                        "applianceID": aid
                    }
                    print(f"🔌 Device connected: {connected_devices[aid]['name']} ({aid})")
                    send_connected_device_to_cloud(connected_devices[aid], "connect") # Notify cloud of new connection
                    print_connected_devices()

            elif ev["event_type"] == "OFF":
                # FINALIZING OFF EVENT: Match power drop against connected devices
                drop = ev["pre_total"] - current_power # The total power drop
                if drop > 0:
                    match = find_best_match_for_drop(drop)
                    if match:
                        info = connected_devices.pop(match) # Remove the device from the active list
                        print(f"❌ Device disconnected: {info['name']} ({match})")
                        info["applianceID"] = match
                        send_connected_device_to_cloud(info, "disconnect") # Notify cloud of disconnection
                        print_connected_devices()
            
            ev["finalized"] = True
            active_events.remove(ev) # Remove the event from the list after processing

    last_power_reading = current_power # Update the baseline for the next cycle

# --------------------------------------------------------------------------
# Connected Devices Cloud Sync
# --------------------------------------------------------------------------
def send_connected_device_to_cloud(device_info, action):
    """
    Sends notification to the cloud whenever an appliance turns ON (connect) or OFF (disconnect).
    This updates the cloud's real-time inventory of active appliances.
    """
    try:
        print(f"📤 Sending {action.upper()} event for {device_info.get('name','Unknown')}...")
        payload = {
            "deviceId": device_id,
            "action": action,
            "applianceID": device_info.get("applianceID", None),
            "applianceName": device_info.get("name", "Unknown"),
            "powerWatt": device_info.get("powerWatt", 0),
            "powerFactor": device_info.get("powerFactor", 0),
            "timestamp": utime.time()
        }
        # Uses urequests to send an HTTP POST request to the cloud endpoint
        resp = urequests.post(CLOUD_FUNCTION_URL_CONNECTEDDEVICES,
                              data=json.dumps(payload),
                              headers={"Content-Type": "application/json"})
        print("☁️ ConnectedDevices response:", resp.text)
        resp.close()
    except Exception as e:
        print("❌ Error sending connected device to cloud:", e)


# ------------------------------------------------------------------------------
# Wi-Fi connection
# ------------------------------------------------------------------------------
# Initializes the Wi-Fi station interface
sta = network.WLAN(network.STA_IF)
print("Connecting to Wi-Fi...")
sta.active(True)
sta.connect(WIFI_SSID, WIFI_PASSWORD)
# Wait loop for connection
for _ in range(30):
    if sta.isconnected(): break
    print(".", end=""); time.sleep(1)
if sta.isconnected():
    print("\n✅ Wi-Fi connected:", sta.ifconfig())
    sync_time_with_ntp() # Synchronize time once connected
else:
    print("\n❌ Wi-Fi failed.")


# ------------------------------------------------------------------------------
# Main loop
# ------------------------------------------------------------------------------
print("EnerGreen ready.")
while True:
    try:
        # Read data from PZEM (or simulate if configured)
        reading = pzem_data() if not USE_SIMULATED_DATA else None
        if reading:
            # Print the raw meter reading for live debugging
            print(
                f"[Reading]\n"
                f"[V]{reading['voltageVolt']:.1f} V || [C]{reading['currentAmp']:.3f} A || [P]{reading['powerWatt']:.1f} W || [E]{reading['kwhConsumed']:.3f} kWh || [PF]{reading['powerFactor']:.2f}\n"
                "----------------------------------------------------------------"
            )
            detect_appliance_event(reading) # Run the core NILM logic

            now = utime.time()
            # Check if it's time to send a regular periodic reading
            if now - last_regular_reading_time >= REGULAR_READING_INTERVAL_SECONDS:
                reading["dataType"] = "RegularReading"
                # Send the reading via HTTP POST to the cloud endpoint
                resp = urequests.post(CLOUD_FUNCTION_URL_READING,
                                      data=json.dumps(reading),
                                      headers={"Content-Type": "application/json"})
                print("☁️ Readings response:", resp.text)
                resp.close()
                last_regular_reading_time = now
        else:
            print("No reading.")
        time.sleep(1) # Wait 1 second before the next reading cycle
    except Exception as e:
        print("Loop error:", e)
        time.sleep(5) # Wait longer on error before trying again
