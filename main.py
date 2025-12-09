# main.py - EnerGreen Autonomous Firmware (with SD fallback)
# -------------------------------------------------------------------
# 1. MQTT Protocol (Outbound only)
# 2. Local State Tracking (Immediate registration of loads)
# 3. Cloud Identification (Passive - Cloud knows, ESP32 tracks Watts only)
# 4. When offline, store readings to SD card (SDHC) via SoftSPI
# 5. Status Indicators for Connection Loss/Regain and Storage Events
# -------------------------------------------------------------------

import time, json, utime, network, os
from machine import UART, Pin, SoftSPI
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
RECONNECT_INTERVAL_MS = 10000 # Retry connection every 10 seconds if lost

# --- SD Card Constants & Pins (SoftSPI) ---
SD_CS_PIN = 13
SD_SCK_PIN = 18
SD_MOSI_PIN = 27
SD_MISO_PIN = 19
SD_MOUNT_POINT = "/sd"
SD_QUEUE_FILE = SD_MOUNT_POINT + "/readings_queue.ndjson"

# --- Global State Memory ---
device_id = MQTT_USER
connected_devices = {}
active_events = []
last_power_reading = 0.0
last_regular_reading_time = 0
last_reconnect_attempt = 0 
debounce_active = False
debounce_start_time = 0
debounce_event_type = None
# --- Time sync state ---
rtc_synced = False
boot_time_at_sync = 0     # wall-clock time (seconds since epoch) recorded when NTP succeeded
boot_ticks_at_sync = 0    # time.ticks_ms() recorded at the same moment


# Sliding Window for Smoothing
power_window = []
POWER_WINDOW_SIZE = 6

# SD state
sd = None
sd_mounted = False
cs = None
spi = None

# Connection State Tracking (For status indicators)
was_connected = False

# --- Hardware Setup ---
pzem_uart = UART(2, baudrate=9600, tx=17, rx=16)
mqtt_client = None

# -------------------------------------------------------------------
# Helper: SD card setup (SoftSPI)
# -------------------------------------------------------------------
def setup_sd(max_retries=2):
    global sd, sd_mounted, cs, spi
    try:
        # lazy import - some MicroPython builds require sdcard module present
        import sdcard
    except Exception as e:
        print("⚠️ sdcard module unavailable:", e)
        sd_mounted = False
        return False

    try:
        cs = Pin(SD_CS_PIN, Pin.OUT)
        spi = SoftSPI(baudrate=1_000_000, polarity=0, phase=0,
                      sck=Pin(SD_SCK_PIN), mosi=Pin(SD_MOSI_PIN), miso=Pin(SD_MISO_PIN))
    except Exception as e:
        print("⚠️ SoftSPI init failed:", e)
        sd_mounted = False
        return False

    for attempt in range(max_retries):
        try:
            sd = sdcard.SDCard(spi, cs)
            # mount (use try/except to avoid exception if already mounted)
            try:
                os.mount(sd, SD_MOUNT_POINT)
            except OSError:
                # maybe already mounted; try unmount then mount
                try:
                    os.umount(SD_MOUNT_POINT)
                    os.mount(sd, SD_MOUNT_POINT)
                except Exception:
                    pass
            # ensure queue file exists
            try:
                with open(SD_QUEUE_FILE, "a") as f:
                    pass
            except Exception as e:
                print("⚠️ SD queue file touch failed:", e)
            sd_mounted = True
            print("✅ SD mounted at", SD_MOUNT_POINT)
            return True
        except Exception as e:
            print(f"⚠️ SD mount attempt {attempt+1} failed:", e)
            time.sleep(1)
    sd_mounted = False
    print("❌ SD mount failed.")
    return False

def sd_append_reading(obj):
    """Append a JSON line to the queue file on SD. Safe and non-blocking as possible."""
    global sd_mounted
    if not sd_mounted:
        # try mount once if previously failed
        if not setup_sd():
            return False
    try:
        # open append; each reading is one JSON line
        with open(SD_QUEUE_FILE, "a") as f:
            f.write(json.dumps(obj) + "\n")
        # INDICATOR: SD Write
        print("💾 [SD] Data stored locally.")
        return True
    except Exception as e:
        print("⚠️ Failed to write to SD:", e)
        sd_mounted = False
        return False

def sd_readlines():
    """Return list of lines from queue file or empty list."""
    if not sd_mounted:
        if not setup_sd():
            return []
    try:
        with open(SD_QUEUE_FILE, "r") as f:
            lines = f.readlines()
        return lines
    except Exception as e:
        print("⚠️ SD readlines failed:", e)
        return []

def sd_truncate_queue():
    """Erase queue file after successful flush."""
    if not sd_mounted:
        if not setup_sd():
            return False
    try:
        # open for writing truncates
        with open(SD_QUEUE_FILE, "w") as f:
            pass
        return True
    except Exception as e:
        print("⚠️ SD truncate failed:", e)
        return False

# -------------------------------------------------------------------
# 1. Signal Processing Functions
# -------------------------------------------------------------------
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

# -------------------------------------------------------------------
# 2. Hardware Driver (PZEM-004T)
# -------------------------------------------------------------------
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
            "timestamp": get_timestamp(),
            "time_synced": rtc_synced,
            "voltageVolt": round(voltage, 1),
            "currentAmp": round(current, 3),
            "powerWatt": round(power, 1),
            "kwhConsumed": round(energy, 3),
            "powerFactor": round(pf, 2),
            "energySource": "Grid",
        }
    except Exception as e:
        print("PZEM Read Error:", e)
        return None

# -------------------------------------------------------------------
# 3. Network & MQTT Setup (Outbound Only) with SD flush on reconnect
# -------------------------------------------------------------------
def sync_time_with_ntp(max_retries=5):
    global rtc_synced, boot_time_at_sync, boot_ticks_at_sync
    import ntptime
    servers = ['pool.ntp.org', 'time.google.com']
    print("🕒 Syncing Time...")
    for i in range(max_retries):
        for host in servers:
            try:
                ntptime.host = host
                ntptime.settime()   # set RTC (UTC)
                # small sanity check: get utime.localtime() year
                year = utime.localtime()[0]
                if year < 2000 or year > 2035:
                    # unrealistic; keep trying
                    print(f"⚠️ NTP gave unrealistic year {year}, continuing.")
                    continue
                rtc_synced = True
                boot_time_at_sync = utime.time()
                boot_ticks_at_sync = time.ticks_ms()
                print(f"✅ NTP synced via {host} (year {year})")
                return True
            except Exception as e:
                print(f"⚠️ NTP Error ({host}): {e}")
                time.sleep(1)
    rtc_synced = False
    print("❌ NTP Failed.")
    return False

def get_timestamp():
    """Return a trustworthy timestamp (seconds since epoch).
       If RTC has been synced, use utime.time().
       If not, return an approximate monotonic-derived timestamp (boot-based).
       Also returns a tuple (ts, synced_bool) when called with get_timestamp(True).
    """
    global rtc_synced, boot_time_at_sync, boot_ticks_at_sync
    # If RTC was synced, ensure the value is sane
    if rtc_synced:
        try:
            ts = utime.time()
            year = utime.localtime(ts)[0]
            if 1970 <= year <= 2035:
                return ts
            else:
                # fallback to unsynced path
                rtc_synced = False
                print("⚠️ RTC returned abnormal year; treating as unsynced.")
        except Exception as e:
            rtc_synced = False
            print("⚠️ Error reading RTC:", e)

    # Unsynced: approximate using boot_time_at_sync if available, else use uptime base (monotonic)
    ticks_now = time.ticks_ms()
    if boot_time_at_sync and boot_ticks_at_sync:
        # compute elapsed seconds since we recorded boot_time_at_sync
        elapsed_ms = time.ticks_diff(ticks_now, boot_ticks_at_sync)
        approx_ts = int(boot_time_at_sync + (elapsed_ms / 1000.0))
        return approx_ts
    else:
        # we have no reference wall-clock; return uptime milliseconds as negative offset  (or zero)
        # To avoid producing absurd future dates, return 0 (Unix epoch) and rely on 'time_synced' flag.
        return 0


def setup_wifi(timeout_seconds=20):
    sta = network.WLAN(network.STA_IF)
    sta.active(True)
    if not sta.isconnected():
        try:
            sta.connect(WIFI_SSID, WIFI_PASSWORD)
        except Exception as e:
            print("⚠️ WiFi connect error:", e)
    print("Connecting to WiFi...", end="")
    waited = 0
    while not sta.isconnected() and waited < timeout_seconds:
        time.sleep(1)
        waited += 1
        print(".", end="")
    print("")
    if sta.isconnected():
        print("✅ WiFi Connected:", sta.ifconfig())
        sync_time_with_ntp()
        return True
    else:
        print("❌ WiFi Failed.")
        return False

def flush_sd_queue_to_mqtt():
    """Attempt to publish queued SD readings to MQTT. On full success, truncate queue."""
    if not mqtt_client:
        return False
    lines = sd_readlines()
    if not lines:
        return True
    
    # INDICATOR: Recovery Upload
    print(f"🔄 [Recovery] Found {len(lines)} readings on SD. Uploading to Cloud...")
    
    success_all = True
    for idx, line in enumerate(lines):
        try:
            payload = json.loads(line)
            # Attempt publish
            mqtt_client.publish(TOPIC_REGULAR, json.dumps(payload))
            print(f"   ⬆️ Uploaded cached reading {idx+1}/{len(lines)}")
            # small delay so broker isn't overwhelmed
            time.sleep(0.05)
        except Exception as e:
            print("⚠️ Failed to publish queued reading:", e)
            success_all = False
            break
    if success_all:
        sd_truncate_queue()
        print("✅ [Recovery] All offline data synced to Cloud.")
        return True
    else:
        print("⚠️ [Recovery] Sync interrupted. Will retry later.")
        return False

def mqtt_connect():
    global mqtt_client, was_connected
    try:
        # Dynamic SSL check
        use_ssl = (MQTT_PORT == 8883)
        ssl_params = {"server_hostname": MQTT_BROKER, "cert_reqs": 0} if use_ssl else {}
        
        mqtt_client = MQTTClient(
            MQTT_CLIENT_ID,
            MQTT_BROKER,
            port=MQTT_PORT,
            user=MQTT_USER,
            password=MQTT_PASS,
            keepalive=60,
            ssl=use_ssl,
            ssl_params=ssl_params
        )
        mqtt_client.connect()
        
        # INDICATOR: Regained Connection
        print("✅ MQTT Connected.")
        if not was_connected:
             print("✅ [Status] Connection REGAINED. Switching to Online Mode.")
             was_connected = True
        
        # Attempt flush of SD queue after connecting
        try:
            if sd_mounted:
                flush_sd_queue_to_mqtt()
        except Exception as e:
            print("⚠️ flush_sd_queue_to_mqtt error:", e)
        return True
    except Exception as e:
        print("❌ MQTT Connection Error:", e)
        mqtt_client = None
        
        # INDICATOR: Connection Lost
        if was_connected:
            print("⚠️ [Status] Connection LOST. Switching to Offline Mode (SD).")
            was_connected = False
        return False

def mqtt_pub(topic, payload):
    """Try to publish. On error or offline, append to SD queue if possible."""
    global mqtt_client, was_connected
    
    # Check if client is None (hard disconnect)
    if not mqtt_client:
        print("ℹ️ MQTT client not connected. Saving to SD.")
        # Changed: Save pure payload (JSON structure) to match MQTT data
        sd_append_reading(payload)
        return False

    try:
        # Publish
        if isinstance(payload, (dict, list)):
            mqtt_client.publish(topic, json.dumps(payload))
        else:
            mqtt_client.publish(topic, payload)
            
        # INDICATOR: Online Upload
        print(f"☁️ [Cloud] Uploaded to {topic}")
        return True
        
    except Exception as e:
        print("❌ Publish Error:", e)
        mqtt_client = None # Force reconnect next loop
        
        # INDICATOR: Connection Lost Detection
        if was_connected:
            print("⚠️ [Status] Connection LOST during publish. Switching to Offline Mode (SD).")
            was_connected = False
            
        # Fallback to SD - Save pure payload to match MQTT data
        sd_append_reading(payload)
        return False

# -------------------------------------------------------------------
# 4. Logic Core: Matching & State Management (unchanged)
# -------------------------------------------------------------------
def print_connected_devices():
    if not connected_devices:
        print("State: [Empty]")
    else:
        print("State: ", [f"{v['name']} ({v['powerWatt']}W)" for k,v in connected_devices.items()])

def find_best_match_for_drop(drop_watt):
    if not connected_devices: return None
    best_id = None
    best_diff = 100000.0
    for aid, info in connected_devices.items():
        stored_watt = info.get("powerWatt", 0)
        diff = abs(stored_watt - drop_watt)
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
            "buffer": []
        }
        active_events.append(event)
        debounce_active = False

    # --- C. Processing ---
    for ev in list(active_events):
        if not ev["finalized"]:
            ev["buffer"].append({
                "timestamp": reading["timestamp"],
                "voltageVolt": reading["voltageVolt"],
                "currentAmp": reading["currentAmp"],
                "powerWatt": current_power,
                "powerFactor": reading["powerFactor"]
            })

        if now >= ev["stabilization_time"] and not ev["finalized"]:
            if ev["event_type"] == "ON":
                residual = current_power - ev["pre_total"]
                if residual >= MIN_RESIDUAL_WATT:
                    print(f"🚀 Detected ON: {residual:.1f}W")
                    session_id = f"dev_{int(utime.time())}_{random.randint(100,999)}"
                    connected_devices[session_id] = {
                        "name": f"Unknown_{residual:.0f}W",
                        "powerWatt": residual,
                        "last_seen": utime.time()
                    }
                    print_connected_devices()
                    buf = ev["buffer"]
                    transient_data = buf[:4] if len(buf) >= 4 else buf
                    steady_data = buf[-4:] if len(buf) >= 4 else buf
                    sig = {
                        "dataType": "ApplianceSignature",
                        "deviceId": device_id,
                        "session_id": session_id,
                        "event_type": "ON",
                        "timestamp": reading["timestamp"],
                        "transient_data": transient_data,
                        "steady_state_data": steady_data,
                        "features": {
                            "steady_avg_power": round(residual, 1),
                            "powerFactor": reading["powerFactor"],
                            "detected_as": f"Unknown_{residual:.0f}W"
                        }
                    }
                    mqtt_pub(TOPIC_SIGNATURE, sig)
                else:
                    print("Ignored noise.")
            elif ev["event_type"] == "OFF":
                drop = ev["pre_total"] - current_power
                if drop > 0:
                    print(f"📉 Detected OFF: {drop:.1f}W")
                    match_id = find_best_match_for_drop(drop)
                    if match_id:
                        device = connected_devices.pop(match_id)
                        print(f"✅ Matched to {device['name']}. Removing.")
                        print_connected_devices()
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

# -------------------------------------------------------------------
# 5. Main Loop
# -------------------------------------------------------------------
# Attempt SD setup early
setup_sd()
# Attempt WiFi and MQTT
setup_wifi()
mqtt_connect()

print("🚀 System Ready (Autonomous Mode).")

while True:
    try:
        # --- 1. Connection Maintenance (NEW) ---
        # If we think we are disconnected, try to reconnect periodically
        if mqtt_client is None:
            current_time = time.ticks_ms()
            # Non-blocking reconnect attempt
            if time.ticks_diff(current_time, last_reconnect_attempt) > RECONNECT_INTERVAL_MS:
                last_reconnect_attempt = current_time
                print("🔄 [Auto-Reconnect] Checking Network...")
                
                # Check WiFi first
                sta = network.WLAN(network.STA_IF)
                if not sta.isconnected():
                    setup_wifi(timeout_seconds=5)
                
                # Check MQTT (only if WiFi is up)
                if sta.isconnected():
                    mqtt_connect()
        else:
            # If we think we are connected, check for messages (keepalive)
            try:
                mqtt_client.check_msg()
            except Exception as e:
                print("⚠️ Connection lost during check_msg:", e)
                mqtt_client = None
                if was_connected:
                    print("⚠️ [Status] Connection LOST. Switching to Offline Mode.")
                    was_connected = False

        # --- 2. Sensor Reading ---
        reading = pzem_data()

        if reading:
            # Full status output restored
            print(f"[Status] {reading['voltageVolt']}V | {reading['currentAmp']}A | {reading['powerWatt']}W | PF: {reading['powerFactor']}")

            detect_appliance_event(reading)

            now = utime.time()
            if now - last_regular_reading_time >= REGULAR_READING_INTERVAL_SECONDS:
                reading["dataType"] = "RegularReading"
                # Prefer direct publish; mqtt_pub will save to SD if offline
                mqtt_pub(TOPIC_REGULAR, reading)
                last_regular_reading_time = now

        time.sleep(0.5)

    except KeyboardInterrupt:
        break
    except Exception as e:
        print("Error in Main Loop:", e)
        # Fallback reconnect logic just in case
        time.sleep(5)