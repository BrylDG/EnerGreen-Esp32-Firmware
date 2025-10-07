# boot.py
import esp
import network
import time

# --- Import secrets ---
try:
    from config import WIFI_SSID, WIFI_PASSWORD
except ImportError:
    print("Error: secrets.py not found or incomplete. Please create it.")
    # You might want to halt here or provide default generic credentials for debugging
    WIFI_SSID = 'default_ssid'
    WIFI_PASSWORD = 'default_password'
# --- End Import secrets ---

esp.osdebug(None)

print("Connecting to WiFi...")
sta_if = network.WLAN(network.STA_IF)
if not sta_if.isconnected():
    sta_if.active(True)
    sta_if.connect(WIFI_SSID, WIFI_PASSWORD)
    timeout = 0
    while not sta_if.isconnected() and timeout < 20:
        print('.', end='')
        time.sleep(1)
        timeout += 1

if sta_if.isconnected():
    print("\nWi-Fi connected!")
    print('Network config:', sta_if.ifconfig())
else:
    print("\nFailed to connect to Wi-Fi.")
    print("Please check SSID, password, and network availability.")

print("Boot sequence complete.")