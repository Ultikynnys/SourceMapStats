import sys
import os

# Add local paths
cwd = os.getcwd()
sys.path.insert(0, os.path.join(cwd, "pythonvalve"))
sys.path.insert(0, os.path.join(cwd, "a2s"))

from app import app
from utils import ADMIN_IPS

print(f"Loaded Admin IPs: {ADMIN_IPS}")

with app.test_client() as client:
    # Test 1: Access without admin IP
    print("\n--- Test 1: Non-admin IP (10.0.0.1) ---")
    resp = client.get('/api/admin/stats', environ_base={'REMOTE_ADDR': '10.0.0.1'})
    print(f"Status: {resp.status_code}")
    print(f"Data: {resp.get_data(as_text=True)}")

    # Test 2: Access with admin IP (127.0.0.1)
    print("\n--- Test 2: Admin IP (127.0.0.1) ---")
    resp = client.get('/api/admin/stats', environ_base={'REMOTE_ADDR': '127.0.0.1'})
    print(f"Status: {resp.status_code}")
    print(f"Data: {resp.get_data(as_text=True)}")
    
    # Test 3: Access with Admin IP (::1) - Should now work due to my fix
    print("\n--- Test 3: Admin IP (::1) ---")
    resp = client.get('/api/admin/stats', environ_base={'REMOTE_ADDR': '::1'})
    print(f"Status: {resp.status_code}")
    print(f"Data: {resp.get_data(as_text=True)}")
