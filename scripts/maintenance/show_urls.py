import socket

def get_ip_addresses():
    ip_list = []
    try:
        # 获取主机名
        hostname = socket.gethostname()
        # 获取所有关联的IP地址
        _, _, addresses = socket.gethostbyname_ex(hostname)
        
        for ip in addresses:
            # 过滤掉回环地址
            if not ip.startswith("127."):
                # 简单的内网IP过滤 (10.x, 172.16-31.x, 192.168.x)
                if ip.startswith("10.") or ip.startswith("192.168.") or (ip.startswith("172.") and 16 <= int(ip.split('.')[1]) <= 31):
                    ip_list.append(ip)
    except Exception:
        pass
    return ip_list

if __name__ == "__main__":
    ips = get_ip_addresses()
    
    print("\n[Network Access Addresses]")
    if not ips:
        print("  Warning: No active LAN connection detected.")
    
    for ip in ips:
        print(f"  Interface IP: {ip}")
        print(f"    - Docs:      http://{ip}:8000")
        print(f"    - Dashboard: http://{ip}:8501")
    print("")
