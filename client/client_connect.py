# -*- coding: utf-8 -*-
import socket
import platform
import psutil
import json
import time
import uuid
import threading

def get_mac_address(): 
    mac = uuid.UUID(int=uuid.getnode()).hex[-12:] 
    return ":".join([mac[e:e+2] for e in range(0, 11, 2)])

def get_device_info():
    info = {
        "mac": get_mac_address(),
        "cpu_cores": psutil.cpu_count(logical=False),
        "cpu_freq": psutil.cpu_freq().max,
        "cpu_arch": platform.machine(),
        "total_memory": psutil.virtual_memory().total,
        "memory_bandwidth": "N/A",
        "storage": psutil.disk_usage('/').total,
        "gpu_cores": "N/A",
        "gpu_memory": "N/A",
        "power_consumption": "N/A"
    }
    return info

def report_to_server(server_ip, server_port):
    while True:
        try:
            tcp_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            tcp_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            tcp_socket.connect((server_ip, server_port))
            
            device_info = get_device_info()
            tcp_socket.send(json.dumps(device_info).encode('utf-8'))
            tcp_socket.close()
            time.sleep(10)
        except Exception as e:
            print(f"fail to connect: {e}")
            time.sleep(10)

# Handles host computer connections
def handle_client_connection(client_socket):
    try:
        data = client_socket.recv(1024)
        if data:
            message = data.decode('utf-8')
            print(f"receive data: {message}")
    except Exception as e:
        print(f"An error occurred while processing the host computer connection: {e}")
    finally:
        client_socket.close()

def start_listening(port):
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server_socket.bind(('0.0.0.0', port))
    server_socket.listen(5)
    print(f"listening port {port}，waiting the connection from host computer...")

    while True:
        try:
            client_socket, client_address = server_socket.accept()
            print(f"receive connection from {client_address} ")
            client_thread = threading.Thread(target=handle_client_connection, args=(client_socket,))
            client_thread.start()
        except Exception as e:
            print(f"An error occurred while processing connection: {e}")



if __name__ == "__main__":
    server_ip = "192.168.8.66"
    server_port = 12347

    report_thread = threading.Thread(target=report_to_server, args=(server_ip, server_port))
    report_thread.daemon = True  
    report_thread.start()

    while True:
        time.sleep(1)
