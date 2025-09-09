from collections import deque
import socket
import os
import struct
import subprocess
import time
import sys
import threading
from datetime import datetime
import csv
import psutil
import matplotlib.pyplot as plt
import matplotlib.dates as mdates

# log configuration module
REPORTS_DIR = "reports"  # The subdirectory where the report file is stored
os.makedirs(REPORTS_DIR, exist_ok=True)  
MAX_DATA_POINTS = 10000  

# Memory monitor module
memory_data = []  
current_root_dir = None  
data_lock = threading.Lock()  
monitoring_active = True  
sampling_interval = 0.1  
start_time = None  

def memory_monitor_thread():
    global start_time
    start_time = time.time()
    
    while monitoring_active:
        try:
            timestamp = time.time() - start_time
            mem_info = psutil.virtual_memory()
            mem_usage_percent = mem_info.percent
            mem_used_mb = mem_info.used / (1024 * 1024)
            
            with data_lock:
                memory_data.append((timestamp, mem_usage_percent, mem_used_mb, current_root_dir))
        except Exception as e:
            print(f"Memory monitoring error: {e}")
        
        time.sleep(sampling_interval)

# Force a memory sample for the beginning and end of the task
def force_memory_sample():
    try:
        timestamp = time.time() - start_time  
        mem_info = psutil.virtual_memory()
        mem_usage_percent = mem_info.percent
        mem_used_mb = mem_info.used / (1024 * 1024)
        
        with data_lock:
            memory_data.append((timestamp, mem_usage_percent, mem_used_mb, current_root_dir))
    except Exception as e:
        print(f"Forced sampling error: {e}")

# The data in memory is processed by moving average to make the curve smoother
def smooth_memory_data(data):
    window_size = 5  # Sliding window size
    smoothed_data = []
    for i in range(len(data)):
        start = max(0, i - window_size // 2)
        end = min(len(data), i + window_size // 2 + 1)
        window = data[start:end]
        avg_percent = sum(row[1] for row in window) / len(window)
        avg_used_mb = sum(row[2] for row in window) / len(window)
        smoothed_data.append((data[i][0], avg_percent, avg_used_mb, data[i][3]))
    return smoothed_data


def generate_report():
    with data_lock:
        data_copy = list(memory_data)  

    if not data_copy:
        return

    smoothed_data = smooth_memory_data(data_copy)
    time_str = datetime.now().strftime("%Y%m%d_%H%M%S")
    csv_file = os.path.join(REPORTS_DIR, f"memory_report_{time_str}.csv")
    img_file = os.path.join(REPORTS_DIR, f"memory_usage_{time_str}.png")

    with open(csv_file, mode='w', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        writer.writerow(['relative_time', 'usage_percent', 'used_mb', 'root_dir'])
        writer.writerows(smoothed_data)

    relative_times = [row[0] for row in smoothed_data]
    usages_percent = [row[1] for row in smoothed_data]
    usages_mb = [row[2] for row in smoothed_data]
    root_dirs = [row[3] for row in smoothed_data]

    fig, ax1 = plt.subplots(figsize=(12, 6))
    
    # Main vertical axis: percentage of memory usage
    ax1.set_xlabel('Time(s)', fontsize=12)
    ax1.set_ylabel('Memory_Usage(%)', fontsize=12)
    line1, = ax1.plot(relative_times, usages_percent, 'b-', marker='o', markersize=4, label='Memory_Usage(%)')
    ax1.tick_params(axis='y', labelcolor='b')
    ax1.grid(True, linestyle='--', alpha=0.7)
    
    # Secondary vertical axis: Used memory (MB)
    ax2 = ax1.twinx()
    ax2.set_ylabel('Allocated_Memory(MB)', fontsize=12)
    line2, = ax2.plot(relative_times, usages_mb, 'r-', marker='s', markersize=4, label='Allocated_Memory(MB)')
    ax2.tick_params(axis='y', labelcolor='r')
    
    prev_root = None
    for i, root_dir in enumerate(root_dirs):
        if root_dir and root_dir != prev_root:
            ax1.axvline(relative_times[i], color='g', linestyle='--', alpha=0.5)
            ax1.text(relative_times[i], usages_percent[i], 
                    root_dir, rotation=45, ha='right', va='bottom')
            prev_root = root_dir
    
    lines = [line1, line2]
    labels = [line.get_label() for line in lines]
    ax1.legend(lines, labels, loc='upper left')
    
    plt.title('System memory usage monitoring', fontsize=14)
    plt.tight_layout()
    plt.savefig(img_file, dpi=150)
    plt.close()
    print(f"Generated report: {csv_file} and {img_file}")

# Generates a report every 5 minutes
def report_generator_thread():
    while monitoring_active:
        time.sleep(120)
        generate_report()
        
def recv_all(sock, n):
    data = bytearray()
    while len(data) < n:
        packet = sock.recv(n - len(data))
        if not packet:
            return None
        data.extend(packet)
    return data

def listen_for_connection(port=12346):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(('0.0.0.0', port))
    s.listen(1)
    print(f"Listening port {port}，waiting connection from host computing...")
    conn, addr = s.accept()
    print(f"received connection from {addr[0]}:{addr[1]} ")
    return conn

# Receive the data and return the root directory name
def receive_data(sock, save_dir='./client_data'):
    os.makedirs(save_dir, exist_ok=True)
    root_dir = None
    
    try:
        while True:
            header = recv_all(sock, 5)
            if not header or len(header) != 5:
                print("Failed to receive the header message")
                break

            type_code, path_len = struct.unpack('!B I', header)
            
            if chr(type_code) == 'D':
                path_bytes = recv_all(sock, path_len)
                path = path_bytes.decode('utf-8')
                full_path = os.path.join(save_dir, path)
                
                if root_dir is None:
                    root_dir = path
                    print(f"The root directory is identified: {root_dir}")
                
                os.makedirs(full_path, exist_ok=True)

            elif chr(type_code) == 'F':
                size_bytes = recv_all(sock, 8)
                if not size_bytes:
                    print("Failed to receive the file size.")
                    break
                file_size = struct.unpack('!Q', size_bytes)[0]
                
                path_bytes = recv_all(sock, path_len)
                path = path_bytes.decode('utf-8')
                full_path = os.path.join(save_dir, path)

                os.makedirs(os.path.dirname(full_path), exist_ok=True)
                
                received = 0
                with open(full_path, 'wb') as f:
                    while received < file_size:
                        data = sock.recv(min(4096, file_size - received))
                        if not data:
                            break
                        f.write(data)
                        received += len(data)
            else:
                print(f"Unknown data type: {type_code}")
                break
    finally:
        sock.close()
    
    return root_dir
    
# Send the generated data file to the server
def send_data_to_server(root_dir, save_dir='./client_data', host='192.168.8.66', data_port=12348):
    target_dir = os.path.join(save_dir, root_dir)
    data_files = []
    
    for root, _, files in os.walk(target_dir):
        for file in files:
            if file == 'data':
                full_path = os.path.join(root, file)
                rel_path = os.path.relpath(full_path, save_dir)
                data_files.append((full_path, rel_path))

    if not data_files:
        return

    max_retries = 3
    for retry in range(max_retries):
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.settimeout(30)
                s.connect((host, data_port))
                
                s.sendall(struct.pack('!I', len(data_files)))

                for idx, (full_path, rel_path) in enumerate(data_files):
                    file_size = os.path.getsize(full_path)
                    path_bytes = rel_path.encode('utf-8')
                    
                    header = struct.pack('!B I Q', ord('F'), len(path_bytes), file_size)
                    s.sendall(header + path_bytes)

                    with open(full_path, 'rb') as f:
                        bytes_sent = 0
                        while bytes_sent < file_size:
                            chunk = f.read(4096)
                            s.sendall(chunk)
                            bytes_sent += len(chunk)

                print(f"send file successfully of a mount of {len(data_files)} ")
                return
        except Exception as e:
            print(f"fail to transport ({retry+1}/3): {e}")

# execute task module
def execute_scripts(root_dir, save_dir='./client_data'):
    if not root_dir:
        return
    
    target_dir = os.path.join(save_dir, root_dir)
    
    for root, _, files in os.walk(target_dir):
        for file in files:
            if file.endswith('.sh'):
                script_path = os.path.join(root, file)
                script_dir = os.path.dirname(script_path)
                
                try:
                    force_memory_sample()
                    
                    os.chmod(script_path, 0o755)
                    result = subprocess.run(
                        ['bash', file],
                        cwd=script_dir,
                        stdout=subprocess.PIPE,
                        stderr=subprocess.PIPE,
                        universal_newlines=True
                    )
                    
                    print(f"execute {file} result:")
                    print(result.stdout)
                    if result.stderr:
                        print("output error:")
                        print(result.stderr)
                        
                except Exception as e:
                    print(f"fail to execute the task: {str(e)}")
                finally:
                    force_memory_sample()
                    
def main():
    global current_root_dir, monitoring_active

    monitor_thread = threading.Thread(target=memory_monitor_thread, daemon=True)
    monitor_thread.start()

    report_thread = threading.Thread(target=report_generator_thread, daemon=True)
    report_thread.start()

    while True:
        try:
            sock = listen_for_connection()
            root_dir = receive_data(sock)

            with data_lock:
                current_root_dir = root_dir

            if root_dir:
                print("\nStart executing the script...")
                execute_scripts(root_dir)

                print("\nThe data is sending back...")
                send_data_to_server(root_dir)
            
            with data_lock:
                current_root_dir = None
                
        except Exception as e:
            print(f"Main loop error: {e}")
        finally:
            if 'sock' in locals():
                sock.close()

if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print("\nProgram termination")
        monitoring_active = False
        sys.exit(0)
