import socket
import os
import struct
import threading
import time
from shared_vars import task_time_info


# Send directory structure
def send_directory(sock, path):
    path_bytes = path.encode('utf-8')
    path_len = len(path_bytes)
    header = struct.pack('!B I', ord('D'), path_len)
    sock.sendall(header + path_bytes)
    print(f"send path: {path}, path length: {path_len}")  

# Receive a specified amount of data
def recv_all(sock, n):
    data = bytearray()
    while len(data) < n:
        packet = sock.recv(n - len(data))
        if not packet:
            return None
        data.extend(packet)
    return data

# Send a single file
def send_file(sock, file_path, relative_path):
    try:
        file_size = os.path.getsize(file_path)
        path_bytes = relative_path.encode('utf-8')
        
        path_len = len(path_bytes)
        print(f"send file: {file_path}, file path: {relative_path}, path length: {path_len}, file size: {file_size}, path_bytes: {path_bytes}")  
        header = struct.pack('!B I Q', ord('F'), path_len, file_size)
        sock.sendall(header)
        sock.sendall(path_bytes)
        
        with open(file_path, 'rb') as f:
            while True:
                data = f.read(4096)
                if not data:
                    break
                sock.sendall(data)
    except Exception as e:
        print(f"send file {file_path} ERROR: {str(e)}")

# Iterate through the directory recursively and send data
def traverse_directory(sock, source_dir):
    parent_dir = os.path.dirname(source_dir)  
    base_name = os.path.basename(source_dir)  
    
    # Send the root directory first
    send_directory(sock, base_name)
    
    # Traverse the entire directory tree
    for root, dirs, files in os.walk(source_dir):
        # get the path relative to the parent directory
        rel_root = os.path.relpath(root, parent_dir)
        if rel_root == base_name:
            rel_root = base_name
        else:
            send_directory(sock, rel_root)
        for file in files:
            abs_path = os.path.join(root, file)
            rel_path = os.path.relpath(abs_path, parent_dir)
            send_file(sock, abs_path, rel_path)

# Send the specified file and the data folder (if present)
def send_selected_files_and_data(sock, base_dir, file_paths):
    # Record the directory to be created (relative path)
    required_dirs = set()
    all_files = set(file_paths)
    data_dir = os.path.join(base_dir, "data")
    if os.path.exists(data_dir) and os.path.isdir(data_dir):
        for root, _, files in os.walk(data_dir):
            for file in files:
                all_files.add(os.path.join(root, file))
    
    # Collect all directories that need to be created
    for abs_path in all_files:
        rel_path = os.path.relpath(abs_path, base_dir)
        dir_path = os.path.dirname(rel_path)
        parts = []
        current_dir = dir_path
        while current_dir:
            parts.append(current_dir)
            current_dir = os.path.dirname(current_dir)
        required_dirs.update(parts)

    sorted_dirs = sorted(required_dirs, key=lambda x: x.count(os.sep))
    for rel_dir in sorted_dirs:
        send_directory(sock, rel_dir)
    for abs_path in all_files:
        rel_path = os.path.relpath(abs_path, base_dir)
        send_file(sock, abs_path, rel_path)

def start_feedback_server(host='0.0.0.0', port=12348, save_dir='./server_received', scheduler=None):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.bind((host, port))
        s.listen(5)
        print(f"Data receiving server started, listening {host}:{port}")

        while True:
            conn, addr = s.accept()
            print(f"receiving data from {addr}")
            try:
                count_bytes = recv_all(conn, 4)
                file_count = struct.unpack('!I', count_bytes)[0] if count_bytes else 0
                print(f"Expected recieve files count {file_count}")

                while True:
                    header = recv_all(conn, 13)  # 1 + 4 + 8
                    if not header or len(header) != 13:
                        break

                    type_code, path_len, file_size = struct.unpack('!B I Q', header)
                    path_bytes = recv_all(conn, path_len)
                    path = path_bytes.decode('utf-8')
                    
                    full_path = os.path.join(save_dir, path)
                    os.makedirs(os.path.dirname(full_path), exist_ok=True)

                    received = 0
                    with open(full_path, 'wb') as f:
                        while received < file_size:
                            data = conn.recv(min(4096, file_size - received))
                            if not data:
                                break
                            f.write(data)
                            received += len(data)
                    
                    print(f"received data size: {path} ({received}/{file_size} bytes)")
                
                print(f"Complete receiving data from {addr}")
                lock = 1
                if scheduler:
                    device_ip = addr[0]
                    if lock:
                        if device_ip in scheduler.devices:
                            task_id = scheduler.devices[device_ip].get('allocated_tasks')
                            if isinstance(task_id, list):
                                task_id = task_id[0] if task_id else None
                            
                            if task_id is not None:
                                with scheduler.devices_lock:
                                    scheduler.devices[device_ip]['status'] = 1
                                    scheduler.devices[device_ip]['allocated_tasks'] = None
                                    print(f"device {device_ip} return to free")

                                with scheduler.tasks_lock:
                                    if task_id in scheduler.tasks:  
                                        task = scheduler.tasks[task_id]
                                        task['status'] = 1
                                        task['completion_time'] = time.time()
                                        print(f"task {task_id} completed at: {task['completion_time']}")
                                        
                                        task_key = f"{task['file_path']}_{task['task_id']}"
                                        if task_key in task_time_info:
                                            task_time_info[task_key]["completion_time"] = task['completion_time']
                                            print(f"renew task_time_info: {task_key}, completion_time: {task['completion_time']}")
                                        else:
                                            print(f"fail to find the key in task_time_info: {task_key}")
                                        print("task_time_info key at present:", task_time_info.keys())
                            lock = 0
            except Exception as e:
                print(f"An error occurred while processing feedback: {e}")
                import traceback
                traceback.print_exc()
            finally:
                conn.close()

# Create and connect to the target IP and port              
def create_connection(ip, port):
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect((ip, port))  
    return sock

if __name__ == '__main__':
    target_ip = "192.168.8.81"
    target_port = 12346
    sock = create_connection(target_ip, target_port)
    source_dir = "./bayes"
    traverse_directory(sock, source_dir)
    threading.Thread(target=start_feedback_server, daemon=True).start()

