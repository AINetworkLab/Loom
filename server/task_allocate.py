import time
import pymysql
import random
import threading
from pymysql.cursors import DictCursor
import subprocess
import task_send_monitor
import vm_exec
import os
import glob
import shlex  
import logging  
import csv  
from collections import defaultdict  
from shared_vars import task_time_info

# Log Configuration
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# compiled_mode = 0-JIT \ 2-AOT \ 1-balance
compiled_mode = 1
# task_mode = sequence \ random
task_mode = 'sequence'
tasks_count = 60
# run mode = WRCS \ k8s-roll \ k8s-resources
run_mode = 'WRCS'
# use_prior = 1: use prior \ 0: do not use prior
use_prior = 0
#use_given_time = 1:compared with the given time \ 0:do not compared with the given time
use_given_time = 1

def generate_csv():
    if not os.path.exists("data"):
        os.makedirs("data")
    timestamp = time.strftime("%Y%m%d_%H%M%S")
    csv_file_path = os.path.join("data", f"task_time_info_{timestamp}.csv")
    with open(csv_file_path, mode='w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(["Task Key", "Creation Time (s)", "Transfer Time (s)", "Compile Time (s)", "Execution Time (s)", "Device IP", "Start Time (s)", "Completion Time (s)", "Given Time (s)", "Life Time (s)"])
        for task_key, times in task_time_info.items():
            creation_time = times.get('creation_time', 0)
            transfer_time = times.get("transfer_time", 0)
            compile_time = times.get("compile_time", 0)
            start_time = times.get("start_time", 0)
            completion_time = times.get("completion_time", 0)
            execution_time = completion_time - start_time - compile_time - transfer_time if completion_time > 0 else 0  
            device_ip = times.get('device', 0)
            given_time = times.get('given_time', 0)
            life_time = completion_time - creation_time
            writer.writerow([task_key, creation_time, transfer_time, compile_time, execution_time, device_ip, start_time, completion_time, given_time, life_time])
    logger.info(f"CSV file has been generated: {csv_file_path}")

# periodically generates CSV files
def csv_generation_thread():
    while True:
        time.sleep(120)  
        generate_csv()

class TaskScheduler:
    def __init__(self, task_templates, devices):
        self.task_templates = list(task_templates.values())  # task template
        self.devices = devices
        self.tasks = {}  # task pool
        self.running = True
        self.tasks_lock = threading.Lock()  
        self.devices_lock = threading.Lock()  
        self.task_id_counter = max(t['task_id'] for t in self.task_templates) + 1 if self.task_templates else 1
        self.task_over = 0
        self.start = time.time()
        self.task_assigned_event = threading.Event()  

    # Dynamic task generation
    def start_task_generation(self, count):
        def generate_tasks():
            while self.running:
                if(task_mode == 'random'):
                    if count == 0:
                        lambda_rate = 3 / 6  
                        interval = random.expovariate(lambda_rate)
                        time.sleep(interval)
                        self._add_new_task_random()
                    else:
                        for i in range(count):
                            lambda_rate = 3 / 6  
                            interval = random.expovariate(lambda_rate)
                            time.sleep(interval)
                            self._add_new_task_random()
                        self.task_over = 1
                        break
                    
                elif(task_mode == 'sequence'):
                    self._add_tasks_in_order(rounds=3)
                    self.task_over = 1
                    break
        threading.Thread(target=generate_tasks, daemon=True).start()

    # generate all templates in order
    def _add_tasks_in_order(self, rounds=1):
        for _ in range(rounds):
            for template in self.task_templates:
                new_task = template.copy()
                new_task['creation_time'] = time.time()
                new_task['status'] = -1
                
                with self.tasks_lock:
                    new_task['task_id'] = self.task_id_counter
                    self.task_id_counter += 1
                    self.tasks[new_task['task_id']] = new_task
                
                logger.info(
                    f"Sequential generation task: ID={new_task['task_id']}, "
                    f"JIT_memory={new_task['memory_JIT']}, "
                    f"AOT_memory={new_task['memory_AOT']}"
                )
                time.sleep(0.5)  

    # Generated based on the weight of the task template randomly
    def _add_new_task_random(self):
        selected_template = random.choices(
            self.task_templates, 
            weights=[t['weight'] for t in self.task_templates], 
            k=1
        )[0]
        
        new_task = selected_template.copy()
        new_task['creation_time'] = time.time()
        new_task['status'] = -1
        
        with self.tasks_lock:  
            new_task['task_id'] = self.task_id_counter
            self.task_id_counter += 1
            self.tasks[new_task['task_id']] = new_task
        
        logger.info(f"New task has been generated: ID={new_task['task_id']}, JIT_memory={new_task['memory_JIT']}, AOT_memory={new_task['memory_AOT']}")

    # Get free devices
    def get_available_devices(self):
        # with self.devices_lock:
        return [d.copy() for d in self.devices.values() if d['status'] == 1]
    
    # Synthesize wait time and priority ordering
    def sort_tasks_by_priority(self):
        current_time = time.time()
        with self.tasks_lock:  
            unexecuted_tasks = [task.copy() for task in self.tasks.values() if task.get('status') == -1]
        def priority_key(task):
            wait_time = current_time - task['creation_time']
            score = task['prior'] - (wait_time / 10) * 0.1
            return max(round(score, 2), 0.1)
        return sorted(unexecuted_tasks, key=priority_key)

    def get_tasks_k8s(self):
        with self.tasks_lock:  
            unexecuted_tasks = [task.copy() for task in self.tasks.values() if task.get('status') == -1]
        return unexecuted_tasks

    def sort_devices_by_memory(self, devices):
        return sorted(devices, key=lambda x: (-x['memory_level'], -x['total_memory']))
    
    def CompileSend_AOT(self, task, device):
        real_source_path = os.path.join("file", "JIT", task['file_path'])
        real_target_path = os.path.join("file", "AOT", device['cpu_arch'], task['file_path'])
        
        if '../' in task['file_path']:
            raise ValueError(f"illegal file path: {task['file_path']}")
        
        try:
            os.makedirs(real_target_path, exist_ok=True)
        except PermissionError as e:
            logger.error(f"Unable to create directory {real_target_path}: {e}")
            return None
        
        if os.path.exists(real_target_path):
            cwasm_files = glob.glob(os.path.join(real_target_path, "*.cwasm"))
            if cwasm_files:
                return real_target_path

        suffix = ".wasm"
        
        if device['cpu_arch'] == 'x86_64':
            aot_path = f"/home/wyn/Desktop/socket/file/AOT/{device['cpu_arch']}/{task['file_path']}"
            source_path = f"/home/wyn/Desktop/socket/file/JIT/{task['file_path']}"
            for filename in os.listdir(source_path):
                if filename.endswith(suffix):
                    filename = os.path.splitext(filename)[0] 
                    compile_cmd = (
                            f"wasmtime compile {shlex.quote(source_path)}/{filename}.wasm "
                            f"-o {shlex.quote(aot_path)}/{filename}.cwasm && "
                            f"cp {shlex.quote(source_path)}/run.sh {shlex.quote(aot_path)}/run.sh"
                        )
                    subprocess.run(compile_cmd, shell=True)

        else:
            virt_aot_path = f"/root/work/AOT/{device['cpu_arch']}/{task['file_path']}"
            virt_source_path = f"/root/work/JIT/{task['file_path']}"
            
            for filename in os.listdir(real_source_path):
                if filename.endswith(suffix):
                    filename = os.path.splitext(filename)[0]
                    compile_cmd = (
                        f"wasmtime compile {shlex.quote(virt_source_path)}/{filename}.wasm "
                        f"-o {shlex.quote(virt_aot_path)}/{filename}.cwasm && "
                        f"cp {shlex.quote(virt_source_path)}/run.sh {shlex.quote(virt_aot_path)}/run.sh"
                    )
                    vm_name = f"ubuntu22.04-{device['cpu_arch']}"
                    logger.info(f"exeute the compiling command: {compile_cmd}")  
                    vm_exec.send_command_to_vm(vm_name, compile_cmd)
        
        return real_target_path
    
    # Assign tasks and update status
    def allocate_task(self, task, device, max_retries=10, retry_delay=5):
        with self.tasks_lock:
            task = self.tasks[task['task_id']]  
        logger.info(f"allocate task {task['task_id']} to device {device['ip']}") 
        with self.devices_lock:
            self.devices[device['ip']]['status'] = 0
            self.devices[device['ip']]['allocated_tasks'] = [task['task_id']]

        with self.tasks_lock:
            self.tasks[task['task_id']]['status'] = 0
            self.tasks[task['task_id']]['start_time'] = time.time()
        
        task['start_time'] = time.time()  
        print(f"allocate task {task['task_id']}，device_ip: {device['ip']}")

        task_key = f"{task['file_path']}_{task['task_id']}"
        task_time_info[task_key] = {
        "creation_time": task['creation_time'],
        "start_time": task['start_time'],
        "device": device['ip'],
        "transfer_time": 0,
        "compile_time": 0,
        "completion_time": 0,  
        "given_time": task['given_time']
    }
        
        self.task_assigned_event.set()  

        target_ip = device['ip']
        target_port = 12346
        source_dir = os.path.join("file/JIT", task['file_path'])
        
        retries = 0
        while retries < max_retries:
            try:
                sock = task_send_monitor.create_connection(target_ip, target_port)
                logger.info(f"connect the device successfully {target_ip}:{target_port}")
                break
            except ConnectionError as e:
                retries += 1
                logger.warning(f"fail to connect: {e}，retry {retries}/{max_retries}")  
                if retries < max_retries:
                    time.sleep(retry_delay)
                else:
                    with self.devices_lock:
                        self.devices[device['ip']]['status'] = 1  
                    with self.tasks_lock:
                        self.tasks[task['task_id']]['status'] = -1
                    logger.error(f"fail to connect {target_ip}:{target_port}，fail to allocate the task")  
                    return
        
        if compiled_mode == 1:   
            if task['memory_AOT'] < task['memory_JIT']:
                compile_start = time.time()
                source_dir = self.CompileSend_AOT(task, device)
                compile_end = time.time()
                compile_time = compile_end - compile_start
                logger.info(f"task {task['file_path']} AOT compilation time consuming: {compile_time:.2f} seconds")  
                task_time_info[f"{task['file_path']}_{task['task_id']}"]["compile_time"] = compile_time

        elif compiled_mode == 2:    
            compile_start = time.time()
            source_dir = self.CompileSend_AOT(task, device)
            compile_end = time.time()
            compile_time = compile_end - compile_start
            logger.info(f"task {task['file_path']} AOT compilation time consuming: {compile_time:.2f} seconds")  
            task_time_info[f"{task['file_path']}_{task['task_id']}"]["compile_time"] = compile_time
            
        else:   
            source_dir = os.path.join("file/JIT", task['file_path'])
            task_time_info[f"{task['file_path']}_{task['task_id']}"]["compile_time"] = 0
        
        transfer_start = time.time()
        task_send_monitor.traverse_directory(sock, source_dir)
        transfer_end = time.time()
        transfer_time = transfer_end - transfer_start
        logger.info(f"Task transfer time consuming: {transfer_time:.2f} seconds")  
        task_time_info[f"{task['file_path']}_{task['task_id']}"]["transfer_time"] = transfer_time
        sock.close() 
   
    def run(self):
        if run_mode == 'WRCS':
            self.start_task_generation(count = tasks_count)  
            threading.Thread(target=task_send_monitor.start_feedback_server, daemon=True, kwargs={'scheduler': self}).start()
            threading.Thread(target=csv_generation_thread, daemon=True).start()
            while self.running:
                available_devices = self.get_available_devices()
                sorted_devices = self.sort_devices_by_memory(available_devices)
                if use_prior == 1:
                    sorted_tasks = self.sort_tasks_by_priority()  
                elif use_prior == 0:
                    sorted_tasks = self.get_tasks_k8s()

                if(task_mode == 'random' and tasks_count == 0):
                    if time.time()-self.start >= 300:
                        print("Five minutes is up")
                        generate_csv()
                        break
                if self.task_over and not sorted_tasks and all(device['status'] == 1 for device in self.devices.values()):
                    print(f"All tasks completed, time consuming: {time.time()-self.start:.2f} seconds")
                    generate_csv()
                    break
                if not available_devices or not sorted_tasks:
                    time.sleep(1)
                    continue

                allocated = False
                for device in sorted_devices:
                    # Try a peer task first -> Downgrade to try all tasks if you fail
                    for task_source in [
                        [t for t in sorted_tasks if t['memory_level'] == device['memory_level']],  
                        sorted_tasks  
                    ]:
                        for task in task_source.copy():  
                            required_memory = max(task['memory_JIT'], task['memory_AOT']) * 1024**2
                            
                            if required_memory <= device['total_memory']:
                                if task in sorted_tasks:  
                                    self.allocate_task(task, device)
                                    sorted_tasks.remove(task)
                                    allocated = True
                                    print(f"allocated successfully: task {task['task_id']} -> device {device['ip']}")
                                    break  
                            else:
                                print(f"task {task['task_id']} -> device {device['ip']} is Out of memory")
                        if allocated:
                            break  
                    if allocated:
                        break  

                time.sleep(0.5 if allocated else 1)  

        elif run_mode == 'k8s-roll':
            self.start_task_generation(count = tasks_count)  
            threading.Thread(target=task_send_monitor.start_feedback_server, daemon=True, kwargs={'scheduler': self}).start()
            threading.Thread(target=csv_generation_thread, daemon=True).start()
            while self.running:
                available_devices = self.get_available_devices()               
                if use_prior == 1:
                    tasks = self.sort_tasks_by_priority()  
                elif use_prior == 0:
                    tasks = self.get_tasks_k8s()
                if(task_mode == 'random' and tasks_count == 0):
                    if time.time()-self.start >= 300:
                        print("Five minutes is up")
                        generate_csv()
                        break
                if self.task_over and not tasks and all(device['status'] == 1 for device in self.devices.values()):
                    print(f"All tasks completed, time consuming: {time.time()-self.start:.2f} seconds")
                    generate_csv()
                    break

                if not available_devices or not tasks:
                    time.sleep(1)
                    continue
                i = 0
                allocated = False
                while i < len(tasks):
                    task = tasks[i]
                    required_memory = max(task['memory_JIT'], task['memory_AOT']) * 1024**2
                    for device in available_devices:
                        if device.get('total_memory') >= required_memory:
                            self.allocate_task(task, device)
                            print(f"allocated successfully: task {task['task_id']} -> device {device['ip']}")
                            allocated = True
                            break  
                        else:
                            i += 1  
                            print(f"task {task['task_id']} -> device {device['ip']} is Out of memory")
                    if allocated:
                        break

                time.sleep(0.5 if allocated else 1)  
                    

        elif run_mode == 'k8s-resources':
            self.start_task_generation(count = tasks_count)  
            threading.Thread(target=task_send_monitor.start_feedback_server, daemon=True, kwargs={'scheduler': self}).start()
            threading.Thread(target=csv_generation_thread, daemon=True).start()
            while self.running:
                available_devices = self.get_available_devices()                
                tasks = self.get_tasks_k8s()
                if(task_mode == 'random' and tasks_count == 0):
                    if time.time()-self.start >= 300:
                        print("Five minute is up")
                        generate_csv()
                        break
                
                if self.task_over and not tasks and all(device['status'] == 1 for device in self.devices.values()):
                    print(f"All tasks completed, time consuming: {time.time()-self.start:.2f} seconds")
                    generate_csv()
                    break

                if not available_devices or not tasks:
                    time.sleep(1)
                    continue
                i = 0
                allocated = False
                while i < len(tasks):
                    task = tasks[i]
                    required_memory = max(task['memory_JIT'], task['memory_AOT']) * 1024**2
                    max_device = max(available_devices, key=lambda x: x['total_memory'])
                    if max_device.get('total_memory') >= required_memory:
                        self.allocate_task(task, max_device)
                        print(f"allocated successfully: task {task['task_id']} -> devcie {max_device['ip']}")
                        allocated = True
                        break
                    else:
                        print(f"task {task['task_id']} -> device {max_device['ip']} is Out of memory")
                        i += 1
                time.sleep(0.5 if allocated else 1)               

# Get the task template and device list from the database and initialize the state
def fetch_tasks(host, user, password, database):
    with pymysql.connect(
        host='localhost',
        user='root',
        password='xxxxx',
        database='my_database',
        charset='utf8mb4',
        cursorclass=DictCursor
    ) as connection:
        with connection.cursor() as cursor:
            cursor.execute("UPDATE tasks SET created_at = NOW()")
            cursor.execute("SELECT *, UNIX_TIMESTAMP(created_at) as creation_time FROM tasks")
            tasks_list = cursor.fetchall()
        with connection.cursor() as cursor:
            cursor.execute("SELECT * FROM devices")
            devices_list = cursor.fetchall()

    # Processing tasks template (Status set to 0, indicating that it does not participate in scheduling)
    for task in tasks_list:
        max_mem = max(task.get('memory_JIT', 0), task.get('memory_AOT', 0))
        task['memory_level'] = 1 + (max_mem >= 128) + (max_mem >= 256) + (max_mem >= 512) + (max_mem >= 1024)
        task['status'] = 0  

    # Processing devices
    for device in devices_list:
        total_mem_mb = device['total_memory'] / (1024 * 1024)  
        device['memory_level'] = 1 + (total_mem_mb >= 128) + (total_mem_mb >= 256) + (total_mem_mb >= 512) + (total_mem_mb >= 1024)
        device['status'] = 1  

    return tasks_list, devices_list

if __name__ == "__main__":
    task_templates, devices = fetch_tasks(
        host="localhost",
        user="root",
        password="xxxxx",
        database="my_database"
    )
    scheduler = TaskScheduler(
        task_templates={t['task_id']: t for t in task_templates},
        devices={d['ip']: d for d in devices}
    )
    scheduler.run()
