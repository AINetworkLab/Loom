import pexpect
import libvirt
import os
import csv
import argparse
def list_running_vms():
    conn = libvirt.open('qemu:///system')  # Connect to the local libvirt daemon
    if conn is None:
        print('Failed to open connection to qemu:///system')
        return

    domains = conn.listAllDomains()  # Gets a list of all domains (virtual machines)
    if len(domains) == 0:
        print('No running domains')
    else:
        print('Running domains:')
        for domain in domains:
            print('  Name: {}'.format(domain.name()))
            print('  ID: {}'.format(domain.ID()))
            print('  State: {}'.format(domain.state()))
            print('')

    conn.close()  

# Sends commands to the specified virtual machine
def send_command_to_vm(vm_name, command):
    try:
        child = pexpect.spawn(f'virsh console {vm_name}')
        child.expect('Escape character is', timeout=10)
        child.sendline(command)
        child.expect(['#', pexpect.EOF], timeout=60)  
        print(child.before.decode('utf-8'))
        child.sendline('~.')  
        child.close()
    except pexpect.exceptions.ExceptionPexpect as e:
        print(f"Error while interacting with VM console: {e}")

# returns a list of dictionaries containing IP, MAC, and schema information
def read_hosts(file_path):
    data = []
    with open(file_path, 'r', newline='') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            data.append({'IP': row['IP'], 'MAC': row['MAC'], 'Architecture': row['Architecture']})
    return data



if __name__ == '__main__':

            source_dir = os.path.join("JIT", 'fft')
            file_name = os.path.basename('fft')
            aot_output_dir = os.path.abspath(
                os.path.join("file/AOT", 'fft')
            )

            os.makedirs(aot_output_dir, exist_ok=True)
            # Construct compile command
            compile_cmd = (
                f"wasmtime compile /root/work/JIT/{file_name}/{file_name}.wasm "
                f"-o /root/work/AOT/{file_name}/{file_name}.cwasm"
            )
            vm_name = (f"ubuntu22.04-aarch64")
            # execute compile command
            send_command_to_vm(vm_name, compile_cmd)
            source_dir = aot_output_dir

