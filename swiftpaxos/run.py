import subprocess
import threading
import time

# --- 配置 ---
# 根据你的 local.conf 解析或手动定义这些节点信息
# 格式: (alias, host_ip)
# 假设你的 local.conf 定义了这些别名和对应的IP
master_nodes = [
    ("master", "57.182.103.74") # 示例IP，请替换为你的实际Master IP
]
replica_nodes = [ # 注意：这些现在是 "-run server"
    ("replica1", "18.181.68.30"), # 示例IP，请替换
    ("replica2", "3.115.96.150"), # 示例IP，请替换
    ("replica3", "54.178.167.57")  # 示例IP，请替换
]
client_nodes = [
    ("client1", "54.95.19.6")  # 示例IP，请替换
]

# 远程主机上 swiftpaxos 相关文件的路径
# 确保这个路径在所有远程服务器上都是正确的，并且 swiftpaxos 在此路径下
REMOTE_SWIFΤPAXOS_EXECUTABLE = "./share/swiftpaxos/swiftpaxos" # 如果它就在用户的 home 目录或其他已知相对路径
REMOTE_CONFIG_FILE = "./share/swiftpaxos/local.conf"            # 同样，相对于 swiftpaxos 可执行文件的路径或绝对路径


def execute_ssh_and_background_no_log(host, swiftpaxos_base_command_args, node_alias, role_name):

    remote_shell_command = f"nohup {' '.join(swiftpaxos_base_command_args)} > /dev/null 2>&1 &"

    # 构建将由本地 Python 脚本运行的完整 SSH 命令参数列表
    full_ssh_command_args = [
        'ssh',
        '-o', 'StrictHostKeyChecking=no',
        '-o', 'BatchMode=yes',
        '-o', 'ConnectTimeout=10',
        host,
        remote_shell_command
    ]

    print(f"  [Thread for {node_alias} ({role_name}) on {host}] Attempting to launch: {remote_shell_command}")

    process = subprocess.run(full_ssh_command_args, capture_output=True, text=True, check=False, timeout=30)


def launch_node_group_async_no_log(nodes_to_launch, role_name_for_run_arg):
    """
    并行启动一组节点。每个节点的命令都在远程主机后台运行，输出被丢弃。
    此函数会等待本组所有 SSH 命令发送完毕后才返回。
    """
    threads = []
    # 使用 role_name_for_run_arg 来决定显示的是 "SERVER" (for replicas) 还是 "MASTER"/"CLIENT"
    display_role_name = "SERVER" if role_name_for_run_arg == "server" else role_name_for_run_arg.upper()
    print(f"\n--- Initiating launch for {display_role_name} nodes ---")

    for alias, host_ip in nodes_to_launch:
        # 构建 swiftpaxos 命令的参数列表
        # 确保路径是相对于用户在远程 SSH 登录后的当前目录，
        # 或者使用绝对路径。
        core_swiftpaxos_command_args = [
            REMOTE_SWIFΤPAXOS_EXECUTABLE,
            "-run", role_name_for_run_arg,
            "-config", REMOTE_CONFIG_FILE,
            "-alias", alias,
            "-log", f"./share/swiftpaxos/logs/{alias}.log"
        ]

        thread = threading.Thread(target=execute_ssh_and_background_no_log,
                                 args=(host_ip, core_swiftpaxos_command_args, alias, display_role_name))
        threads.append(thread)
        thread.start()

    for thread in threads:
        thread.join()

    print(f"--- All SSH launch commands for {display_role_name} group sent. ---")
    print(f"--- Since output is discarded, verify process status on remote hosts (e.g., using 'ps aux | grep swiftpaxos'). ---")

# --- 主执行流程 ---

# 1. 启动 Master(s)
# `role_name_for_run_arg` 必须是 "master"
launch_node_group_async_no_log(master_nodes, "master")
print("Master launch sequence initiated. Pausing before starting servers (replicas)...")
MASTER_INIT_DELAY = 1
time.sleep(MASTER_INIT_DELAY)

# 2. 启动 Servers (Replicas)
# `role_name_for_run_arg` 必须是 "server"
launch_node_group_async_no_log(replica_nodes, "server") # 注意这里的 "server"
print("Server (Replica) launch sequence initiated. Pausing before starting clients...")
SERVER_INIT_DELAY = 1
time.sleep(SERVER_INIT_DELAY)

# 3. 启动 Client(s)
# `role_name_for_run_arg` 必须是 "client"
launch_node_group_async_no_log(client_nodes, "client")
print("Client launch sequence initiated.")

print("\nAll node launch sequences have been initiated.")