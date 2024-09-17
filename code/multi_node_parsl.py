import parsl
from parsl import bash_app, python_app
from parsl.executors import HighThroughputExecutor
from parsl.providers import LocalProvider
from parsl import Config
import os
from parsl import File

parsl.load() 


# 保存Fibonacci结果的目录
os.makedirs('fib_results', exist_ok=True)

# 第0和第1项Fibonacci并保存
with open("fib_results/fib_0.txt", "w") as f0:
    f0.write("0\n")
with open("fib_results/fib_1.txt", "w") as f1:
    f1.write("1\n")

# 计算Fibonacci并保存到文件
@bash_app
def fibonacci_bash(n, inputs=[], outputs=[]):
    return f"""
    fib_n_1=$(cat {inputs[0]})  # 读取第n-1个Fibonacci数
    fib_n_2=$(cat {inputs[1]})  # 读取第n-2个Fibonacci数
    fib_n=$((fib_n_1 + fib_n_2))  # 计算第n个Fibonacci数
    echo $fib_n > {outputs[0]}  # 保存结果到文件
    """


def create_fibonacci_tasks(N):
    tasks = [None] * (N + 1)  # 创建任务列表，初始为 None

    # 第 0 和第 1 项的任务已经完成，无需计算
    tasks[0] = "completed"
    tasks[1] = "completed"

    for i in range(2, N):
        input_file_n_1 = File(f"fib_results/fib_{i-1}.txt")
        input_file_n_2 = File(f"fib_results/fib_{i-2}.txt")
        output_file = File(f"fib_results/fib_{i}.txt")

        # 提交 bash 任务来计算第 i 个 Fibonacci 数
        task = fibonacci_bash(i, inputs=[input_file_n_1, input_file_n_2], outputs=[output_file])
        tasks[i] = task

        # 确保第 n-1 和第 n-2 个任务完成
        if tasks[i-1] != "completed":
            tasks[i-1].result()
            tasks[i-1] = "completed"  # 标记第 n-1 项任务为已完成
        if tasks[i-2] != "completed":
            tasks[i-2].result()
            tasks[i-2] = "completed"  # 标记第 n-2 项任务为已完成

    return tasks


# 设置计算的最大Fibonacci数列项
n = 12  # 示例计算到第2^n-1项
N = 2**n
tasks = create_fibonacci_tasks(N)

# 等待所有任务完成
[task.result() for task in tasks if task not in (None, "completed")]


# 汇总结果并打印到fibonacci_results.txt
results = []
for i in range(N - 1):
    with open(f"fib_results/fib_{i}.txt", "r") as f:
        results.append(f"Fibonacci({i}) = {f.read().strip()}")

with open("fibonacci_results.txt", "w") as f:
    f.write("\n".join(results))

# 在parsl_output_JOBID.txt中打印结果
print("所有 Fibonacci 结果已保存到 fibonacci_results.txt")



'''
#上面是计算Fibonacci数列的代码,测试了单节点的情况,下面并行计算结果的和，测试多节点的情况

对结果进行并行计算，得到第0~N-1的N个斐波那契数分为M个子任务，每个任务计算N/M个斐波那契数，并把结果
保存到另一个文件夹中，最后再求和print出来，这些子任务用脚本来启动，使得在不同节点上运行，以体现多节点分布流的效果
'''

m=4  # 分2^m个任务
M=2**m # 任务数

# 创建保存部分和的目录
os.makedirs('fib_partial_sums', exist_ok=True)

chunk_size = N // M  # 每个子任务的Fibonacci数项数

# 生成脚本并启动多节点求和任务
for i in range(M):
    start_idx = i * chunk_size
    end_idx = (i + 1) * chunk_size
    os.system(f'yhbatch scripts/run_sub.sh {start_idx} {end_idx}')

print("任务提交完毕。等待所有子任务完成以计算最终结果。")


import time

# 等待所有子任务完成
def wait_for_completion(folder='fib_partial_sums', expected_files=M):
    while True:
        files = os.listdir(folder)
        if len(files) >= expected_files:
            break
        print(f"等待 {expected_files} 个文件生成，目前已发现 {len(files)} 个文件。")
        time.sleep(30)  # 每隔30秒检查一次

wait_for_completion()

# 汇总部分和
total_sum = 0
for i in range(M):
    partial_sum_file = f"fib_partial_sums/partial_sum_{i * chunk_size}_to_{(i + 1) * chunk_size}.txt"
    with open(partial_sum_file, "r") as f:
        partial_sum = int(f.read().strip())
        total_sum += partial_sum

# 打印最终结果
print(f"所有部分和的总和是: {total_sum}")

# 保存最终结果到文件
with open("total_fibonacci_sum.txt", "w") as f:
    f.write(f"所有部分和的总和是: {total_sum}")
