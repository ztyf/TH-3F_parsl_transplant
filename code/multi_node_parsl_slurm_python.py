import parsl
from parsl import bash_app, python_app
from parsl.executors import HighThroughputExecutor
from parsl.launchers import SrunLauncher
from parsl.providers import SlurmProvider
from parsl.usage_tracking.levels import LEVEL_3
from parsl.executors.taskvine import TaskVineExecutor
from parsl.executors.taskvine import TaskVineFactoryConfig
from parsl.executors.taskvine import TaskVineManagerConfig
from parsl import Config
from parsl import File
import os

@python_app
def add_python(a, b):
    import os
    os.system(f'echo $(uname -a) >> tmplog.log')
    return a + b

@python_app
def add_1_python(a):
    import os
    os.system(f'echo $(uname -a) >> tmplog.log')
    return a + 1

def fibonacci_python(n):
    fn1 = add_1_python(n-1).result()
    fn2 = add_1_python(n).result()
    return add_python(fn1, fn2).result()
    

def create_fibonacci_tasks(N):
    tasks = [None] * (N + 1)  # 创建任务列表，初始为 None

    for i in range(0, N):
        # 提交 bash 任务来计算第 i 个 Fibonacci 数
        task = fibonacci_python(i)
        tasks[i] = task

    return tasks



if __name__ == "__main__":
    slurmConfig = Config(
        executors=[
            HighThroughputExecutor(
                label="tianhe",
                # cores_per_worker=4,
                # max_workers=16,
                worker_debug=False,
                provider=SlurmProvider(
                    partition="thcp1", # -p = thcp3
                    # account=None, # 默认不处理
                    # qoe # 默认不处理
                    # channel # 默认不处理
                    nodes_per_block=2, # -N 1
                    cores_per_node=64, # -n 4
                    # mem_per_node 
                    # init_blocks=1, 
                    # min_blocks=1, 
                    # max_blocks=1,
                    # parallelism 
                    # walltime 
                    # scheduler_options
                    # regex_job_id 
                    worker_init='mamba activate deepdrivemd',
                    exclusive=True,
                    launcher=SrunLauncher(),
                    # move_files
                )
            )
        ],
        usage_tracking=LEVEL_3,
    )
    parsl.load(slurmConfig)
    # 保存Fibonacci结果的目录
    os.makedirs('fib_results', exist_ok=True)

    # 设置计算的最大Fibonacci数列项
    n = 5  # 示例计算到第2^n-1项
    N = 2**n
    tasks = create_fibonacci_tasks(N)

    # 等待所有任务完成
    results = [task for task in tasks]

    with open("fibonacci_results.txt", "w") as f:
        for i in range(N - 1):
            f.write(f"Fibonacci({i}) = {results[i]}")
            f.write("\n")

    # 在parsl_output_JOBID.txt中打印结果
    print("所有 Fibonacci 结果已保存到 fibonacci_results.txt")

    parsl.dfk().cleanup()
    parsl.clear()



# '''
# #上面是计算Fibonacci数列的代码,测试了单节点的情况,下面并行计算结果的和，测试多节点的情况

# 对结果进行并行计算，得到第0~N-1的N个斐波那契数分为M个子任务，每个任务计算N/M个斐波那契数，并把结果
# 保存到另一个文件夹中，最后再求和print出来，这些子任务用脚本来启动，使得在不同节点上运行，以体现多节点分布流的效果
# '''

# m=4  # 分2^m个任务
# M=2**m # 任务数

# # 创建保存部分和的目录
# os.makedirs('fib_partial_sums', exist_ok=True)

# chunk_size = N // M  # 每个子任务的Fibonacci数项数

# # 生成脚本并启动多节点求和任务
# for i in range(M):
#     start_idx = i * chunk_size
#     end_idx = (i + 1) * chunk_size
#     os.system(f'yhbatch scripts/run_sub.sh {start_idx} {end_idx}')

# print("任务提交完毕。等待所有子任务完成以计算最终结果。")


# import time

# # 等待所有子任务完成
# def wait_for_completion(folder='fib_partial_sums', expected_files=M):
#     while True:
#         files = os.listdir(folder)
#         if len(files) >= expected_files:
#             break
#         print(f"等待 {expected_files} 个文件生成，目前已发现 {len(files)} 个文件。")
#         time.sleep(30)  # 每隔30秒检查一次

# wait_for_completion()

# # 汇总部分和
# total_sum = 0
# for i in range(M):
#     partial_sum_file = f"fib_partial_sums/partial_sum_{i * chunk_size}_to_{(i + 1) * chunk_size}.txt"
#     with open(partial_sum_file, "r") as f:
#         partial_sum = int(f.read().strip())
#         total_sum += partial_sum

# # 打印最终结果
# print(f"所有部分和的总和是: {total_sum}")

# # 保存最终结果到文件
# with open("total_fibonacci_sum.txt", "w") as f:
#     f.write(f"所有部分和的总和是: {total_sum}")