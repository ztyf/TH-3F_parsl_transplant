import parsl
from parsl import bash_app, python_app
from parsl.executors import HighThroughputExecutor
from parsl.launchers import SrunLauncher
from parsl.providers import SlurmProvider
from parsl.usage_tracking.levels import LEVEL_3
from parsl import Config
from parsl import File
import os


# 计算Fibonacci并保存到文件
@bash_app
def fibonacci_bash(n, inputs=[], outputs=[]):
    return f"""
    echo $(uname -a) >> tmplog.log
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



if __name__ == "__main__":
    os.system("echo $(uname -a) >> tmp.log2")
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
                    nodes_per_block=1, # -N 1
                    cores_per_node=4, # -n 4
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

    # 第0和第1项Fibonacci并保存
    with open("fib_results/fib_0.txt", "w") as f0:
        f0.write("0\n")
    with open("fib_results/fib_1.txt", "w") as f1:
        f1.write("1\n")


    # 设置计算的最大Fibonacci数列项
    n = 5  # 示例计算到第2^n-1项
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

    parsl.dfk().cleanup()
    parsl.clear()