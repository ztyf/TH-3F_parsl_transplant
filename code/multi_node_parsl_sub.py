import sys
import os

def compute_partial_sum(start_idx, end_idx):
    partial_sum = 0
    for i in range(start_idx, end_idx):
        with open(f"fib_results/fib_{i}.txt", "r") as f:
            fib_value = int(f.read().strip())
            partial_sum += fib_value
    
    # 将部分和保存到文件
    partial_sum_file = f"fib_partial_sums/partial_sum_{start_idx}_to_{end_idx}.txt"
    with open(partial_sum_file, "w") as f:
        f.write(str(partial_sum))

    return partial_sum

if __name__ == "__main__":
    # 从命令行获取 start_idx 和 end_idx
    start_idx = int(sys.argv[1])
    end_idx = int(sys.argv[2])

    # 计算部分和
    compute_partial_sum(start_idx, end_idx)
    print(f"部分和从 {start_idx} 到 {end_idx} 已保存")
