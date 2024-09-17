#!/bin/bash
#SBATCH -N 1               # 申请单节点单核
#SBATCH -n 1
#SBATCH -p thcp1            # 分区
#SBATCH --output=parsl_output_%j.txt    # 标准输出文件
#SBATCH --error=parsl_error_%j.txt      # 错误输出文件


# 运行Parsl工作流脚本
yhrun python code/multi_node_parsl.py
