#!/bin/bash
#SBATCH -N 1               # 申请1个节点
#SBATCH -n 4               # 申请4个核
#SBATCH -p thcp1           # 分区名称
#SBATCH --output=parsl_output_slurm_%j.txt    # 标准输出文件
#SBATCH --error=parsl_error_slurm_%j.txt      # 错误输出文件

yhrun python code/multi_node_parsl_slurm.py
