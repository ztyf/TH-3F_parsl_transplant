#!/bin/bash
#SBATCH -N 1               # 申请1个节点
#SBATCH -n 32              # 每个节点使用32核
#SBATCH -p thcp1           # 分区

# 动态生成输出和错误文件名
OUTPUT_FILE="parsl_sub_output_${1}_${2}.txt"
ERROR_FILE="parsl_sub_error_${1}_${2}.txt"

#SBATCH --output=$OUTPUT_FILE   # 标准输出文件
#SBATCH --error=$ERROR_FILE     # 错误输出文件

# 运行Parsl工作流脚本
yhrun python code/multi_node_parsl_sub.py $1 $2
