#!/bin/bash
#SBATCH -N 1 -n 1 -p thcp1
yhrun python code/parsl_test.py
