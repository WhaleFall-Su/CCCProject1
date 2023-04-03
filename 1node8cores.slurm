#!/bin/bash
#SBATCH --nodes=1
#SBATCH --ntasks=8
#SBATCH --time=0-00:10:00

module load foss/2019b
module load python/3.9.5
module load mpi4py/3.0.2-timed-pingpong

echo =====1 node and 8 core=====
time srun -n 8  python3 A1MPI.py

my-job-stats -a -n -s