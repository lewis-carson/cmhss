#!/bin/bash
#SBATCH --job-name=download_trades
#SBATCH --output=download_trades.log
#SBATCH --error=download_trades.err
#SBATCH --time=08:00:00
#SBATCH --cpus-per-task=1
#SBATCH --mem=8GB
#SBATCH --partition=shared

python3 -u download_trades.py
