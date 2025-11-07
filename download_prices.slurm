#!/bin/bash
#SBATCH --job-name=download_prices
#SBATCH --output=download_prices.log
#SBATCH --error=download_prices.err
#SBATCH --time=12:00:00
#SBATCH --cpus-per-task=1
#SBATCH --mem=8GB
#SBATCH --partition=shared

python3.11 -u download_prices.py
