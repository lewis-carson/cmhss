#!/usr/bin/env python3
import os
import json
import requests
import time
import gzip
from pathlib import Path

BASE_URL = "https://data-api.polymarket.com/trades"
OUTPUT_DIR = "trades"
EVENTS_DIR = "events"
NO_DATA_FILE = os.path.join(OUTPUT_DIR, "no_data.txt")
SUBDIRECTORY_MOD = 1000

# Ensure base trades directory exists
Path(OUTPUT_DIR).mkdir(exist_ok=True)

def get_subdirectory_for_market(market_id):
    """Get the subdirectory path for a given market ID (conditionId) using last three digits of its integer representation."""
    try:
        # market_id is a hex string (conditionId)
        market_int = int(market_id, 16)
        subdir_idx = market_int % SUBDIRECTORY_MOD
        subdir = os.path.join(OUTPUT_DIR, f"trades_{subdir_idx:03d}")
        Path(subdir).mkdir(exist_ok=True)
        return subdir
    except ValueError:
        subdir = os.path.join(OUTPUT_DIR, "trades_misc")
        Path(subdir).mkdir(exist_ok=True)
        return subdir

def get_all_markets_with_volume():
    """Load all markets from events files and return a list of (conditionId, volume)."""
    markets = []
    if not os.path.exists(EVENTS_DIR):
        print(f"Events directory {EVENTS_DIR} does not exist.")
        return markets

    event_files = sorted([f for f in os.listdir(EVENTS_DIR) if f.startswith("events_") and f.endswith(".json")])
    
    for event_file in event_files:
        with open(os.path.join(EVENTS_DIR, event_file), 'r') as f:
            try:
                events = json.load(f)
                for event in events:
                    if 'markets' in event:
                        for market in event['markets']:
                            condition_id = market.get('conditionId')
                            volume = market.get('volumeNum', 0)
                            if condition_id:
                                markets.append({'conditionId': condition_id, 'volume': volume})
            except json.JSONDecodeError:
                print(f"Error reading {event_file}")
                continue
    return markets

def filter_top_markets(markets, percentile=0.10):
    """Filter markets to keep only the top percentile by volume."""
    if not markets:
        return []
    sorted_markets = sorted(markets, key=lambda x: x['volume'], reverse=True)
    count = len(sorted_markets)
    cutoff_index = int(count * percentile)
    # Ensure at least one market if there are markets
    if cutoff_index == 0 and count > 0:
        cutoff_index = 1
    return sorted_markets[:cutoff_index]

def get_already_processed_markets():
    """Get all markets that have already been processed (downloaded or marked as no data)"""
    processed = set()
    
    if os.path.exists(OUTPUT_DIR):
        for item in os.listdir(OUTPUT_DIR):
            item_path = os.path.join(OUTPUT_DIR, item)
            if os.path.isdir(item_path) and item.startswith("trades_"):
                for filename in os.listdir(item_path):
                    if filename.startswith("trades_") and (filename.endswith(".json") or filename.endswith(".json.gz")):
                        market_id = filename.replace("trades_", "").replace(".json.gz", "").replace(".json", "")
                        processed.add(market_id)
    
    if os.path.exists(NO_DATA_FILE):
        with open(NO_DATA_FILE, 'r') as f:
            for line in f:
                processed.add(line.strip())
                
    return processed

def download_and_save_trades(condition_id):
    """Download trades for a specific market (conditionId) and save to file incrementally."""
    subdir = get_subdirectory_for_market(condition_id)
    filename = os.path.join(subdir, f"trades_{condition_id}.json.gz")
    
    offset = 0
    limit = 500
    backoff = 1
    first_trade = True
    total_trades = 0
    
    try:
        with gzip.open(filename, 'wt', encoding='utf-8') as f:
            f.write('[\n')
            
            while True:
                params = {
                    "limit": limit,
                    "offset": offset,
                    "takerOnly": "true",
                    "market": condition_id,
                    "filterType": "CASH",
                    "filterAmount": 10000
                }
                
                try:
                    response = requests.get(BASE_URL, params=params)
                    
                    if response.status_code == 429:
                        print(f"Rate limited. Waiting {backoff} seconds...")
                        time.sleep(backoff)
                        backoff = min(backoff * 2, 60)
                        continue
                    
                    response.raise_for_status()
                    backoff = 1 # Reset backoff on success
                    
                    trades = response.json()
                    
                    if not trades:
                        break
                        
                    for trade in trades:
                        if not first_trade:
                            f.write(',\n')
                        json.dump(trade, f, indent=2)
                        first_trade = False
                        total_trades += 1
                        
                        if total_trades % 10000 == 0:
                            print(f"Downloaded {total_trades} trades for {condition_id}...")
                    
                    if len(trades) < limit:
                        break
                        
                    offset += limit
                    time.sleep(10/75) # Small delay to be nice
                    
                except requests.exceptions.RequestException as e:
                    print(f"Error downloading trades for {condition_id}: {e}")
                    return None # Indicate failure
            
            f.write('\n]')
            
    except IOError as e:
        print(f"Error writing to file {filename}: {e}")
        return None

    return total_trades

def main():
    print("Gathering markets...")
    all_markets = get_all_markets_with_volume()
    print(f"Found {len(all_markets)} markets.")
    
    top_markets = filter_top_markets(all_markets, percentile=0.10)
    print(f"Filtered to top {len(top_markets)} markets (top 10% by volume).")
    
    processed_markets = get_already_processed_markets()
    print(f"Found {len(processed_markets)} already processed markets.")
    
    markets_to_process = [m for m in top_markets if m['conditionId'] not in processed_markets]
    print(f"Remaining markets to process: {len(markets_to_process)}")
    
    for i, market in enumerate(markets_to_process):
        condition_id = market['conditionId']
        print(f"[{i+1}/{len(markets_to_process)}] Downloading trades for {condition_id} (Volume: {market['volume']})...")
        
        count = download_and_save_trades(condition_id)
        
        if count is not None:
            print(f"Saved {count} trades.")
        else:
            print("Failed to download trades. Skipping...")

if __name__ == "__main__":
    main()
