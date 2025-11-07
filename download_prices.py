#!/usr/bin/env python3
import os
import json
import requests
import time
from pathlib import Path
from datetime import datetime, timezone, timedelta

BASE_URL = "https://clob.polymarket.com/prices-history"
OUTPUT_DIR = "prices"
EVENTS_DIR = "events"
NO_DATA_FILE = os.path.join(OUTPUT_DIR, "no_data.txt")
INITIAL_BACKOFF = 1  # Start with 1 second
MAX_BACKOFF = 60  # Cap at 60 seconds
BACKOFF_MULTIPLIER = 2
SUBDIRECTORY_MOD = 1000  # Bucket markets based on their last three digits
BUFFER_MINUTES = 72 * 60  # Start downloads 72 hours before the market goes live

# Ensure base prices directory exists
Path(OUTPUT_DIR).mkdir(exist_ok=True)

def parse_iso_datetime(value):
    """Parse ISO timestamp strings into timezone-aware UTC datetimes."""
    if not value:
        return None
    try:
        normalized = value.replace('Z', '+00:00') if value.endswith('Z') else value
        dt = datetime.fromisoformat(normalized)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        else:
            dt = dt.astimezone(timezone.utc)
        return dt
    except ValueError:
        return None

def compute_start_timestamp(market_data):
    """Determine the startTs parameter (seconds) by subtracting BUFFER_MINUTES."""
    field = market_data.get('endDate')

    dt = parse_iso_datetime(field)
    if dt:
        start_ts = int(dt.timestamp()) - BUFFER_MINUTES * 60
        return max(start_ts, 0)
    fallback = datetime.now(timezone.utc) - timedelta(minutes=BUFFER_MINUTES)
    return int(fallback.timestamp())

def get_subdirectory_for_market(market_id):
    """Get the subdirectory path for a given market ID using last three digits."""
    last_three_digits = int(market_id) % SUBDIRECTORY_MOD
    subdir = os.path.join(OUTPUT_DIR, f"prices_{last_three_digits:03d}")
    Path(subdir).mkdir(exist_ok=True)
    return subdir

def get_all_markets():
    """Load all markets from events files and return a dict of market CLOB token ID -> market data"""
    markets = {}
    
    # List all event files
    event_files = sorted([f for f in os.listdir(EVENTS_DIR) if f.startswith("events_") and f.endswith(".json")])
    
    for event_file in event_files:
        with open(os.path.join(EVENTS_DIR, event_file), 'r') as f:
            events = json.load(f)
            for event in events:
                if 'markets' in event:
                    for market in event['markets']:
                        clob_token_ids = market.get('clobTokenIds')
                        if clob_token_ids:
                            # clobTokenIds is a JSON string array, parse it
                            try:
                                token_ids = json.loads(clob_token_ids)
                                # Create an entry for each token ID
                                if token_ids:
                                    for token_id in token_ids:
                                        markets[token_id] = market
                            except (json.JSONDecodeError, TypeError):
                                pass
    
    return markets

def get_already_processed_markets():
    """Get all markets that have already been processed (downloaded or marked as no data)"""
    processed = set()
    
    # Add markets from prices/ subdirectories (and legacy files in the root)
    if os.path.exists(OUTPUT_DIR):
        for item in os.listdir(OUTPUT_DIR):
            item_path = os.path.join(OUTPUT_DIR, item)
            if os.path.isdir(item_path) and item.startswith("prices_"):
                for filename in os.listdir(item_path):
                    if filename.startswith("prices_") and filename.endswith(".json"):
                        market_id = filename.replace("prices_", "").replace(".json", "")
                        processed.add(market_id)
            elif item.startswith("prices_") and item.endswith(".json"):
                market_id = item.replace("prices_", "").replace(".json", "")
                processed.add(market_id)
    
    # Add markets from no_data.txt
    if os.path.exists(NO_DATA_FILE):
        with open(NO_DATA_FILE, 'r') as f:
            for line in f:
                market_id = line.strip()
                if market_id:
                    processed.add(market_id)
    
    return processed

def get_start_market_index(markets, downloaded_markets):
    """Find the index to resume from based on the latest downloaded market"""
    if not downloaded_markets:
        return 0
    
    market_list = list(markets.keys())
    
    # Find the latest market that was downloaded
    latest_downloaded = None
    latest_index = -1
    
    for market_id in downloaded_markets:
        if market_id in market_list:
            index = market_list.index(market_id)
            if index > latest_index:
                latest_index = index
                latest_downloaded = market_id
    
    if latest_downloaded is not None:
        return latest_index + 1
    
    return 0

def load_no_data_markets():
    """Load the set of markets that have no price history data"""
    if not os.path.exists(NO_DATA_FILE):
        return set()
    
    with open(NO_DATA_FILE, 'r') as f:
        return set(line.strip() for line in f if line.strip())

def save_no_data_market(market_id):
    """Add a market to the no_data list"""
    with open(NO_DATA_FILE, 'a') as f:
        f.write(market_id + '\n')

def download_prices_for_market(market_id, market_data):
    """Download all price history for a specific market"""
    total_downloaded = 0
    backoff = INITIAL_BACKOFF
    
    # Get the appropriate subdirectory for this market
    subdir = get_subdirectory_for_market(market_id)
    output_file = os.path.join(subdir, f"prices_{market_id}.json")

    start_ts = compute_start_timestamp(market_data)

    params = {
        "market": int(market_id),
        "fidelity": 1,
        "startTs": start_ts
    }
    
    try:
        print(f"  Downloading price history for {market_id}...")
        response = requests.get(BASE_URL, params=params)
        response.raise_for_status()
        
        # Reset backoff on successful request
        backoff = INITIAL_BACKOFF
        
        data = response.json()
        
        # Check if we got any results
        if not data or not data.get('history') or len(data.get('history', [])) == 0:
            print(f"  No price history found for market {market_id}")
            save_no_data_market(market_id)
        else:
            # Write data to file
            with open(output_file, "w") as f:
                json.dump(data, f, indent=2)
            
            total_downloaded = len(data.get('history', []))
            print(f"  Retrieved {total_downloaded} price points")
            print(f"  Saved to {output_file}")
        
    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 429:  # Too Many Requests
            print(f"  Rate limited. Backing off for {backoff} seconds...")
            time.sleep(backoff)
            backoff = min(backoff * BACKOFF_MULTIPLIER, MAX_BACKOFF)
            # Recursively retry
            return download_prices_for_market(market_id, market_data)
        else:
            print(f"  HTTP Error downloading price history for {market_id}: {e}")
    except requests.exceptions.RequestException as e:
        print(f"  Error downloading price history for {market_id}: {e}")
    
    return total_downloaded

print("Loading all markets from events files...")
markets = get_all_markets()
print(f"Found {len(markets)} markets")

print("\nChecking already processed markets...")
already_processed = get_already_processed_markets()
print(f"Already processed: {len(already_processed)} markets")

print("\nDownloading price history for all markets...")
for i, (market_id, market_data) in enumerate(markets.items(), 1):
    # Skip markets that have already been processed
    if market_id in already_processed:
        print(f"[{i}/{len(markets)}] Market: {market_id} (SKIPPED - already processed)")
        continue
    
    print(f"\n[{i}/{len(markets)}] Market: {market_id}")
    download_prices_for_market(market_id, market_data)

print("\n\nDownload complete!")
