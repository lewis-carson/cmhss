#!/usr/bin/env python3
import os
import json
import requests
from pathlib import Path

BASE_URL = "https://gamma-api.polymarket.com/events"
LIMIT = 100
OUTPUT_DIR = "events"

# Create data directory if it doesn't exist
Path(OUTPUT_DIR).mkdir(exist_ok=True)

# Infer state from existing files
def get_current_state():
    """Infer offset and file index from existing files in data/"""
    existing_files = [f for f in os.listdir(OUTPUT_DIR) if f.startswith("events_") and f.endswith(".json")]
    
    if not existing_files:
        return 0, 0
    
    # Sort files and get the highest index
    file_indices = [int(f.replace("events_", "").replace(".json", "")) for f in existing_files]
    max_index = max(file_indices)
    
    # The next file index is max_index + 1, and offset is based on LIMIT
    next_file_index = max_index + 1
    offset = next_file_index * LIMIT
    
    print(f"Found {len(existing_files)} existing files. Resuming from offset {offset}, file index {next_file_index}")
    return offset, next_file_index

offset, file_index = get_current_state()

while True:
    params = {
        "closed": "true",
        "limit": LIMIT,
        "offset": offset
    }
    
    try:
        print(f"Downloading offset {offset}...")
        response = requests.get(BASE_URL, params=params)
        response.raise_for_status()
        
        data = response.json()
        
        # Check if we got any results
        if not data or len(data) == 0:
            print("No more data to download.")
            break
        
        # Save to file
        output_file = os.path.join(OUTPUT_DIR, f"events_{file_index:04d}.json")
        with open(output_file, "w") as f:
            json.dump(data, f, indent=2)
        
        print(f"Saved {len(data)} events to {output_file}")
        
        # Update counters for next iteration
        offset += LIMIT
        file_index += 1
        
        # If we got fewer results than the limit, we've reached the end
        if len(data) < LIMIT:
            print("Reached end of data.")
            break
        
    except requests.exceptions.RequestException as e:
        print(f"Error downloading data: {e}")
        print("Will resume from this point on next run")
        break

print("Download complete!")
