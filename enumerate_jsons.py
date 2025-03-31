#!/usr/bin/env python3
import os
import json
import argparse
from collections import defaultdict
from pathlib import Path
import jsonlines


def enumerate_jsons(root_dir, batch_size=10000, output_dir=None):
    """
    Enumerate all JSON files in the given directory, chunk them by post ID modulo batch_size,
    and save them to JSONL files.
    
    Args:
        root_dir (str): Root directory containing post_X folders
        batch_size (int): Number of posts per batch (default: 10000)
        output_dir (str): Directory to save JSONL files (default: same as root_dir)
    """
    if output_dir is None:
        output_dir = root_dir
    
    # Create output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)
    
    # Dictionary to store batches
    batches = defaultdict(dict)
    
    # Iterate through all directories in root_dir
    root_path = Path(root_dir)
    
    # Count total folders for progress reporting
    total_folders = sum(1 for item in root_path.iterdir() if item.is_dir() and item.name.startswith('post_'))
    processed = 0
    
    print(f"Found {total_folders} post folders")
    
    for post_dir in root_path.iterdir():
        # Skip if not a directory or not starting with 'post_'
        if not post_dir.is_dir() or not post_dir.name.startswith('post_'):
            continue
        
        try:
            # Extract post ID from directory name
            post_id = int(post_dir.name.split('_')[1])
            
            # Calculate batch number
            batch_num = post_id // batch_size
            
            # Find JSON file in the post directory
            json_files = list(post_dir.glob('*.json'))
            if not json_files:
                continue
            
            # Read JSON content
            json_path = json_files[0]
            with open(json_path, 'r', encoding='utf-8') as f:
                json_content = json.load(f)
            
            # Add to batch with key as filename and value as content
            batches[batch_num][json_path.name] = json_content
            
            # Update progress
            processed += 1
            if processed % 1000 == 0:
                print(f"Processed {processed}/{total_folders} folders ({processed / total_folders * 100:.2f}%)")
        
        except (ValueError, IndexError, json.JSONDecodeError) as e:
            print(f"Error processing {post_dir}: {e}")
    
    # Save batches to JSONL files
    print(f"Saving {len(batches)} batch files...")
    for batch_num, batch_data in batches.items():
        output_file = os.path.join(output_dir, f'batch_{batch_num}.jsonl')
        with jsonlines.open(output_file, mode='w') as writer:
            # Each line is a dict with one key (filename) and value (content)
            for filename, content in batch_data.items():
                writer.write({filename: content})
        
        print(f"Saved batch {batch_num} with {len(batch_data)} entries to {output_file}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Enumerate JSON files and save to batched JSONL files')
    parser.add_argument('root_dir', help='Root directory containing post_X folders')
    parser.add_argument('--batch-size', type=int, default=10000, help='Number of posts per batch (default: 10000)')
    parser.add_argument('--output-dir', help='Directory to save JSONL files (default: same as root_dir)')
    
    args = parser.parse_args()
    
    enumerate_jsons(args.root_dir, args.batch_size, args.output_dir)