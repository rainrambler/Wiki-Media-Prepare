import json
from typing import Iterator, List, Dict, Any
import argparse
import traceback
import os
import sys
import time

class JSONLParser:
    def __init__(self, file_path: str):
        self.file_path = file_path
    
    def parse(self) -> List[Dict[str, Any]]:
        """Parse entire file into list"""
        return list(self.iter_parse())
    
    def iter_parse(self) -> Iterator[Dict[str, Any]]:
        """Iterate through records one by one"""
        with open(self.file_path, 'r', encoding='utf-8') as file:
            for line_num, line in enumerate(file, 1):
                line = line.strip()
                if not line:
                    continue
                try:
                    yield json.loads(line)
                except json.JSONDecodeError:
                    print(f"Skipping invalid JSON on line {line_num}")
                    continue
    
    def filter_parse(self, condition) -> List[Dict[str, Any]]:
        """Parse with filtering"""
        results = []
        for record in self.iter_parse():
            if condition(record):
                results.append(record)
        return results

# Generator function for large files
def jsonl_generator(file_path):
    with open(file_path, 'r', encoding='utf-8') as file:
        for line in file:
            line = line.strip()
            if line:
                yield json.loads(line)

def llm_usage():
    # Usage
    parser = JSONLParser('../wikipedia-cn-20230720-filtered.jsonl')

    # Get all data
    all_data = parser.parse()

    cur_id = 1
    
    # Process incrementally
    for record in parser.iter_parse():
        file_name = f"wiki_{cur_id}.txt"
        with open("WikiMDs/" + file_name, 'w', encoding='utf-8') as f:
            f.write(record["completion"])
        if cur_id >= 10:
            break
        cur_id = cur_id + 1

def main():
    cur_id = 100
    
    for record in jsonl_generator('../wikipedia-cn-20230720-filtered.jsonl'):
        file_name = f"WikiMDs/wiki_{cur_id}.txt"
        with open(file_name, 'w', encoding='utf-8') as f:
            f.write(record["completion"])

        cur_id = cur_id + 1

    parser = argparse.ArgumentParser(
        description="JSONL Parser",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Usage:
  %(prog)s wiki.jsonl --output ./txts         # Specified output dir
        """
    )
    
    parser.add_argument("input", help="Jsonl file path")
    parser.add_argument("-o", "--output", help="Output directory")
    
    args = parser.parse_args()
    
    try:        
        cur_id = 100
        input_path=args.input
        output_dir=args.output
        print(f"Input dir: {input_path}")
        print(f"Save to dir: {output_dir}")
        
        start = time.perf_counter()
        
        for record in jsonl_generator(input_path):
            file_name = os.path.join(output_dir, f"wiki_{cur_id}.txt")
            with open(file_name, 'w', encoding='utf-8') as f:
                f.write(record["completion"])

            cur_id = cur_id + 1
        
        end = time.perf_counter()
        elapsed_time = end - start
        print(f"Elapsed time: {elapsed_time:.1f} seconds") # Output: Elapsed time: 1.5 seconds
        return 0
        
    except Exception as e:
        print(f"❌ Error: {e}")
        if "--debug" in sys.argv:
            traceback.print_exc()
        return 1

if __name__ == "__main__":
    sys.exit(main())
