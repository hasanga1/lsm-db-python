import os
import time
import bisect
import hashlib

class SimpleBloomFilter:
    """
    A probabilistic data structure that tells us if a key 
    MIGHT be in the set or definitely is NOT.
    """
    def __init__(self, size=1000):
        self.size = size
        self.bit_array = [0] * size
    
    def _hash(self, item):
        result = hashlib.md5(item.encode()).hexdigest()
        return int(result, 16) % self.size

    def add(self, item):
        index = self._hash(item)
        self.bit_array[index] = 1

    def contains(self, item):
        index = self._hash(item)
        return self.bit_array[index] == 1

class Segment:
    """
    Represents one immutable SSTable file on disk.
    Contains:
    1. The Filename (where data lives)
    2. A Sparse Index (in memory)
    3. A Bloom Filter (in memory)
    """
    def __init__(self, filename):
        self.filename = filename
        self.index = {}
        self.bloom_filter = SimpleBloomFilter() # Per-segment Bloom Filter

    def search(self, key):
        # OPTIMIZATION 1: Check Bloom Filter first!
        # If Bloom Filter says "False", the key is definitely not here.
        if not self.bloom_filter.contains(key):
            print(f"  [Bloom Filter] Skipped {self.filename} (Key definitely not here)")
            return None
        
        # If we are here, the key MIGHT be in the file.
        # OPTIMIZATION 2: Use Sparse Index to find approximate location
        sorted_keys = sorted(self.index.keys())
        i = bisect.bisect_right(sorted_keys, key)
        
        if i == 0:
            return None 
            
        # Jump to the closest known key
        closest_key = sorted_keys[i - 1]
        offset = self.index[closest_key]
        
        # OPTIMIZATION 3: Disk Seek & Scan
        with open(self.filename, "r") as f:
            f.seek(offset)
            print(f"  [Disk I/O] Scanning {self.filename} starting at offset {offset}...")
            for line in f:
                k, v = line.strip().split(",")
                if k == key:
                    return v
                if k > key:
                    return None
        return None

class LSMTreeDB:
    def __init__(self):
        self.memtable = {}
        self.threshold = 5
        self.segments = []

    def set(self, key, value):
        self.memtable[key] = value
        if len(self.memtable) >= self.threshold:
            self.flush_to_disk()

    def flush_to_disk(self):
        # Create a unique filename based on time
        filename = f"sstable_{int(time.time() * 1000)}.db"
        print(f"\n[Flush] Dumping memory to {filename}...")
        
        # Create new Segment
        new_segment = Segment(filename)
        sorted_keys = sorted(self.memtable.keys())
        
        with open(filename, "w") as f:
            for i, key in enumerate(sorted_keys):
                value = self.memtable[key]
                
                # 1. Add to Bloom Filter
                new_segment.bloom_filter.add(key)
                
                # 2. Add to Sparse Index (every 2nd key for this demo)
                if i % 2 == 0:
                    offset = f.tell()
                    new_segment.index[key] = offset
                
                # 3. Write to Disk
                f.write(f"{key},{value}\n")
        
        # Add to our list of segments
        self.segments.append(new_segment)
        self.memtable.clear()

    def get(self, key):
        print(f"\nQuerying: '{key}'")
        
        # 1. Check MemTable (Fastest)
        if key in self.memtable:
            print("  Found in MemTable!")
            return self.memtable[key]
        
        # 2. Check Segments (Newest -> Oldest)
        # We iterate backwards because the newest file has the latest data
        for segment in reversed(self.segments):
            val = segment.search(key)
            if val:
                print(f"  Found in {segment.filename}")
                return val
        
        print("  Key not found.")
        return None

db = LSMTreeDB()

print("--- Writing Batch 1 (Old Data) ---")
db.set("apple", "1")
db.set("banana", "2")
db.set("cat", "3")
db.set("dog", "4")
db.set("elephant", "5") # Triggers Flush 1

time.sleep(0.1) # Ensure filename timestamp changes

print("--- Writing Batch 2 (New Data) ---")
db.set("apple", "100") # UPDATE: 'apple' is now 100
db.set("frog", "6")
db.set("goat", "7")
db.set("horse", "8")
db.set("iguana", "9") # Triggers Flush 2

# Case A: Reading a key that was updated in the second batch
# Expectation: Should find 100 in the NEW file, ignoring the 1 in the OLD file.
print(f"Result: {db.get('apple')}")

# Case B: Reading a key that only exists in the old batch
# Expectation: Bloom filter of NEW file should say "Skip", then find in OLD file.
print(f"Result: {db.get('cat')}")

# Case C: Reading a key that doesn't exist at all
# Expectation: Bloom filters for BOTH files should say "Skip". ZERO Disk I/O.
print(f"Result: {db.get('zebra')}")