import bisect

class SSTableDB:
    def __init__(self, sstable_file="data.db"):
        self.memtable = {}
        self.threshold = 5  # Flush to disk after 5 entries
        self.sstable_file = sstable_file
        self.index = {} # Sparse Index: Key -> Byte Offset in file

    def set(self, key, value):
        self.memtable[key] = value
        if len(self.memtable) >= self.threshold:
            self.flush_to_disk()

    def flush_to_disk(self):
        print("Flushing memory to disk...")
        sorted_keys = sorted(self.memtable.keys())
        
        with open(self.sstable_file, "w") as f:
            for i, key in enumerate(sorted_keys):
                value = self.memtable[key]
                
                # OPTIMIZATION: Sparse Index
                # Only record the offset for every 10th key (or start of a block)
                if i % 10 == 0:
                    offset = f.tell()
                    self.index[key] = offset  
                    print(f"Indexed key '{key}' at offset {offset}")
                
                f.write(f"{key},{value}\n")
        
        self.memtable.clear()

    def get(self, key):
        # 1. Check Memory first
        if key in self.memtable:
            return self.memtable[key]
        
        # 2. Sparse Search
        # We need the largest key in self.index that is <= search_key
        # In Python, we can use bisect to find the insertion point
        sorted_index_keys = sorted(self.index.keys())
        import bisect
        i = bisect.bisect_right(sorted_index_keys, key)
        
        if i == 0:
            return None # The key is smaller than the first indexed key
        
        # Get the "Block Start" key
        closest_key = sorted_index_keys[i - 1]
        offset = self.index[closest_key]
        
        # 3. Seek & Scan
        with open(self.sstable_file, "r") as f:
            f.seek(offset) 
            
            # Linearly scan until we find our key OR we go past it
            for line in f:
                k, v = line.strip().split(",")
                if k == key:
                    return v
                if k > key:
                    # We went past it! The key doesn't exist.
                    return None
        return None

# Test Run
db = SSTableDB()
# Add data (unordered)
db.set("b", "2")
db.set("a", "1")
db.set("e", "5")
db.set("d", "4")
db.set("c", "3") # This 5th entry triggers the flush

# Now "a", "b", "c", "d", "e" are sorted on disk in 'data.db'
print(f"Fetching 'c' from disk: {db.get('c')}")