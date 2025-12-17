class MemTableDB:
    def __init__(self):
        self.memtable = {}
        self.wal_filename = "wal.log" # Write Ahead Log (for crash recovery)

    def set(self, key, value):
        # 1. Write to disk log (so we don't lose data if we crash)
        with open(self.wal_filename, "a") as f:
            f.write(f"{key},{value}\n")
        
        # 2. Write to memory (Fast access)
        self.memtable[key] = value
    
    def get(self, key):
        # O(1) lookup in memory
        return self.memtable.get(key)

db = MemTableDB()
db.set("price_btc", "65000")
print(db.get("price_btc"))