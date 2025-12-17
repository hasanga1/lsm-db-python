import os

class LogStorage:
    def __init__(self, filename="database.log"):
        self.filename = filename

    def set(self, key, value):
        """Append the key,value pair to the end of the file (CSV format)"""
        with open(self.filename, "a") as f:
            f.write(f"{key},{value}\n")
    
    def get(self, search_key):
        """Scan the entire file to find the key (Inefficient!)"""
        if not os.path.exists(self.filename):
            return None
            
        # We read lines in reverse because we want the LATEST value
        with open(self.filename, "r") as f:
            lines = f.readlines()
            for line in reversed(lines):
                key, value = line.strip().split(",")
                if key == search_key:
                    return value
        return None

# Test it
db = LogStorage()
db.set("user_1", "Alice")
db.set("user_2", "Bob")
db.set("user_1", "Alice_Updated") # Update value

print(f"User 1 is: {db.get('user_1')}")