import os
import logging

class Tracker:
    def __init__(self, name, path="./"):
        self.file_path = os.path.join(path, f"{name}.txt")
        self.tracker = set()
        self.new_items = []

        if not os.path.exists(path):
            os.makedirs(path)
        
        if os.path.exists(self.file_path):
            logging.info("Tracker exists on disk. Loading tracker from disk")
            with open(self.file_path, "r", encoding="utf-8") as f:
                self.tracker = set([line.rstrip() for line in f])

    def add(self, item: str):
        self.tracker.add(item)
        self.new_items.append(item)

    def check(self, item: str):
        return item in self.tracker

    def save(self):
        with open(self.file_path, "a", encoding="utf-8") as f:
            for item in self.new_items:
                f.write(item)
                f.write("\n")
        self.new_items = []