import os
import time
import shutil
import hashlib
import argparse
from datetime import datetime


class FolderSynchronizer:
    def __init__(self, source, replica, log_file, interval):
        self.source = source
        self.replica = replica
        self.log_file = log_file
        self.interval = interval
        self.md5_cache = {}

    def calculate_md5(self, file_path):
        """Calculate MD5 hash of a file."""
        if file_path in self.md5_cache:
            return self.md5_cache[file_path]
        hash_md5 = hashlib.md5()
        try:
            with open(file_path, "rb") as f:
                for chunk in iter(lambda: f.read(4096), b""):
                    hash_md5.update(chunk)
        except FileNotFoundError:
            return None
        self.md5_cache[file_path] = hash_md5.hexdigest()
        return self.md5_cache[file_path]

    def log_operation(self, message):
        """Log operation to console and log file."""
        print(message)
        with open(self.log_file, "a") as log:
            log.write(f"{datetime.now()} - {message}\n")

    def sync_folders(self):
        """Synchronize replica folder with source folder."""
        try:
            # Create replica folder if it doesn't exist
            if not os.path.exists(self.replica):
                os.makedirs(self.replica)
                self.log_operation(f"Created replica folder: {self.replica}")

            # Copy or update files from source to replica
            for root, _, files in os.walk(self.source):
                relative_path = os.path.relpath(root, self.source)
                replica_root = os.path.join(self.replica, relative_path)

                if not os.path.exists(replica_root):
                    os.makedirs(replica_root)
                    self.log_operation(f"Created folder: {replica_root}")

                for file in files:
                    source_file = os.path.join(root, file)
                    replica_file = os.path.join(replica_root, file)

                    if not os.path.exists(replica_file) or self.calculate_md5(source_file) != self.calculate_md5(
                            replica_file):
                        shutil.copy2(source_file, replica_file)
                        self.log_operation(f"Copied/Updated file: {replica_file}")

            # Remove files and directories from replica that are not in source
            for root, _, files in os.walk(self.replica):
                relative_path = os.path.relpath(root, self.replica)
                source_root = os.path.join(self.source, relative_path)

                if not os.path.exists(source_root):
                    shutil.rmtree(root)
                    self.log_operation(f"Removed folder: {root}")

                for file in files:
                    replica_file = os.path.join(root, file)
                    source_file = os.path.join(source_root, file)

                    if not os.path.exists(source_file):
                        os.remove(replica_file)
                        self.log_operation(f"Removed file: {replica_file}")
        except Exception as e:
            self.log_operation(f"Error during synchronization: {e}")

    def run(self):
        """Run synchronization at specified intervals."""
        while True:
            self.sync_folders()
            time.sleep(self.interval)


def main():
    parser = argparse.ArgumentParser(description="Synchronize two folders.")
    parser.add_argument("source", help="Source folder path")
    parser.add_argument("replica", help="Replica folder path")
    parser.add_argument("interval", type=int, help="Synchronization interval in seconds")
    parser.add_argument("log_file", help="Log file path")

    args = parser.parse_args()

    synchronizer = FolderSynchronizer(args.source, args.replica, args.log_file, args.interval)
    synchronizer.run()


if __name__ == "__main__":
    main()
