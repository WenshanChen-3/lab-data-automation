import os
import time
import logging
from datetime import datetime, timedelta
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler


logging.basicConfig(
    filename="watchdog_debug.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

def convert_lr_to_epic(file_path, output_base_dir):
    """
    Converts LR metadata from .dat format to EPIC log format and appends it to LR.txt
    inside the correct dated folder. Adds a header if the file doesn't exist yet.
    """
    try:
        base_time = datetime.fromtimestamp(os.path.getctime(file_path))

        with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
            lines = f.readlines()

        converted_lines = []
        for line in lines:
            parts = line.strip().split("\t")
            if len(parts) != 2:
                continue
            try:
                time_offset = float(parts[0])
                intensity = parts[1]

                new_time = base_time + timedelta(seconds=time_offset)
                formatted_time = new_time.strftime("%d/%m/%Y %H:%M:%S.%f")

                converted_line = f"{formatted_time},{intensity}\n"
                converted_lines.append(converted_line)

            except ValueError:
                logging.warning(f"Skipping invalid line: {line.strip()} in file {file_path}")

        now = datetime.now()
        year_str = now.strftime("%Y")
        date_str = now.strftime("%Y_%m_%d")
        dated_output_dir = os.path.join(output_base_dir, year_str, date_str)
        os.makedirs(dated_output_dir, exist_ok=True)

        output_file_path = os.path.join(dated_output_dir, "LR.txt")

        file_exists = os.path.isfile(output_file_path)

        # Append is correct; we just prevent double-processing the same .dat elsewhere
        with open(output_file_path, 'a', encoding='utf-8') as f:
            if not file_exists:
                f.write("EPIC LR Log File\n\n")
                f.write("Date,LR\n")
            f.writelines(converted_lines)

        logging.info(f"Appended {len(converted_lines)} lines to: {output_file_path}")

    except Exception as e:
        logging.error(f"Failed to convert file {file_path}: {e}")


class LRMetaDataHandler(FileSystemEventHandler):
    def __init__(self, output_dir, inactivity_period=30):
        self.output_dir = output_dir
        self.inactivity_period = inactivity_period
        self.file_timestamps = {}       # path -> last time we saw activity
        self.processed_mtimes = {}      # path -> last mtime we already processed  <-- NEW

    def _should_track(self, path: str) -> bool:
        """Only track files whose mtime is newer than what we've processed."""
        try:
            mtime = os.path.getmtime(path)
        except FileNotFoundError:
            return False
        last = self.processed_mtimes.get(path, 0.0)
        return mtime > last

    def on_created(self, event):
        if event.is_directory:
            return
        if event.src_path.endswith('.dat') and self._should_track(event.src_path):  # <-- UPDATED
            logging.info(f"Detected new .dat file: {event.src_path}")
            self.file_timestamps[event.src_path] = time.time()

    def on_modified(self, event):
        if event.is_directory:
            return
        # Windows can fire many on_modified events; only update if mtime advanced
        if event.src_path.endswith('.dat') and self._should_track(event.src_path):  # <-- UPDATED
            self.file_timestamps[event.src_path] = time.time()

    def check_and_process_files(self):
        current_time = time.time()
        files_to_process = []

        for file_path, last_modified in list(self.file_timestamps.items()):
            logging.info(f"Checking file: {file_path}, last modified: {last_modified}")
            if current_time - last_modified > self.inactivity_period:
                files_to_process.append(file_path)

        for file_path in files_to_process:
            logging.info(f"Processing file: {file_path}")
            try:
                # Capture mtime before processing
                try:
                    latest_mtime = os.path.getmtime(file_path)
                except FileNotFoundError:
                    logging.warning(f"File disappeared before processing: {file_path}")
                    self.file_timestamps.pop(file_path, None)
                    continue

                convert_lr_to_epic(file_path, self.output_dir)

                # Record processed mtime so duplicate 'modified' events won't re-queue it  <-- NEW
                self.processed_mtimes[file_path] = latest_mtime
                self.file_timestamps.pop(file_path, None)

            except Exception as e:
                logging.error(f"Error processing {file_path}: {e}")


if __name__ == "__main__":
    lr_meta_dir = r"d:\PDIRS"
    epic_logs_dir = r"c:\EPIC\Latest\Logs"
    inactivity_period = 20

    try:
        event_handler = LRMetaDataHandler(output_dir=epic_logs_dir, inactivity_period=inactivity_period)
        observer = Observer()
        observer.schedule(event_handler, path=lr_meta_dir, recursive=False)
        observer.start()

        logging.info("Started watchdog. Monitoring for new .dat files...")

        while True:
            event_handler.check_and_process_files()
            time.sleep(5)

    except Exception as e:
        logging.error(f"Watchdog crashed: {e}")
        time.sleep(5)
