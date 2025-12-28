import csv
import json
from pathlib import Path

from src.utils.logger import logger
from src.utils.chunk_reader import read_chunks

BASE_DIR = Path(__file__).resolve().parent.parent
CONFIG_PATH = BASE_DIR / "config2.json"

with open(CONFIG_PATH) as f:
    CONFIG = json.load(f)

def run():
    logger.info("Join job started")

    key = CONFIG["join"]["key"]

    with open(CONFIG["input"]["users_file"], newline="") as user_f, \
         open(CONFIG["input"]["orders_file"], newline="") as order_f, \
         open(CONFIG["output"]["joined_file"], "w", newline="") as joined_f:

        user_reader = csv.DictReader(user_f)
        order_reader = csv.DictReader(order_f)

        order_fields = order_reader.fieldnames
        user_fields = [
            f for f in user_reader.fieldnames
            if f != key and f not in order_fields
        ]

        writer = csv.DictWriter(
            joined_f,
            fieldnames=order_fields + user_fields
        )
        writer.writeheader()

        users = {}
        for row in user_reader:
            users[row[key]] = row

        for chunk in read_chunks(order_reader, CONFIG["processing"]["chunk_size"]):
            for row in chunk:
                uid = row.get(key)
                user_row = users.get(uid)

                if user_row:
                    for f in user_fields:
                        row[f] = user_row[f]
                else:
                    for f in user_fields:
                        row[f] = "not_available"

                writer.writerow(row)

    logger.info("Join job finished")

if __name__ == "__main__":
    run()
