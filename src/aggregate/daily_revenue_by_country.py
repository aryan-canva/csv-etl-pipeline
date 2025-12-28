import csv
from src.utils.logger import logger
from src.utils.chunk_reader import read_chunks

CHUNK_SIZE = 3

def run():
    logger.info("Daily revenue aggregation started")

    with open("data/raw/users.csv", newline="") as user_f, \
         open("data/raw/orders.csv", newline="") as order_f, \
         open("data/output/revenue.csv", "w", newline="") as out_f:

        user_reader = csv.DictReader(user_f)
        order_reader = csv.DictReader(order_f)

        writer = csv.DictWriter(
            out_f,
            fieldnames=["order_date", "country", "total_order_amount"]
        )
        writer.writeheader()

        users = {}
        for row in user_reader:
            users[row.get("user_id")] = row

        revenue = {}

        for chunk in read_chunks(order_reader, CHUNK_SIZE):
            for row in chunk:
                uid = row.get("user_id")
                if uid not in users:
                    continue

                user_row = users[uid]
                key = (row.get("order_date"), user_row.get("country"))

                if key not in revenue:
                    revenue[key] = {
                        "order_date": row.get("order_date"),
                        "country": user_row.get("country"),
                        "total_order_amount": 0
                    }

                revenue[key]["total_order_amount"] += int(row.get("amount"))

        for value in revenue.values():
            writer.writerow(value)

    logger.info("Daily revenue aggregation finished")

if __name__ == "__main__":
    run()
