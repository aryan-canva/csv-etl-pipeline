import csv
from src.utils.logger import logger
from src.utils.chunk_reader import read_chunks


CHUNK_SIZE = 3

def run():
    logger.info("Aggregation by user started")

    with open("data/raw/orders.csv", newline="") as order_f, \
         open("data/output/aggregate.csv", "w", newline="") as out_f:

        reader = csv.DictReader(order_f)
        writer = csv.DictWriter(
            out_f,
            fieldnames=["user_id", "total_order_amount"]
        )
        writer.writeheader()

        totals = {}

        for chunk in read_chunks(reader, CHUNK_SIZE):
            for row in chunk:
                uid = row.get("user_id")
                amount = int(row["amount"])

                if uid not in totals:
                    totals[uid] = 0

                totals[uid] += amount

        for uid, total in totals.items():
            writer.writerow({
                "user_id": uid,
                "total_order_amount": total
            })

    logger.info("Aggregation by user finished")

if __name__ == "__main__":
    run()
