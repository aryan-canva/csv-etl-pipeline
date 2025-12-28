import csv
from src.utils.logger import logger
from src.utils.chunk_reader import read_chunks
from src.config import CONFIG


TYPE_MAP = {
    "int": int,
    "str": str
}

schema = {
    col: TYPE_MAP[type_name]
    for col, type_name in CONFIG["schema"].items()
}

def isgarbage(value):
    if value is None:
        return True

    value = value.strip()

    if value == "":
        return True

    if value.lower() in CONFIG["garbage_values"]:
        return True

    return False

def cast_row(row):
    clean = {}

    if set(row.keys()) != set(schema.keys()):
        raise ValueError("SCHEMA_MISMATCH")

    for col, dtype in schema.items():
        value = row.get(col)

        if isgarbage(value):
            raise ValueError(f"Missing Value: {col}")

        try:
            clean[col] = dtype(value)
        except (ValueError, TypeError):
            raise TypeError(f"Bad_Type: {col}")

    return clean

def run():
    logger.info("Cleaning job started")

    with open(CONFIG["input_path"], newline="") as src, \
         open(CONFIG["output_path"], "w", newline="") as clean_f, \
         open("data/output/rejected.csv", "w", newline="") as bad_f:

        reader = csv.DictReader(src)
        clean_writer = csv.DictWriter(clean_f, fieldnames=schema.keys())
        bad_writer = csv.DictWriter(
            bad_f,
            fieldnames=reader.fieldnames + ["error"]
        )

        clean_writer.writeheader()
        bad_writer.writeheader()

        chunk_id = 0

        for chunk in read_chunks(reader, CONFIG["chunk_size"]):
            chunk_id += 1
            processed = valid = rejected = 0
            error_counts = {}

            for row in chunk:
                processed += 1
                try:
                    clean_row = cast_row(row)
                    clean_writer.writerow(clean_row)
                    valid += 1
                except Exception as e:
                    error_counts[str(e)] = error_counts.get(str(e), 0) + 1
                    row["error"] = str(e)
                    bad_writer.writerow(row)
                    rejected += 1

                if rejected / processed > 0.8:
                    raise RuntimeError("Dataset_Too_Corrupt")

            logger.info(
                f"Chunk {chunk_id} | processed={processed} "
                f"valid={valid} rejected={rejected} errors={error_counts}"
            )

    logger.info("Cleaning job finished")

if __name__ == "__main__":
    run()
