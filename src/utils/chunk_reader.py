def read_chunks(reader, size):
    chunk = []
    for row in reader:
        chunk.append(row)

        if len(chunk) == size:
            yield chunk
            chunk = []

    if chunk:
        yield chunk
