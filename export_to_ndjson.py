from hexlib.db import pg_fetch_cursor_all
import psycopg2
from tqdm import tqdm
import orjson
import zstandard as zstd

TABLE = "chan_8kun2_post"
THREADS = 12

if __name__ == '__main__':

    conn = psycopg2.connect(
        host="",
        port="",
        user="",
        password="",
        dbname="feed_archiver"
    )

    cur = conn.cursor()

    cur.execute("SELECT COUNT(*) FROM %s" % TABLE)
    row_count = cur.fetchone()[0]

    cur.execute("DECLARE cur1 CURSOR FOR SELECT * FROM %s" % TABLE)

    rows = pg_fetch_cursor_all(cur, name="cur1", batch_size=5000)

    with open("out_mp.ndjson.zst", "wb") as f:
        cctx = zstd.ZstdCompressor(level=19, threads=THREADS)
        with cctx.stream_writer(f) as compressor:
            for row in tqdm(rows, total=row_count, unit="row"):
                _id, archived_on, data = row
                data["_archived_on"] = int(archived_on.timestamp())
                compressor.write(orjson.dumps(data))
                compressor.write(b"\n")

    conn.close()
