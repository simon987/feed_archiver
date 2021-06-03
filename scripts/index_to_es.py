import elasticsearch
import psycopg2
import ujson as json

HOST = "localhost"
PORT = "5432"
DB = "feed_archiver"
USER = "feed_archiver"
PW = "feed_archiver"

TABLES = [
    "chan_4chan_post",
    "chan_4chan_thread",
]

TO_REMOVE = [
    "_chan",
    "_urls"
]

TS = 0
BULK_SIZE = 10000
ES_HOST = "localhost"
ES_INDEX = "feed_archiver"
docs = []
es = elasticsearch.Elasticsearch(hosts=[ES_HOST])


def create_bulk_index_string(docs: list):

    string = ""
    for doc in docs:
        doc_id = doc["_id"]
        del doc["_id"]

        for key in TO_REMOVE:
            if key in doc:
                del doc[key]

        string += '{"index":{"_id": "%d"}}\n%s\n' % (doc_id, json.dumps(doc))

    return string


def index():
    print("Indexing %d documents" % (len(docs), ))
    bulk_string = create_bulk_index_string(docs)
    es.bulk(body=bulk_string, index=ES_INDEX, refresh=True)

    docs.clear()


with psycopg2.connect(database=DB, user=USER, password=PW, host=HOST, port=PORT) as conn:
    for tab in TABLES:
        cur = conn.cursor()
        cur.execute(
            "SELECT id, data FROM $TABLE$ "
            "WHERE cast(extract(epoch from archived_on) as integer) > %s".replace("$TABLE$", tab),
            (TS,)
        )

        for row in cur:

            if not isinstance(row[1], dict) or not isinstance(row[1]["_id"], int):
                continue

            docs.append(row[1])

            if len(docs) >= BULK_SIZE:
                index()
    index()

