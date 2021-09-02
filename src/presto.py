import redis
import json
import os

host = "tendis"
port = 51002
password = "adminpass"
db = 0


class TableRowImporter:
    def __init__(self, schema, table):
        self.schema = schema
        self.table = table
        self.conn = redis.Redis(
            host=host, port=port, password=password, db=db)
        self.fields = []
        json_path = os.path.join(os.getcwd(), os.path.pardir, "presto", "tables", "%s.%s.json" % (
            self.schema, self.table))
        with open(json_path) as f:
            data = json.loads(f.read())
            for field in data["value"]["fields"]:
                self.fields.append(field["name"])

    def save(self, key, values):
        with self.conn.pipeline(transaction=False) as pipe:
            name = ":".join([self.schema, self.table, key])
            for i in range(0, len(self.fields)):
                self.conn.hset(name, self.fields[i], values[i])
