
import redis
import json
import os
import platform

if(platform.system() == 'Windows'):
    host = "127.0.0.1"
    port = 6379
    password = ""
    db = 7
else:
    host = "pika"
    port = 6379
    password = ""
    db = 7


class TableRowImporter:
    def __init__(self: object, schema: str, table: str):
        self.schema = schema
        self.table = table
        self.conn = redis.Redis(host=host, port=port, password=password, db=db)
        self.fields = []
        json_path = os.path.join(os.path.dirname(os.getcwd()), "presto", "tables",
                                 "%s.%s.json" % (self.schema, self.table))
        with open(json_path) as f:
            data = json.loads(f.read())
            for field in data["value"]["fields"]:
                self.fields.append(field["name"])

    def save(self: object, key: str, values: list):
        with self.conn.pipeline(transaction=False) as pipe:
            name = ":".join([self.schema, self.table, key])
            for i in range(0, len(self.fields)):
                pipe.hset(name, self.fields[i], values[i])
