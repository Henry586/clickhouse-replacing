from clickhouse_driver import Client
from datetime import datetime

if __name__ == "__main__":

    client = Client("127.0.0.1", port="9000")

    client.execute("CREATE DATABASE IF NOT EXISTS billing on cluster example_cluster ")

    client.execute(r'''CREATE TABLE IF NOT EXISTS billing.transactions on cluster example_cluster (
                      timestamp DateTime,
                      currency String,
                      value Float64)
                      ENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/{shard}/billing.transactions', '{replica}')
                      PARTITION BY currency
                      ORDER BY (currency,value)''')

