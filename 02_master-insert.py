from clickhouse_driver import Client
from datetime import datetime
if __name__ == "__main__":
    client = Client("127.0.0.1", port="9000")

    client.execute("CREATE DATABASE IF NOT EXISTS billing")

    client.execute('''CREATE TABLE IF NOT EXISTS billing.transactions(
                      timestamp DateTime,
                      currency String,
                      value Float64)
                      ENGINE = Distributed(example_cluster, billing, transactions, cityHash64(currency))''')

    client.execute("INSERT INTO billing.transactions (timestamp, currency, value) VALUES", \
        [(datetime.utcnow(), "integrity", 38.9), (datetime.utcnow(), "voltage", 27.2), \
            (datetime.utcnow(), "resilience", 19.8)])

    client.execute("INSERT INTO billing.transactions (timestamp, currency, value) VALUES", \
        [(datetime.utcnow(), "temperature", 38.9), (datetime.utcnow(), "humidity", 27.2), \
            (datetime.utcnow(), "density", 12.3)])

    client.execute("INSERT INTO billing.transactions (timestamp, currency, value) VALUES", \
        [(datetime.utcnow(), "voltage", 72.8), (datetime.utcnow(), "humidity", 39.8), \
            (datetime.utcnow(), "temperature", 88.13)])

    client.execute("INSERT INTO billing.transactions (timestamp, currency, value) VALUES", \
        [(datetime.utcnow(), "elasticity", 38.9), (datetime.utcnow(), "gravity", 27.2), \
            (datetime.utcnow(), "density", 19.8)])

    client.execute("INSERT INTO billing.transactions (timestamp, currency, value) VALUES", \
        [(datetime.utcnow(), "transitivity", 38.9), (datetime.utcnow(), "velocity", 27.2), \
            (datetime.utcnow(), "ferocity", 19.8)])

    data = client.execute("SELECT * FROM billing.transactions")

    for row in data:
        print("Timestamp", row[0], sep=": ")
        print("Currency", row[1], sep=": ")
        print("Value", row[2], sep=": ")
        print()