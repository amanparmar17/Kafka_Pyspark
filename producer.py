from flask import Flask
import requests
import json
import random
import time

ENDPOINT = "http://localhost:8000/makerequest"
COL_A = ["a", "b"]


def read_json():
    location = "./request.json"
    with open(location, "r") as f:
        data = json.load(f)
    return data


if __name__ == "__main__":

    while True:
        data = read_json()

        data["col_a"] = random.choice(COL_A)
        data["col_b"] = str(random.randint(1, 3))
        data["col_c"] = str(random.random())
        data["col_d"] = str(random.random())

        data = json.dumps(data)

        r = requests.post(ENDPOINT, json=data)
        print("request made")
        time.sleep(3)
