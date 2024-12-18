import time
from flask import Flask, request, jsonify
import threading
import requests
import json

app = Flask(__name__)


class Node():

    def __init__(self, node_id, cluster):
        self.node_id = node_id
        self.cluster = cluster

        self.kv = {}
        self.vector_clock = {}

        threading.Thread(target=self.sync_replicas, daemon=True).start()

    def sync_replicas(self):
        while True:
            self.broadcast_state_to_replicas(endpoint='/sync')
            time.sleep(3)


    def merge_kvs(self, other_kv, other_vector_clock, other_id):
        for key, new_val in other_kv.items():
            local_val = self.kv.get(key)
            if local_val == None or self.compare_vclocks_for_key(other_vector_clock, key, other_id):
                self.kv[key] = new_val
                self.vector_clock[key] = other_vector_clock.get(key, {})

    

    def compare_vclocks_for_key(self, other_vector_clock, key, other_id):
        local_vc = self.vector_clock.get(key, {})
        other_vc = other_vector_clock.get(key, {})

        print(self.node_id, local_vc, other_vc)
        local_newer = False
        other_newer = False
        for replica_id in set(local_vc.keys()).union(other_vc.keys()):
            local_cnt = local_vc.get(replica_id, 0)
            other_cnt = other_vc.get(replica_id, 0)

            if local_cnt > other_cnt:
                local_newer = True
            if other_cnt > local_cnt:
                other_newer = True
            
        print(self.node_id, local_vc, other_vc, local_newer, other_newer)
        if local_newer == False and other_newer == True:
            return 1

        if local_newer == False and other_newer == False and other_id > self.node_id:
            return 1
        return 0
        
    def broadcast_state_to_replicas(self, endpoint):
        for replica_id in self.cluster:
            if replica_id != self.node_id:
                port = 5000 + replica_id
                try:
                    data = {
                        'kv': self.kv,
                        'vector_clock': self.vector_clock,
                        'node_id': self.node_id
                    }
                    requests.post(f"http://127.0.0.1:{port}{endpoint}", json=data, timeout=1)
                except requests.exceptions.RequestException:
                    pass

    def apply_updates(self, updates):
        print("-----applying updates-----------")
        for key, val in updates.items():
            print(key, val)
            self.vector_clock[key] = self.vector_clock.get(key, {})
            self.vector_clock[key][str(self.node_id)] = self.vector_clock[key].get(self.node_id, 0) + 1
            self.kv[key] = val
        print("--------------------------------")
        self.broadcast_state_to_replicas(endpoint="/sync")

@app.route("/patch", methods=["PATCH"])
def patch_handler():
    msg = request.get_json()
    kv_updates = msg.get("updates", {})
    # print(kv_updates)
    node.apply_updates(kv_updates)

    return jsonify({"status": "ok"})


@app.route("/sync", methods=["POST"])
def sync_handler():
    msg = request.get_json()
    other_kv = msg['kv']
    other_vector_clock = msg['vector_clock']
    other_id = msg['node_id']

    node.merge_kvs(other_kv, other_vector_clock, other_id)
    return jsonify({"status": "ok"})

@app.route("/state", methods=["GET"])
def get_state():
    return jsonify({"kv": node.kv, "vector_clock": node.vector_clock})

if __name__ == "__main__":
    import sys
    node_id = int(sys.argv[1])
    cluster = [0, 1, 2]
    node = Node(node_id, cluster)
    app.run(host="0.0.0.0", port=5000 + node_id)
