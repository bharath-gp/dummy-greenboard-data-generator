import hashlib
import multiprocessing
from couchbase.cluster import Cluster, ClusterOptions, PasswordAuthenticator
import faker
import random
import time
from constants import *
HOST = '172.23.120.147'

def do_query():
    cluster = Cluster("couchbase://" + HOST, ClusterOptions(
        PasswordAuthenticator("Administrator", "password")))
    for i in range(0,5000):
        query = random.choice(QUERIES)
        if callable(query):
            query = query()
        try:
            print('Querying : %s' % query)
            result = cluster.query(query)
            for row in result:
                    print(row)
        except Exception as e:
            print(e)


def run_query():
    while(True):
        procs = []
        for i in range(0, 4):
            proc = multiprocessing.Process(target=do_query)
            procs.append(proc)
            proc.start()
        for proc in procs:
            proc.join()

def run():
    query = multiprocessing.Process(target=run_query)
    query.start()
    query.join()

if __name__ == '__main__':
    do_query()
    run()