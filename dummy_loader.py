import hashlib
import multiprocessing
from couchbase.cluster import Cluster, ClusterOptions, PasswordAuthenticator
import faker
import random
import time
from constants import *
HOST = '172.23.120.147'

def create_document(view):
    fake = faker.Faker()
    doc = {}
    platforms = view['platforms']
    features = view['features']
    doc['build_id'] = random.randint(10000, 50000)
    doc['claim'] = ''
    doc["os"] = random.choice(platforms)
    component = random.choice(features).split('-')[1]
    doc["component"] = component
    doc['name'] = "%s-%s_%s" % (doc['os'].lower(),
                                doc['component'].lower(), fake.pystr(
        10, 25))
    doc["totalCount"] = random.randint(5,50)
    doc["url"] = "http=//qa.sc.couchbase.com/job/test_suite_executor/"
    doc["color"] = random.choice(['yellow_anime', 'red', 'grey'])
    doc["result"] = random.choice(['SUCCESS', 'SUCCESS', 'SUCCESS',
                                   'SUCCESS', 'SUCCESS', 'SUCCESS',
                                   'UNSTABLE', 'UNSTABLE', 'FAILURE',
                                   'ABORTED'])
    if doc['result'] == 'SUCCESS':
        doc["failCount"] = 0
    elif doc['result'] == 'FAILURE':
        doc['failCount'] = doc['totalCount']
    else:
        doc['failCount'] = random.randint(1,doc['totalCount'])
    doc["duration"] = random.randint(1000, 10000000)
    doc["priority"] = random.choice(['P1', 'P2', 'P0'])
    doc["build"] = "7.0.0-%s" % random.randint(3000, 5000)
    return doc

def insert_documents():
    clients = create_clients()
    for i in range(0,5000):
        view = random.choice(VIEWS)
        doc = create_document(view)
        key = "%s-%s-%s" % (doc["name"], doc["build_id"], doc["os"])
        key = hashlib.md5(key.encode()).hexdigest()
        client = clients[view['bucket']]
        try:
            client.upsert(key, doc)
            print('inserted %s into %s' % (key, view['bucket']))
        except Exception as e:
            print(e)

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


def create_clients(password='password'):
    clients = {}
    cluster = Cluster("couchbase://" + HOST, ClusterOptions(
        PasswordAuthenticator("Administrator", password)))
    clients['server'] =cluster.bucket('server').default_collection()
    clients['cblite'] = cluster.bucket('cblite').default_collection()
    clients['sync_gateway'] = cluster.bucket('sync_gateway').default_collection()
    return clients

def run_query():
    while(True):
        procs = []
        for i in range(0, 4):
            proc = multiprocessing.Process(target=do_query)
            procs.append(proc)
            proc.start()
        for proc in procs:
            proc.join()

def run_load():
    while(True):
        procs = []
        for i in range(0, 4):
            proc = multiprocessing.Process(target=insert_documents)
            procs.append(proc)
            proc.start()
        for proc in procs:
            proc.join()

def run():
    load = multiprocessing.Process(target=run_load)
    query = multiprocessing.Process(target=run_query)
    load.start()
    query.start()
    load.join()
    query.join()

if __name__ == '__main__':
    run()