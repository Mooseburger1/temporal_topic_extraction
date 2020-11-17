import boto3
from boto3.dynamodb.conditions import Key, Attr
import numpy as np
from utils import *
from multiprocessing import Process, Queue
import time
import random
from queue import Empty
from calendar import monthrange


def query_by_month(year, month, dynamodb):

    query_constraints = Key('year').eq(year) & Key('uid').between('{}.0.0'.format(month), '{}.31.9999999'.format(month))

    return uid_page_iterator(constraints=query_constraints, dynamodb=dynamodb)


def query_by_day(year, month, day, dynamodb):
    
    query_constraints = Key('year').eq(year) & Key('uid').between('{}.{}.0'.format(month,day), '{}.{}.9999999'.format(month,day))

    return uid_page_iterator(constraints=query_constraints, dynamodb=dynamodb)


def uid_page_iterator(constraints, dynamodb):
    paginator = dynamodb.meta.client.get_paginator('query')
    page_iterator = paginator.paginate(
        TableName='articles2',
        ProjectionExpression='uid',
        KeyConditionExpression=constraints
    )
    
    return page_iterator


def accumulate_uids(year: int, month: int, db, throttled=True) -> np.array:
    uids = []
    
    query_constraints = Key('year').eq(year) & Key('uid').between('{}.0.0'.format(month), '{}.99.9999999'.format(month))
    response = uid_page_iterator(query_constraints, dynamodb=db)
    
    for page in response:
        items = page['Items']
        uid = [d['uid'] for d in items]

        uids.extend(uid)
        if throttled: time.sleep(1.5)
    
    return np.array(uids)


def single_query(year, uid, dynamodb):
    response = dynamodb.meta.client.query(
        TableName='articles2',
        KeyConditionExpression=Key('year').eq(year) & Key('uid').eq(uid)
    )

    return (response['Items'][0]['bow'].replace('[',"").replace(']',"").replace("'","").replace(" ","").split(","), 
            response['Items'][0]['year'], 
            response['Items'][0]['uid'], 
            response['Items'][0]['file'])


def get_uids_months(year: int, month: int, dynamodb, throttled):
    
    #check if uids have already been cached
    print('Checking for cached UIDs for {}/{}'.format(year, month))
    if check_for_cached_uid(year=year, month=month):
        print('Cache found - loading from local')
        uids = load_cached_uids(year=year, month=month)
        return uids
    else:
        print('No cached UIDs found for {}/{} - Pulling from dynamodb........This may take a few moments'.format(year, month))
        uids = accumulate_uids(year=year, month=month, db=dynamodb, throttled=throttled)
        print('Caching {} UIDs for {}/{}'.format(len(uids),year, month))
        cache_uids(year=year, month=month, data=uids)
        return uids

def get_uids_days(year: int, month: int, day: int):
    
    return load_cached_uid_days(year=year, month=month, day=day)


def keepalive_accumulate(q, processes, storage_obj):
    liveprocs = processes.copy()
    while liveprocs:
        try:
            while 1:
                print('Flushing Queue pipes for UIDs')
                storage_obj.append(q.get(False))
        except Empty:
            pass

        time.sleep(0.5)    # Give tasks a chance to put more data in
        if not q.empty():
            continue
        liveprocs = [p for p in liveprocs if p.is_alive()]
    
    return storage_obj

def temporal_topic_extraction_whole_year(year: int, sample_size: int, dynamodb, throttled=True):
    
    q = Queue()
    processes = []
    uids = []
    
    for i in range(1,13):
        print('Creating process {} to sample UIDs for month {}'.format(i, i+1))
        p = Process(target=sample_uids_months, args=(sample_size, year, i, dynamodb, q, throttled))
        p.start()
        processes.append(p)


    uids = keepalive_accumulate(q, processes, uids)
  
    for pos, p in enumerate(processes):
        print('joining process', pos)
        p.join()
        
        
    uids = np.array([np.array(x) for x in uids if x is not None])
   

    q = Queue()
    processes=[]
    data= []
    for pos, uid_set in enumerate(uids):
        print('Creating process for BOWs for month', pos+1)
        p = Process(target=query_uid_set, args=(year, uid_set, dynamodb, q, False))
        p.start()
        processes.append(p)

    data = keepalive_accumulate(q, processes, data)

    for pos, p in enumerate(processes):
        p.join()


    return data


def query_uid_set(year, uid_set, dynamodb, q=None, single_thread=False):
    bow_arrays = []

    for uid in uid_set:
        bow, _, _, _ = single_query(year, uid, dynamodb)
        bow_arrays.append(bow)
        
    if single_thread: return bow_arrays

    else: 
        q.put(bow_arrays)



def sample_uids_months(sample_size, year, month, dynamodb, q, throttled):
    uids = get_uids_months(year=year, month=month, dynamodb=dynamodb, throttled=throttled)
    sys_random = random.SystemRandom()
    try:
        rand = np.array(sys_random.sample(set(uids), sample_size, ))
        q.put( rand )
    except ValueError:
        q.put(None)
    
def sample_uids_days(sample_size, year, month, day, q):
    uids = get_uids_days(year=year, month=month, day=day)
    sys_random = random.SystemRandom()

    try:
        print(len(uids))
        rand = np.array(sys_random.choices(list(set(uids)), k=sample_size))
        q.put( rand )
    except ValueError:
        q.put(None)


def temporal_topic_extraction_whole_month(year: int, month: int, sample_size: int, dynamodb, throttled=True):
    
    #check if uids have already been cached
    print('Checking for cached UIDs for {}/{}'.format(year, month))
    if check_for_cached_uid(year=year, month=month):
        print('Cache found - loading from local')
    else:
        print('No cached UIDs found for {}/{} - Pulling from dynamodb........This may take a few moments'.format(year, month))
        uids = accumulate_uids(year=year, month=month, db=dynamodb, throttled=throttled)
        for x in uids:
            print(x)
        print('Caching {} UIDs for {}/{}'.format(len(uids),year, month))
        cache_uids(year=year, month=month, data=uids)

    q = Queue()
    processes = []
    uids = []
    
    num_of_days = monthrange(year, month)[1]
    for i in range(1, num_of_days+1):
        print('Creating process {} to sample UIDs for day {}'.format(i, i))
        p = Process(target=sample_uids_days, args=(sample_size, year, month, i, q))
        p.start()
        processes.append(p)


    uids = keepalive_accumulate(q, processes, uids)
  
    for pos, p in enumerate(processes):
        print('joining process', pos)
        p.join()
        
        
    uids = np.array([np.array(x) for x in uids if x is not None])
   

    q = Queue()
    processes=[]
    data= []
    for pos, uid_set in enumerate(uids):
        print('Creating process for BOWs for day', pos+1)
        p = Process(target=query_uid_set, args=(year, uid_set, dynamodb, q, False))
        p.start()
        processes.append(p)

    data = keepalive_accumulate(q, processes, data)

    for pos, p in enumerate(processes):
        p.join()


    return data