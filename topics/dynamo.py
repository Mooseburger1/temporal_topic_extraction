import boto3
from boto3.dynamodb.conditions import Key, Attr
import numpy as np
from utils import *
from multiprocessing import Process, Queue
import time
import random
from queue import Empty




def uid_page_iterator(year, month, dynamodb):
    paginator = dynamodb.meta.client.get_paginator('query')
    page_iterator = paginator.paginate(
        TableName='articles',
        ProjectionExpression='uid',
        KeyConditionExpression=Key('year').eq(year) & Key('uid').\
                                           between('{}.0'.format(month), '{}.9999999'.format(month))
    )
    
    return page_iterator


def accumulate_uids(year: int, month: int, db, throttled=True) -> np.array:
    uids = []
    
    response = uid_page_iterator(year=year, month=month, dynamodb=db)
    
    for page in response:
        items = page['Items']
        uid = [d['uid'] for d in items]

        uids.extend(uid)
        if throttled: time.sleep(1.5)
    
    return np.array(uids)


def single_query(year, uid, dynamodb):
    response = dynamodb.meta.client.query(
        TableName='articles',
        KeyConditionExpression=Key('year').eq(year) & Key('uid').eq(uid)
    )
    
    return (response['Items'][0]['bow'].replace('[',"").replace(']',"").replace("'","").replace(" ","").split(","), 
            response['Items'][0]['year'], 
            response['Items'][0]['uid'], 
            response['Items'][0]['file'])


def get_uids(year: int, month: int, dynamodb, throttled):
    
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


def temporal_topic_extraction(year: int, sample_size: int, dynamodb, throttled=True):
    
    q = Queue()
    processes = []
    uids = []
    
    for i in range(1,13):
        print('Creating process {} to sample UIDs for month {}'.format(i, i+1))
        p = Process(target=sample_uids, args=(sample_size, year, i, dynamodb, q, throttled))
        p.start()
        processes.append(p)


    liveprocs = processes.copy()
    while liveprocs:
        try:
            while 1:
                print('Flushing Queue pipes for UIDs')
                uids.append(q.get(False))
        except Empty:
            pass

        time.sleep(0.5)    # Give tasks a chance to put more data in
        if not q.empty():
            continue
        liveprocs = [p for p in liveprocs if p.is_alive()]
  
    for pos, p in enumerate(processes):
        # print('Getting sampled UIDs from process', pos)
        # uids.append( yield_from_process(q, p) )
        print('joining process', pos)
        p.join()
        # uids.append(q.get())
        
    uids = np.array([np.array(x) for x in uids if x is not None])
   

    q = Queue()
    processes=[]
    data= []
    for pos, uid_set in enumerate(uids):
        print('Creating process for BOWs for month', pos+1)
        p = Process(target=query_uid_set, args=(year, uid_set, dynamodb, q, False))
        p.start()
        processes.append(p)

    liveprocs = processes.copy()
    while liveprocs:
        try:
            while 1:
                print('Flushing Queue pipes for BOWs')
                data.append(q.get(False))
        except Empty:
            pass

        time.sleep(0.5)    # Give tasks a chance to put more data in
        if not q.empty():
            continue
        liveprocs = [p for p in liveprocs if p.is_alive()]



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



def sample_uids(sample_size, year, month, dynamodb, q, throttled):
    uids = get_uids(year=year, month=month, dynamodb=dynamodb, throttled=throttled)
    sys_random = random.SystemRandom()
    try:
        rand = np.array(sys_random.sample(set(uids), sample_size, ))
        q.put( rand )
    except ValueError:
        q.put(None)
    