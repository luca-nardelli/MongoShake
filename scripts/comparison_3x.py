#!/usr/bin/env python

# -*- coding:utf-8 -*-  

import pymongo
import time
import random
import sys
import getopt
import json
from tqdm import tqdm  

# constant
COMPARISION_COUNT = "comparison_count"
COMPARISION_MODE = "comparisonMode"
EXCLUDE_DBS = "excludeDbs"
EXCLUDE_COLLS = "excludeColls"
SKIP_INDEXES = "skipIndexes"
COUNT_THRESHOLD = "countThreshold"
SAMPLE = "sample"
# we don't check collections and index here because sharding's collection(`db.stats`) is splitted.
CheckList = {"objects": 1, "numExtents": 1, "ok": 1}
configure = {}

def log_info(message):
    print("INFO  [%s] %s " % (time.strftime('%Y-%m-%d %H:%M:%S'), message))

def log_error(message):
    print("ERROR [%s] %s " % (time.strftime('%Y-%m-%d %H:%M:%S'), message))

class MongoCluster:

    # pymongo connection
    conn = None

    # connection string
    url = ""

    def __init__(self, url):
        self.url = url

    def connect(self):
        self.conn = pymongo.MongoClient(self.url)

    def close(self):
        self.conn.close()


def filter_check(m):
    new_m = {}
    for k in CheckList:
        new_m[k] = m[k]
    return new_m

"""
    check meta data. include db.collection names and stats()
"""
def check(src, dst):

    #
    # check metadata 
    #
    srcDbNames = src.conn.list_database_names()
    dstDbNames = dst.conn.list_database_names()
    srcDbNames = [db for db in srcDbNames if db not in configure[EXCLUDE_DBS]]
    dstDbNames = [db for db in dstDbNames if db not in configure[EXCLUDE_DBS]]
    if len(srcDbNames) != len(dstDbNames):
        log_error("DIFF => database count not equals src[%s] != dst[%s].\nsrc: %s\ndst: %s" % (len(srcDbNames),
                                                                                              len(dstDbNames),
                                                                                              srcDbNames,
                                                                                              dstDbNames))
        return False
    else:
        log_info("EQUL => database count equals")

    # check database names and collections
    for db in srcDbNames:
        if db in configure[EXCLUDE_DBS]:
            log_info("IGNR => ignore database [%s]" % db)
            continue

        if dstDbNames.count(db) == 0:
            log_error("DIFF => database [%s] only in srcDb" % (db))
            return False

        # db.stats() comparison
        srcDb = src.conn[db] 
        dstDb = dst.conn[db] 
        # srcStats = srcDb.command("dbstats")
        # dstStats = dstDb.command("dbstats")
        #
        # srcStats = filter_check(srcStats)
        # dstStats = filter_check(dstStats)
        #
        # if srcStats != dstStats:
        #     log_error("DIFF => database [%s] stats not equals src[%s], dst[%s]" % (db, srcStats, dstStats))
        #     return False
        # else:
        #     log_info("EQUL => database [%s] stats equals" % db)

        # for collections in db
        srcColls = srcDb.list_collection_names()
        dstColls = dstDb.list_collection_names()
        srcColls = [coll for coll in srcColls if coll not in configure[EXCLUDE_COLLS] and srcColls.count(coll) > 0]
        dstColls = [coll for coll in dstColls if coll not in configure[EXCLUDE_COLLS] and dstColls.count(coll) > 0]
        if len(srcColls) != len(dstColls):
            log_error("DIFF => database [%s] collections count not equals, src[%s], dst[%s]" % (db, srcColls, dstColls))
            return False
        else:
            log_info("EQUL => database [%s] collections count equals" % (db))

        for coll in srcColls:
            if coll in configure[EXCLUDE_COLLS]:
                log_info("IGNR => ignore collection [%s]" % coll)
                continue

            if dstColls.count(coll) == 0:
                log_error("DIFF => collection only in source [%s]" % (coll))
                return False

            srcColl = srcDb[coll]
            dstColl = dstDb[coll]

            # log_info("compare count for collection [%s]" % coll)
            # comparison collection records number
            src_count = srcColl.estimated_document_count()
            dst_count = dstColl.estimated_document_count()
            count_diff = src_count - dst_count
            count_diff_rel = (abs(count_diff) / min(src_count, dst_count)) * 100 if (src_count > 0 and dst_count > 0) else 100
            if src_count != dst_count:
                if count_diff_rel >= configure[COUNT_THRESHOLD]:
                    log_error("DIFF => collection [%s] record count not equals: src[%d], dst[%d], diff[%d]" % (coll, src_count, dst_count, count_diff))
                    return False
                else: 
                    log_info("EQUL => collection [%s] record count matches, diff[%d], diff_rel[%.3f%%]" % (coll, count_diff, count_diff_rel))
            else:
                log_info("EQUL => collection [%s] record count exactly equal" % (coll))

            
            if not configure.get(SKIP_INDEXES, False):
                log_info("compare index for collection [%s]" % coll)
                # comparison collection index number
                src_index_length = len(srcColl.index_information())
                dst_index_length = len(dstColl.index_information())
                if src_index_length != dst_index_length:
                    log_error("DIFF => collection [%s] index number not equals: src[%r], dst[%r], diff[%r]" % (coll, src_index_length, dst_index_length, src_index_length - dst_index_length))
                    return False
                else:
                    log_info("EQUL => collection [%s] index number equals" % (coll))

            # check sample data
            if not data_comparison(srcColl, dstColl, configure[COMPARISION_MODE]):
                log_error("DIFF => collection [%s] data comparison not equals" % (coll))
                return False
            else:
                log_info("EQUL => collection [%s] data comparison exactly eauals" % (coll))

    return True


def doc_comparison_equals(doc, migrated):
    doc_str = json.dumps(doc, default=str)
    migrated_str = json.dumps(migrated, default=str)
    # Skip match in case we have NaNs in the record, since for python nan == nan => false
    if 'NaN' in doc_str or 'NaN' in migrated_str:
        return True
    # both origin and migrated bson is Map . so use ==
    if doc != migrated:
        # log_error("DIFF\n src_record: %s\n dst_record: %s" % (doc_str, migrated_str))
        return False
    return True

"""
    check sample data. comparison every entry
"""
def data_comparison(srcColl, dstColl, mode):
    if mode == "no":
        return True
    elif mode == "sample":
        # srcColl.count() mus::t equals to dstColl.count()
        count = configure[COMPARISION_COUNT] if configure[COMPARISION_COUNT] <= srcColl.estimated_document_count() else srcColl.estimated_document_count()
    else: # all
        count = srcColl.count_documents({})

    if count == 0:
        return True

    rec_count = count
    batch = 1024
    # show_progress = (batch * 1)
    total = 0
    with tqdm(total=count, desc="Data comparison for collection %s" % (srcColl.name), leave=False) as pbar:
        while count > 0:
            # sample a bounch of docs
            docs = list(srcColl.aggregate([{"$sample": {"size":batch}}]))
            ids = [doc["_id"] for doc in docs]
            migrated = dstColl.find({"_id": {"$in": ids}})
            migrated_dict = {doc["_id"]: doc for doc in migrated}
            for doc in docs:
                migrated = migrated_dict.get(doc["_id"], None)
                match = doc_comparison_equals(doc, migrated)
                cnt = 0
                while not match and cnt < 5:
                    # Sleep and try to refresh document, maybe it's been updated in the meantime
                    time.sleep(1)
                    migrated = dstColl.find_one({"_id": doc["_id"]})
                    match = doc_comparison_equals(doc, migrated)
                    cnt += 1
                if not match:
                    doc_str = json.dumps(doc, default=str)
                    migrated_str = json.dumps(migrated, default=str)
                    log_error("DIFF\n src_record: %s\n dst_record: %s" % (doc_str, migrated_str))
                    return False
            total += len(docs)
            count -= len(docs)
            pbar.update(len(docs))

            # if total % show_progress == 0:
            #     log_info("  ... process %d docs, %.2f %% !" % (total, total * 100.0 / rec_count))
            

    return True


def usage():
    print('|------------------------------------------------------------------------------------------------------------------------------------------------------------------------|')
    print("| Usage: ./comparison.py --src=localhost:27017/db? --dest=localhost:27018/db? --count=10000 (the sample number) --excludeDbs=admin,local --excludeCollections=system.profile --comparisonMode=sample/all/no (sample: comparison sample number, default; all: comparison all data; no: only comparison outline without data)  |")
    print('|------------------------------------------------------------------------------------------------------------------------------------------------------------------------|')
    print('| Like : ./comparison.py --src="localhost:3001" --dest=localhost:3100  --count=1000  --excludeDbs=admin,local,mongoshake --excludeCollections=system.profile --comparisonMode=sample  |')
    print('|------------------------------------------------------------------------------------------------------------------------------------------------------------------------|')
    exit(0)

if __name__ == "__main__":
    opts, args = getopt.getopt(sys.argv[1:], "hs:d:n:e:x:", ["help", "src=", "dest=", "count=", "excludeDbs=", "excludeCollections=", "comparisonMode=", "skip-indexes", 'count-threshold='])

    configure[SAMPLE] = True
    configure[EXCLUDE_DBS] = []
    configure[EXCLUDE_COLLS] = []
    configure[SKIP_INDEXES] = False
    configure[COUNT_THRESHOLD] = 0
    srcUrl, dstUrl = "", ""

    for key, value in opts:
        if key in ("-h", "--help"):
            usage()
        if key in ("-s", "--src"):
            srcUrl = value
        if key in ("-d", "--dest"):
            dstUrl = value
        if key in ("-n", "--count"):
            configure[COMPARISION_COUNT] = int(value)
        if key in ("-e", "--excludeDbs"):
            configure[EXCLUDE_DBS] = value.split(",")
        if key in ("-x", "--excludeCollections"):
            configure[EXCLUDE_COLLS] = value.split(",")
        if key in ("--comparisonMode"):
            print(value)
            if value != "all" and value != "no" and value != "sample":
                log_info("comparisonMode[%r] illegal" % (value))
                exit(1)
            configure[COMPARISION_MODE] = value
        if key in ("--skip-indexes"):
            configure[SKIP_INDEXES] = True
        if key in ("--count-threshold"):
            configure[COUNT_THRESHOLD] = float(value)
    if COMPARISION_MODE not in configure:
        configure[COMPARISION_MODE] = "sample"

    # params verify
    if len(srcUrl) == 0 or len(dstUrl) == 0:
        usage()

    # default count is 10000
    if configure.get(COMPARISION_COUNT) is None or configure.get(COMPARISION_COUNT) <= 0:
        configure[COMPARISION_COUNT] = 10000

    # ignore databases
    configure[EXCLUDE_DBS] += ["admin", "local"]
    configure[EXCLUDE_COLLS] += ["system.profile"]

    # dump configuration
    log_info("Configuration [sample=%s, count=%d, excludeDbs=%s, excludeColls=%s, skipIndexes=%s]" % (configure[SAMPLE], configure[COMPARISION_COUNT], configure[EXCLUDE_DBS], configure[EXCLUDE_COLLS], configure[SKIP_INDEXES]))

    try :
        src, dst = MongoCluster(srcUrl), MongoCluster(dstUrl)
        print("[src = %s]" % srcUrl)
        print("[dst = %s]" % dstUrl)
        src.connect()
        dst.connect()
    except (Exception, e):
        print(e)
        log_error("create mongo connection failed %s|%s" % (srcUrl, dstUrl))
        exit()

    if check(src, dst):
        print("SUCCESS")
        exit(0)
    else:
        print("FAIL")
        exit(-1)

    src.close()
    dst.close()


