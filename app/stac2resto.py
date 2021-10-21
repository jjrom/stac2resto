#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# STAC ingester for resto
#
# Takes an input STAC catalog root url and ingest items recursively into target resto endpoint
# 

import sys
import json
import re
import requests
import configparser
import urllib3
import time
import os 
import subprocess
import validators
import signal
import logging

from colorlog import ColoredFormatter

from environs import Env
from requests.adapters import HTTPAdapter
from requests.auth import HTTPBasicAuth
from urllib3.util import Retry

from tqdm import tqdm

# ########### Usage ##########################################################
def usage():
    print(""" 
    Takes an input STAC catalog root url and ingest items recursively into target resto endpoint

    Usage: 
    
        ./stac2resto.py <stac_url> <resto_url>

    Where:

        [MANDATORY] stac_url is either:
            * a local catalog endpoint (e.g. /data/catalog.json)
            * a remote catalog endpoint (e.g. https://tamn.snapplanet.io)

        [MANDATORY] resto_url is a resto root endpoint (e.g. http://localhost:5252)

    """)

def kill_handler(signum, frame):
    global KILL_ME
    KILL_ME = True
    sys.exit(1)

# ############ helper classes/functions ####################################

#
# Process catalog url/path
#
def process_stuff(url):

    if KILL_ME == True:
        sys.exit(1)

    # Check between url and path for parsing
    if validators.url(url) is True:
        stuff = read_remote_json(url)
    else:
        try:
            if os.path.exists(url) is False:
                logger.warning("  File %s does not exist - skipping", url)
                return    
            f = open(url)
            stuff = json.load(f)
            f.close()
        except:
            logger.warning("  Cannot open file %s - skipping", url)
            f.close()
            return

    # Not STAC - skip
    if stuff is None or "type" not in stuff:
        logger.warning("  Invalid stac file %s - skipping", url)
        return

    # Post Feature and skip
    if stuff["type"] == "Feature":
        post_feature(stuff)
        return

    # Post FeatureCollection and skip
    if stuff["type"] == "FeatureCollection":
        post_features(stuff)
        return

    # Collection - post collection but continue to explore links after
    if stuff["type"] == "Collection":
        post_collection(stuff)

    # No links skip
    if isinstance(stuff["links"], list) is False:
        logger.warning("  No links to process - skipping")
        return
    
    # Recursively process links
    for link in stuff["links"]:

        # Compute the absolute child url
        child_url = get_absolute_url(url, link["href"])

        if link["rel"] in ["child", "item", "items"]:

            if link["rel"] == "child":
                logger.debug("Process %s" % child_url)

            process_stuff(child_url)   

#
# Compute an absolute url
#
def get_absolute_url(rootUrl, url):
    if validators.url(rootUrl) is True:
        if validators.url(url) is False:
            return urllib3.parse.urljoin(rootUrl, url)
        else:
            return url   
    else:
        return os.path.abspath(os.path.join(os.path.dirname(rootUrl), url))  

#
# Read remote json @ url
#
def read_remote_json(url):

    try:
        stuff = session.get(url, timeout = DEFAULT_TIMEOUT, headers=headers, verify=SSL_VERIFY).json()
    except:
        stuff = None
    return stuff


#
# POST a single item to resto
# Example of url: https://explorer.dea.ga.gov.au/collections/s2a_ard_granule/items/0f29a83f-9808-4f39-a822-1accc6085e61
#
def post_collection(collection):

    logger.info("  POST Collection %s to %s" % (collection["id"], RESTO_URL + '/collections'))
    #resp = session.post("%s/collections" % (RESTO_URL), json=collection, headers=RESTO_HEADERS, verify=SSL_VERIFY)
    resp = requests.post("%s/collections" % (RESTO_URL), data=collection, headers=RESTO_HEADERS, verify=SSL_VERIFY)
    logger.debug(resp.json())


#
# POST a single item to resto
# Example of url: https://explorer.dea.ga.gov.au/collections/s2a_ard_granule/items/0f29a83f-9808-4f39-a822-1accc6085e61
#
def post_feature(feature):
    logger.info("  POST Feature %s to %s" % (feature["id"], RESTO_URL + '/collections/' + feature["collection"] + '/items'))
    return
    headers = {
        'Content-Type': 'application/json',
        'Accept': 'application/json',
        'User-Agent': 'dea-access-resto-ingester/v1'
    }
    collection_id = re.search(r".*/collections/(.*)/items/.*", url).group(1)
    feature_id = re.search(r".*/items/(.*)$", url).group(1)
    feature = session.get(url, timeout = DEFAULT_TIMEOUT, headers=headers, verify=SSL_VERIFY, auth=HTTPBasicAuth(API_USER, API_PASSWORD)).json()
    resp = session.post("%s/collections/%s/items" % (RESTO_URL, collection_id), json=feature, headers=headers, verify=SSL_VERIFY, auth=HTTPBasicAuth(API_USER, API_PASSWORD))

    logger.debug(resp.json())

#
# POST a FeatureCollection to resto
# Example of url: https://explorer.dea.ga.gov.au/collections/s2a_ard_granule/items
# 
def post_features(url, max_features=20):

    logger.info("  POST Features")
    return

    post_features.next_item_url = url
    # From api https://resto/api.html#resto-feature
    headers = {
        'Content-Type': 'application/json',
        'Accept': 'application/json',
        'User-Agent': 'dea-access-resto-ingester/v1'
    }
    collection_id = re.search(r".*/collections/(.*)/items$", url).group(1)
    resto_items_url = "%s/collections/%s/items" % (RESTO_URL, collection_id)

    processed = 0
    while True:
        start_time = time.time()
        logging.info("Retrieve FeatureCollection from: %s" % (post_features.next_item_url))
        try:
            feature_col = session.get(post_features.next_item_url, timeout = DEFAULT_TIMEOUT, headers=headers, verify=SSL_VERIFY, auth=HTTPBasicAuth(API_USER, API_PASSWORD)).json()
            numfeat     = int(feature_col["context"]["returned"])
            logging.info("Retrieved %s features" % (numfeat))
        except requests.ConnectionError as e:
            if re.search('Max retries', str(e), re.IGNORECASE):
                logging.error("Read timed out. Max retries exceeded with url: %s" % e.request.url)
            else:
                logging.error("ConnectionError for URL %s: %s" % (e.request.url, e))
            break
        except requests.RequestException as e:
            logging.error("RequestException for URL %s: %s" % (e.request.url, e))
            break
        else:
            try:

                count = 0
                # POST feature one by one to avoid memory issue with very large FeatureCollection
                for feature in tqdm(feature_col["features"]):
                    try:
                        resp = session.post("%s/collections/%s/items" % (RESTO_URL, collection_id), json=feature, headers=headers, verify=SSL_VERIFY, auth=HTTPBasicAuth(API_USER, API_PASSWORD))
                        resp.raise_for_status()
                        logging.debug(resp.json())
                    except requests.HTTPError as e:
                        if re.search('409 Client Error', str(e), re.IGNORECASE):
                            pass
                        else:
                            logging.error("%s" % str(e))
                            raise SystemExit(1)
                    finally:
                        processed = processed + 1
                        count = count + 1
                        if max_features != -1 and processed >= max_features:
                            break
            except:
                logging.error("POST feature failed")
                break
            else:
                end_time = time.time() - start_time
                numsec = numfeat/end_time
                logging.info("%s features posted to resto (collection: %s) at %s item/s" % (str(count), collection_id, numsec))

                # Stop on page_limit reached
                if max_features != -1 and processed >= max_features:
                    logging.info("Maximum number of features (%s) reached for collection %s" % (str(processed), collection_id))
                    break

                # Search next link if any, or end posting feature
                post_features.next_item_url = None
                for link in feature_col["links"]:
                    if link["rel"] == "next":
                        post_features.next_item_url = link["href"]
                        break

                if post_features.next_item_url is None:
                    logging.info("No more items for collection %s" % (collection_id))
                    break


# ############ end functions ##############################################

# CTRL-C handler
signal.signal(signal.SIGINT, kill_handler)

# simple command line management:
if len(sys.argv) < 3 or len(sys.argv) > 4:
    usage()
    exit(1)

# To kill the process on CTRL-C even in the loop
KILL_ME = False

STAC_URL  = sys.argv[1] 
RESTO_URL = sys.argv[2]
RESTO_ADMIN_AUTH_TOKEN = os.environ.get("RESTO_ADMIN_AUTH_TOKEN")

DEFAULT_TIMEOUT = 45 # seconds
DEVEL           = os.getenv('DEVEL', 'False').lower() in ('true', '1', 't')
SSL_VERIFY      = True

env             = Env()

# Color logging
LOG_LEVEL = logging.DEBUG if DEVEL == True else logging.INFO 
LOGFORMAT = "%(log_color)s%(levelname)-8s%(reset)s | %(log_color)s%(message)s%(reset)s"
#LOGFORMAT = "[%(log_color)s%(levelname)s%(reset)s] %(log_color)s%(message)s%(reset)s"
#LOGFORMAT = "%(asctime)s - %(name)s - [%(levelname)s] %(message)s (%(filename)s:%(lineno)d)"
logging.root.setLevel(LOG_LEVEL)
formatter = ColoredFormatter(LOGFORMAT)
stream = logging.StreamHandler()
stream.setLevel(LOG_LEVEL)
stream.setFormatter(formatter)
logger = logging.getLogger('stac2resto')
logger.setLevel(LOG_LEVEL)
logger.addHandler(stream)

if STAC_URL is None:
    logger.error("Missing mandatory input STAC_URL")
    usage()

if RESTO_URL is None:
    logger.error("Missing mandatory target RESTO_URL")
    usage()

if RESTO_ADMIN_AUTH_TOKEN is None:
    logger.error("Missing mandatory environement variable RESTO_ADMIN_AUTH_TOKEN")
    usage()

if DEVEL:
    SSL_VERIFY=False
    logger.debug("Devel set to %s" % DEVEL)
    logger.debug("Disable warnings for insecure ssl request in urllib3")
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    logger.debug("Setting requests verify to %s", SSL_VERIFY )

RETRY_STRATEGY = Retry(
    total=50,
    backoff_factor=2,
    status_forcelist=[ 429, 500, 502, 503, 504 ], # Retry on these error codes. Might have to add more in future. depends on explorer API errors.
    method_whitelist=[ "HEAD", "GET", "POST" ]
)

RESTO_HEADERS = {
    'Content-Type': 'application/json',
    'Accept': 'application/json',
    'User-Agent': 'stac2resto',
    'Authorization': 'Bearer ' + RESTO_ADMIN_AUTH_TOKEN
}

# post items to resto according to item type: item, items, collection, etc.
logger.debug("Input STAC endpoint is %s " % (STAC_URL))
logger.debug("Target resto endpoint is %s " % RESTO_URL)
logger.debug("RESTO_ADMIN_AUTH_TOKEN is set to %s " % RESTO_ADMIN_AUTH_TOKEN)

session = requests.Session()
adapter = HTTPAdapter(max_retries=RETRY_STRATEGY)
session.mount(RESTO_URL.split('://')[0] + '://', adapter)

# Iterative process
logger.debug("Process %s" % STAC_URL)
process_stuff(STAC_URL)
