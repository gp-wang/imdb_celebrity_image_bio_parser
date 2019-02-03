import datetime
import requests
import json
import random
import logging

logger = logging.getLogger("cl_request")
logger.setLevel(logging.INFO)

ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)

formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

ch.setFormatter(formatter)
logger.addHandler(ch)

PROXY_POOL_URI='http://192.168.0.105:8899/api/v1/proxies?https=true&limit=9999'

REFRESH_PERIOD=60               # in sec

PROXY_POOL = []

last_update_tick = 0

MAX_TRIES = 3


def get_proxy_list():
    global last_update_tick, PROXY_POOL, REFRESH_PERIOD, PROXY_POOL_URI
    
    current_time_tick = datetime.datetime.now().timestamp()
    if last_update_tick == 0 or current_time_tick - last_update_tick > REFRESH_PERIOD:
        PROXY_POOL = json.loads(requests.get(PROXY_POOL_URI).content)['proxies']
        last_update_tick = current_time_tick
    return PROXY_POOL


def random_get_proxy():
    """
    return: one proxy from proxy pool randomly
    """
    proxy_pool_list = get_proxy_list()
    
    one_proxy =  proxy_pool_list[random.randint(0, len(proxy_pool_list) - 1)]
    # e.g. {"id":96,"ip":"36.67.95.131","port":30419,"is_valid":true,"created_at":1539290634,"updated_at":1539291007,"latency":248.0,"stability":0.9,"is_anonymous":true,"is_https":true,"attempts":1,"https_attempts":1,"location":"-6.1744,106.8294","organization":"AS17974 PT Telekomunikasi Indonesia","region":"Jakarta Raya","country":"ID","city":"Jakarta"},
    proxy_str = "http://{}".format(one_proxy['ip'])

    if one_proxy['port']:
        proxy_str = proxy_str + ":{}".format(one_proxy['port'])

    return {'https' : proxy_str}

def proxied_get_https_url(url, timeout, params={}, headers={}):
    global MAX_TRIES
    count = 0
    while count < MAX_TRIES:
        count = count + 1
        try:
            proxies = random_get_proxy() # gw: it is named "proxiES", but actually just one proxy (plural coz usually specify one for htttp and one for https)
            logger.debug("parsed url: {} using proxy: {}".format(url, proxies))
            return requests.get(url, proxies=proxies, timeout=timeout, params=params, headers=headers)
        except:
            pass

    return None
        


if __name__ == '__main__':
    logger.debug("get one proxy {}".format(random_get_proxy()))


    url = 'https://sfbay.craigslist.org/search/ata'

    response = proxied_get_https_url(url)
    logger.debug('response content is: ' + str(response.content))

    
