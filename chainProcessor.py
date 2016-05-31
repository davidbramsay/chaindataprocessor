#!/usr/bin/python

from processes import *
import zmq
import sys
import requests
import json
import pkgutil
import logging
from multiprocessing import Process
from chaincrawler import chainCrawler, chainSearch
from chainlearnairdata import chainTraversal


logging.basicConfig(stream=sys.stderr)
log = logging.getLogger(__name__)
log.setLevel(logging.DEBUG)
log.propagate = 0
ch = logging.StreamHandler()
ch.setLevel(logging.INFO)
log.addHandler(ch)


##
#CREATE CRAWLER PROCESS THAT SEARCHES FOR SENSORS AND PUSHES THEM OVER ZMQ
##

def create_crawler_process(socket="tcp://127.0.0.1:5557",
        namespace='http://learnair.media.mit.edu:8000/rels/', criteria=None):

    return Process(target=crawler_spawn, args=(socket, namespace, criteria))


def crawler_spawn(socket, namespace, criteria):
    #TODO: currently the crawler only supports finding one type of object
    # it would be great to pass a list of objects we'd like to find instead of
    # crawling for one particular type.
    # Instead of just crawling for sensors, crawl for list of process names

    crawler = chainCrawler.ChainCrawler(entry_point='http://learnair.media.mit.edu:8000/sites/3')

    if criteria is not None:
        crawler.crawl_zmq(socket=socket, namespace=namespace, resource_extra=criteria)
    else:
        crawler.crawl_zmq(socket=socket, namespace=namespace, resource_type='Sensor')


##
#CREATE MAIN PROCESS THAT RECEIVES SENSOR URIS, CHECKS THEM AGAINST THE PROCESSES
#WE HAVE TO RUN, SENDS DATA TO SECONDARY PROCESS, AND PUBLISHES DATA FROM SECONDARY
#PROCESS TO 'VIRTUAL' SENSORS
##

def create_main_process(socket="tcp://127.0.0.1:5557"):
    return Process(target=main_spawn, args=(socket,))


def main_spawn(socket):

    #get list of names of processes (should be names of sensors we're interested in)
    processes = [name for _, name, _ in pkgutil.iter_modules(['processes'])]

    context = zmq.Context()
    zmqReceive = context.socket(zmq.PULL)
    zmqReceive.connect(socket)

    while(1):

        uri = zmqReceive.recv_string()

        #retrieve uri, put into json
        res_json = get_json_from_uri(uri)

        #check if sensor_type matches a process
        process = check_sensor_type_has_process(res_json, processes)

        metric = get_attribute(res_json, 'metric')
        unit = get_attribute(res_json, 'unit')

        if process is None or metric is None or unit is None:
            continue

        #check if process requires extra data
        aux_data = globals()[process].required_aux_data(metric, unit)
        print 'auxdata is %s' %aux_data
        #get required data using traversal
        traveler = chainTraversal.ChainTraversal(entry_point=uri)
        data = []
        data.append({'from':'main', 'data':traveler.get_all_data()})

        if aux_data is not None:
            searcher = chainSearch.ChainSearch(entry_point=uri)
            for title in aux_data:
                found = searcher.find_first(resource_title=title)
                if found:
                    traveler = chainTraversal.ChainTraversal(entry_point=found[0])
                    data.append({'from':title, 'data':traveler.get_all_data()})

        #call process_data on data from sensor
        publish_vals = globals()[process].process_data(data, metric, unit)

        #publish any data returned from subprocess
        if publish_vals is not None:
            searcher = chainSearch.ChainSearch(entry_point=uri)
            found = searcher.find_first(resource_type='device',
                    namespace='http://learnair.media.mit.edu:8000/rels/')

            if found:
                traveler = chainTraversal.ChainTraversal(entry_point=found[0])

                traveler.add_and_move_to_resource('Sensor',
                        {'sensor_type': publish_vals[0],
                        'metric': publish_vals[1],
                        'unit': publish_vals[2]} )

                traveler.safe_add_data(publish_vals[3])

            else:
                log.warn("can't find device to publish data to")
        else:
            log.info('no values to publish')


def get_attribute(json, field):
    try:
        return json[field]
    except:
        return None


def get_json_from_uri(uri):

    try:
        req = requests.get(uri)
        log.info( '%s downloaded.', uri )
        return req.json()

    except requests.exceptions.ConnectionError:
        log.warn( 'URI "%s" unresponsive', uri )
        return None


def check_sensor_type_has_process(res_json, processes):
    #return process or none if no process for this sensor exists
    try:
        sensor_type = res_json['sensor_type']

        for process in processes:
            if sensor_type.lower() == process.lower():
                log.info('sensor_type %s matches a process', process)
                return process

        log.info('sensor_type %s does not match any process', sensor_type)
        return None

    except:
        log.warn('no sensor_type detected')
        return None




if __name__=='__main__':
    socket="tcp://127.0.0.1:5557"

    p1 = create_main_process(socket)
    p2 = create_crawler_process(socket)

    p1.start()
    p2.start()

