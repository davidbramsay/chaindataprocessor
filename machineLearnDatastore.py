#!/usr/bin/python

import pymongo
from dateutil import parser
import math
import datetime
import sys
import logging

logging.basicConfig(stream=sys.stderr)
log = logging.getLogger(__name__)
log.setLevel(logging.DEBUG)
log.propagate = 0
ch = logging.StreamHandler()
ch.setLevel(logging.INFO)
log.addHandler(ch)


class machineLearnMongo(object):

    def __init__(self, db='learnair'):
        '''initialize mongo and our learnair database'''
        client = pymongo.MongoClient('localhost', 27017)
        self.db = client[db]

        self.create_conditions_collection()


    def create_indexed_collection(self, collection_name):
        '''initialize collection with timestamp/lat/lon unique index'''
        self.current_collection = self.db[collection_name]

        self.current_collection.create_index( [('timestamp', pymongo.ASCENDING),
            ('lat', pymongo.DESCENDING),
            ('lon', pymongo.DESCENDING)], unique=True )


    def create_conditions_collection(self):
        '''initialize the conditions database'''
        self.create_indexed_collection('conditions')


    def switch_to_collection(self, collection_name, create_if_nonexist=True):
        '''switch to another collection'''
        if collection_name in self.db.collection_names():
            self.current_collection = self.db[collection_name]
        elif create_if_nonexist:
            log.info('created collection %s with timestamp/lat/lon index', collection_name)
            self.create_indexed_collection(collection_name)
        else:
            log.warn('could not find collection %s, operation failed', collection_name)


    def add_data_to_collection(self, collection_name, data):
        '''attempt to add each piece of data, if timestamp/lat/lon not unique
        then add to existing.  Data should be formed as [ {'timestamp':x,
        'lat':y, 'lon':z, 'fieldtoadd':xyz}, {'timestamp':x, 'lat':y,'lon':z,
        'fieldtoadd':xyz} ]'''

        #TODO: slow/unoptimized check of possible timestamp lables on every
        # iteration to change to 'timestamp'.  Could be optimized so check only
        # done once.

        for d in data:

            possible_timestamp_labels= ['UTC','utc','Timestamp']

            for label in possible_timestamp_labels:
                try:
                    d['timestamp'] = d.pop(label)
                except:
                    pass

            try:
                d['timestamp'] = self.make_dt(d['timestamp'])
            except:
                log.warn('no timestamp found in data %s', d)

            try:
                self.db[collection_name].update(
                        {   'timestamp': d['timestamp'],
                            'lat': d['lat'],
                            'lon': d['lon'] },
                        { "$set":d },
                        upsert=True )
            except:
                log.warn('failed to add %s', d)


    def add_conditions(self, data):
        self.add_data_to_collection('conditions', data)


    def add_data_to_current_collection(self, data):
        self.add_data_to_collection(self.current_collection.name, data)


    def return_ml_array(self, collection_name=None, conditions=None, measure=None,
            extra_conditions=None, update_conditions_first=True, time_range=30,
            lat_lon_range=1, loc_then_time=True, return_diffs=True):
        '''
        pass a collection_name for the db collection that will the 'measure', as
        well as fields for conditions and measure arrays. If 'None' is specified,
        all values will be returned from these arrays. If a subset is desired, only that
        subset will be returned.  Matches and concatenates data from 'conditions'
        table and 'collection_name' table, using the time_range (seconds) and
        lat_lon_range (degrees).  Adds metadata for diff in lat/lon/time and distance.
        If update_conditions_first is true, call the APIs to update conditions in
        condition array before returning anything.
        conditions and measure variables should be arrays of keys ['keya', 'keyb']
        extra_conditions should be dict in form {"collection_name": ['keya', 'keyb'],
        "collection_name2": None}.  None will pull all values from that array into
        the conditions.
        This function will return a list of dicts, each dict being one training
        example, with the following form:
        [{'conditions':{'keya':val, 'keyb':val}, 'measures':{'keya':val, 'keyb':val}},
         {'conditions':{'keya':val, 'keyb':val}, 'measures':{'keya':val, 'keyb':val}},
         {'conditions':{'keya':val, 'keyb':val}, 'measures':{'keya':val, 'keyb':val}},
         {'conditions':{'keya':val, 'keyb':val}, 'measures':{'keya':val, 'keyb':val}}]
        '''

        if update_conditions_first:
            self.update_conditions_from_api()

        if collection_name is None:
            db = self.current_collection
        else:
            db = self.db[collection_name]


        results=[]

        for doc in db.find({}): #step through each doc in the collection_name db

            this_result = {'conditions':{}, 'measures':{}}

            try:
                #try to get matching conditions for time/geotag given the range
                con = self.get_values_in_range('conditions', doc['timestamp'],
                        doc['lat'], doc['lon'], time_range,
                        lat_lon_range, loc_then_time, return_diffs)


                if con is not None: #found conditions that match the measure
                    #add values to result array

                    #take subset of keys if we specify only certain keys
                    if conditions is not None:
                        unwanted = set(con.keys()) - set(conditions)
                        for key in unwanted: del con[key]

                    if measure is not None:
                        unwanted = set(doc.keys()) - set(measure)
                        for key in unwanted: del doc[key]

                    #add extra_conditions to con
                    if extra_conditions is not None:
                        for key, val in extra_conditions.iteritems():
                            try:
                                extra = self.get_values_in_range(key, doc['timestamp'],
                                        doc['lat'], doc['lon'], time_range,
                                        lat_lon_range, loc_then_time, False)

                                if val is not None:
                                    unwanted = set(extra.keys()) - set(val)
                                    for unwanted_key in unwanted: del extra[unwanted_key]

                                con.update(extra)

                            except:
                                log.warn('error adding extra conditions %s', key)


                    #overwrite any common keys in con with keys from measures
                    #and remove from measures
                    overlapping_keys = set(con.keys()).intersection(set(doc))
                    for key in overlapping_keys:
                        con[key] = doc[key]
                        del doc[key]

                    #remove mongo document ID
                    try:
                        del con['_id']
                        del doc['_id']
                    except:
                        pass

                    #update full results
                    this_result['conditions'] = con
                    this_result['measures'] = doc

                    results.append(this_result)
                    log.debug('appended %s', this_result)

                else: #no matching conditions found at timestamp/geotag
                    log.warn('could not find matching conditions for this measurement: %s', doc)


            except: #something broke when accessing the conditions db
                log.warn('error accessing condition db for this measurement: %s', doc)

        return results


    def get_values_in_range(self, collection_name, timestamp, lat, lon,
            time_range=30, lat_lon_range=1,
            loc_then_time=True, return_diffs=True):
        '''return one document from collection_name that is the closest fit
        to timestamp, lat/lon in the ranges specified (within 30 seconds of
        timestamp, within 1 degree of lat AND 1 degree of lon by default).
        If there are no documents in this range, return None.
        Sorts by location then time if loc_then_time is true, otherwise sorts
        by time first. return_diffs will add the difference in lat/lon/time
        to the returned array if true.'''

        #first try to access the exact timestamp/lat/lon
        result = self.db[collection_name].find_one({'timestamp':self.make_dt(timestamp),
                'lat':lat,
                'lon':lon })

        if result is not None:
            if return_diffs:
                result['lat_diff'] = 0
                result['lon_diff'] = 0
                result['distance'] = 0
                result['time_diff'] = datetime.timedelta()

            log.info('FIND_IN_RANGE: perfect match found.')
            return result

        else:
            #then pull any in range
            time_change = datetime.timedelta(seconds=time_range)
            result = self.db[collection_name].find({
                'timestamp':{ "$gte": self.make_dt(timestamp) - time_change,
                            "$lte": self.make_dt(timestamp) + time_change },
                'lat':{ "$gte": lat - lat_lon_range,
                        "$lte": lat + lat_lon_range },
                'lon':{ "$gte": lon - lat_lon_range,
                        "$lte": lon + lat_lon_range } })

            if result.count() == 0:
                log.info('FIND_IN_RANGE: no match found in range.')
                return None

            #then choose the 'closest' in location and time
            final_result = {}

            if loc_then_time: #choose closest by location
                distance = float('+inf')
                for doc in result:
                    cur_distance = (doc['lat'] - lat)**2 + (doc['lon'] - lon)**2
                    if cur_distance < distance:
                        distance = cur_distance
                        final_result = doc

            else: #choose closest by time
                time_diff = datetime.timedelta.max
                for doc in result:
                    cur_time_diff = abs(self.make_dt(timestamp) - doc['timestamp'])
                    if cur_time_diff < time_diff:
                        time_diff = cur_time_diff
                        final_result = doc

            log.info('FIND_IN_RANGE: found a match, diff %s lat, %s long, %s time.',
                    final_result['lat']-lat,
                    final_result['lon']-lon,
                    final_result['timestamp'] - self.make_dt(timestamp)
                    )

            if return_diffs:
                final_result['lat_diff'] = float(final_result['lat']-lat)
                final_result['lon_diff'] = float(final_result['lon']-lon)
                final_result['distance'] = math.sqrt(final_result['lat_diff']**2 + final_result['lon_diff']**2)
                final_result['time_diff'] = final_result['timestamp'] - self.make_dt(timestamp)

            return final_result


    def update_conditions_from_api(self):
        '''run through documents in conditions, call API using timestamp/geotag
        data, and update/add api data_fields into the conditions database'''
        pass


    def print_collection(self, collection_name):
        print '> %s COLLECTION' % collection_name.upper()
        for doc in self.db[collection_name].find({}):
            print doc


    def print_conditions(self):
        self.print_collection('conditions')


    def print_current_collection(self):
        self.print_collection(self.current_collection.name)


    def drop_collection(self, collection_name):
        self.db.drop_collection(collection_name)


    def drop_conditions(self):
        self.drop_collection('conditions')


    def drop_current_collection(self):
        self.drop_collection(self.current_collection.name)


    def get_collection_data(self, collection_name, query={}):
        return self.db[collection_name].find(query)


    def get_conditions_data(self, query={}):
        return self.get_collection_data('conditions', query)


    def get_current_collection_data(self, query={}):
        return self.get_collection_data(self.current_collection.name, query)


    @staticmethod
    def make_dt(timestamp):
        '''check if datetime is a correctly formated datetime object, if not
        cast it to a datetime object.  return the proper object'''

        if ( type(timestamp) is datetime.datetime ):
            return timestamp
        else:
            try:
                return parser.parse(timestamp)
            except:
                log.warn('could not parse timestamp string %s', timestamp)
                return None



if __name__ == "__main__":

    a = machineLearnMongo()
    a.switch_to_collection('test_measure1')
    a.print_conditions()
    a.add_conditions([
            {'timestamp':'5/23/16 4:30','lat':40,'lon':50, 'testg':5},
            {'timestamp':'5/23/16 4:34','lat':40,'lon':50, 'testh':6},
            {'timestamp':'5/23/16 4:30','lat':45,'lon':50, 'testi':7},
            {'timestamp':'5/23/16 4:34','lat':45,'lon':50, 'testj':8} ])
    #print a.get_values_in_range('conditions', '5/23/16 4:33:31', 39, 55,
    #        time_range=10*60, lat_lon_range=10, loc_then_time=True)
    a.add_data_to_current_collection([
            {'timestamp':'5/23/16 4:30','lat':40,'lon':50, 'testk':9},
            {'timestamp':'5/23/16 4:34','lat':40,'lon':50, 'testl':10},
            {'timestamp':'5/23/16 4:30','lat':45,'lon':50, 'testm':11},
            {'timestamp':'5/23/16 4:34','lat':45,'lon':50, 'testn':12} ])
    a.print_conditions()
    a.print_current_collection()
    print a.return_ml_array()[1]
