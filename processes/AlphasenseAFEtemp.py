#AlphasenseAFEtemp Sensor Processing

#every process must have required_aux_data and process_data routines


#this tells which extra data are required and which functions to use to process
#a given metric/unit combination for this sensor type
def dispatcher(metric, unit):
    return {
        'temperature_raw': { 'raw': {
            'function': raw_to_temp
            }},
        'temperature': { 'celcius':{
            'extra_data': ['O3_raw_work','O3_raw_aux'],
            'function': temp_to_learned_temp
            }}

            }.get(metric, None)[unit]


def required_aux_data(metric, unit):
    #logic for extra data required by this module: for instance, if we
    #have an alphasense NO2-A4 sensor, if we have a raw working electrode data
    #we also need raw aux electrode data and perhaps raw temp data to make
    #sense of the reading and create a virtual sensor.  This returns a list
    #of required secondary data for the sensor_type of this file, and the metric/unit
    #of that type, so that the main process routine can traverse, find that extra
    #data, and pass it back to our process_data routine
    try:
        return dispatcher(metric, unit)['extra_data']
    except:
        return None


def process_data(data, metric, unit):
    #call logic - depending on metric/unit, call subprocess
    #return processed data and metric/unit to post
    try:
        return dispatcher(metric, unit)['function'](data)
    except:
        return None


#all functions should return [sensor_type, metric, unit, data_to_post]
def raw_to_temp(data):

    new_data = data[0]['data']
    return('test_post_sensortype', 'temperature', 'celcius', new_data)


def temp_to_learned_temp(data):
    pass
