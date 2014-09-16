import logging

LOGFMT = '%(asctime)s %(name)-30s %(levelname)-8s %(message).320s'
logging.basicConfig(level = logging.DEBUG,
                    format = LOGFMT)
import sys
from datetime import datetime, timedelta

#sys.path.append('/home/alan/workspace/')
#sys.path.append('/h')
sys.path.append('..')

#import SensorSystem.SensorAPI as SensorAPI
import SensorSystem.SensorAPI.API as API
import json
import collections
#$import SensorSystem.SensorAPI.Tags as Tags


epoch = datetime(1970, 1, 1)
def timestamp(dt):
    return "{:.0f}".format((dt - epoch).total_seconds())
def fromtimestamp(ts):
    return epoch + timedelta(seconds = int(ts) / 1000.0)


def getDataTable(begin, end):
    #client = SensorAPI.SensorAPI()
    client = API.SensorClient() 
    
    # lookup all metrics
    lookup = json.loads(client.lookup({'m':'*'}))
    
    # metrics is the form metrics['metric name'] = list of sensor_names
    metrics = {}
    for metric in lookup['results']:
        logging.info("Metric: {}, tags: {}".format(metric['metric'], 
                                                   metric['tags']))
        
        if metric['metric'] not in metrics:
            metrics[metric['metric']] = []
            
        name = metric['tags']['sensor_name']
        
        if name not in metrics[metric['metric']]:
            metrics[metric['metric']].append(name) 
            
    for metric, names in metrics.iteritems():
        logging.info ("{}.{}".format(metric, names))
    
    
    agg_avg = API.QueryAggregator.Average
    ds_15m = API.DownSample("15m", agg_avg)
    
    metric = "Table"
    
    data = {}
    units = {}
    for name in metrics[metric]:
        tags = API.Tags(metric)
        tags.addTag("sensor_name", name)
        qr = json.loads(                            
            client.singleQuery(#timestamp(datetime.now()-timedelta(days=1)),
                                # timestamp(datetime.now()+timedelta(minutes=15)),
                                begin,
                                end,
                                 tags = tags,
                                 aggregator = agg_avg,
                                 downSample =ds_15m)     
                        )
                                 
        if 'error' in qr:
            logging.error("ERROR {} - {}".format(qr['error']['code'], qr['error']['message']))
            logging.error("DETAIL: {}".format(qr['error']['details']))
        
        else:
            qr = qr[0] # for some reason the result is packed in a list.. maybe fore multiple queries...
            
            logging.debug("qr={}".format(qr))
            logging.info("got {} points for last hour".format( len(qr['dps'])))
            
            
            if 'sensor_units' in qr['tags']:
                units[name] = qr['tags']['sensor_units']
            else:
                units[name] = ""
            
            #s = sorted(qr['dps'].items())
            #for ts, value in qr['dps'].iteritems():
            for ts, value in qr['dps'].iteritems():   
                #logging.info("Got: {}: {} {}".format(
                #                                     fromtimestamp(ts),
                #                                     value,
                #                                     qr['tags']['sensor_units']
                #                                     ))
                if ts not in data:
                    data[ts] = {}
                data[ts][name] = value
    
    output = []
    
    cols = metrics[metric]
    fmt = {}
    # first output a header
    row = []
    row.append("{:>19s}".format("timestamp"))
    fmt['ts'] = '{:19s}'
    
    max_col_len = 0
    for col in cols:
        s = "{} ({})".format(col, units[col])
        if len(s) > max_col_len:
            max_col_len = len(s)
        
    for col in cols:        
        row.append("{0:>{1}s}".format("{} ({})".format(col, units[col]), max_col_len))
        fmt[col] = "{{:>{0}.8f}}".format(max_col_len)
        
    output.append(", ".join(row))
    
    for ts, data in sorted(data.iteritems()):
        # this will be a single output row
        
        row = []
        row.append(fmt['ts'].format(fromtimestamp(ts).isoformat()))    
        for name in metrics[metric]:
            if name in data:            
                row.append(fmt[name].format(data[name]))
            else:
                output.append(", NaN")
                
        output.append(", ".join(row))
    
        #logging.info("".join(row))
        
    return "\n".join(output)


b = timestamp(datetime.now()-timedelta(days=1))
e = timestamp(datetime.now()+timedelta(minutes=15))

f =open ("data.csv", 'w')
f.write(getDataTable(b, e))
f.close()
