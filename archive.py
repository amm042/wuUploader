import logging
LOGFMT = '%(asctime)s %(name)-30s %(levelname)-8s %(message).320s'
logging.basicConfig(level = logging.DEBUG,
                    format = LOGFMT)
log = logging.getLogger()
import datetime
import requests
import json
import pytz
import urlparse
import os
import os.path

server_url = 'http://amm-csr2:4242/api'
  

def convert_time(ts):
    if isinstance(ts, datetime.date):
        ts = datetime.datetime(ts.year, ts.month, ts.day)
    return int((ts - datetime.datetime(1970,1,1) +
                datetime.timedelta(hours=4)).total_seconds())

def unconvert_time(uxtime):
    return datetime.datetime(1970,1,1,tzinfo=pytz.timezone('US/Eastern')) + \
            datetime.timedelta(seconds=uxtime) - \
            datetime.timedelta(hours=4)


def get_tags_from_metric(m):
    r = requests.get("{}/search/lookup?m={}".format(server_url, m))
    
    if r.status_code == requests.codes.ok:    
        rsp = json.loads(r.text)
        log.debug(rsp)
        if len(rsp['results']) == 0: # metric has no tags
            return []
        else:
            tags = []
            for result in rsp['results']:
                tags += result['']                    
    else:
        log.error("Error processing request")
        log.error(r.text)
    
        
# this gets all the metrics:
#r = requests.get("{}/suggest?type=metrics&q=&max=999999".format(server_url))


def get_day(day, 
            span = datetime.timedelta(days=1), 
            outdir = './archive',
            downsample = None):
    
    # construct output filename, to see if we already have the file
    if not os.path.exists(outdir):
        os.mkdir(outdir)
    
    if downsample:
        resolution = downsample
    else:
        resolution = "raw"
    
    full_path = os.path.join(outdir,                                 
                             str(day.year),
                             resolution)
    if not os.path.exists(full_path):
        os.makedirs(full_path)    
    outputfilename = os.path.join(full_path, '{}-data-{}.csv'.format(day.isoformat(), resolution))
    
    if os.path.exists(outputfilename):
        logging.info("Skipping, file already exists: {}".format(outputfilename))
        return
    
    queries = [{
                'metric': 'Table',
                'aggregator': 'avg',
                'downsample': downsample,
                'tags': {'sensor_name': x} 
                } for x in ['AirTemp', 'RelHum', 'WaterLevel', 'WindDir', 'WindSpeed', 'Solar_kW', 'BattV', 'WaterTemp', 'PTemp_C', 'Rain_Tot', 'Solar_MJ_Tot']]
    queries += [{
                'metric': 'CR800_1',
                'aggregator': 'avg',
                'downsample': downsample,
                'tags': {'sensor_name': x}  
                } for x in ['WaterLevel', 'WaterTemp', 'PTemp_C', 'BattV']]
    
    queries += [{
                'metric': 'CR800_2',
                'aggregator': 'avg',
                'downsample': downsample,
                'tags': {'sensor_name': x}  
                } for x in ['WaterLevel', 'WaterTemp', 'PTemp_C', 'BattV']]
    
    queries += [{
                'metric': 'lwbp1',
                'aggregator': 'avg',
                'downsample': downsample,
                }]          
    begin = day
    end = begin + span
    
    query = {"start": convert_time(begin),
             "end": convert_time(end),
             "queries": queries}
    url = "{}/{}".format(server_url, "query")
    
    log.info("requesting: {}, data: {}".format(url, 
                                               json.dumps(query)))
    r = requests.post(url, 
                      data=json.dumps(query),
                      timeout = 60*60) # set timeout to 1 hr
    
    rename = {"Table": "WeatherStation"}
    
    if r.status_code == requests.codes.ok:
        
        text = r.text
        rslt = json.loads(text)    
        
        rows = {} ## key is the timestamp
        #cols= ['Timestamp']
        cols = []
            
        for res in rslt:
                     
            if 'sensor_name' in res['tags']:
                m = res['metric']
                if m in rename:
                    m = rename[m]
                    
                name = "{}.{} ({})".format(m, res['tags']['sensor_name'], res['tags']['sensor_units'])
            elif res['metric'] == 'lwbp1':
                name = "{} ({})".format(res['metric'], res['tags']['units'])
                
            cols.append(name)
            
        log.info(cols)
        
        for res in rslt:
                   
            if 'sensor_name' in res['tags']:
                m = res['metric']
                if m in rename:
                    m = rename[m]
                    
                name = "{}.{} ({})".format(m, res['tags']['sensor_name'], res['tags']['sensor_units'])
            elif res['metric'] == 'lwbp1':
                name = "{} ({})".format(res['metric'], res['tags']['units'])
                
            if "dps" in res:
                k = list(res["dps"].keys())
                k.sort()
                for i in k:
                    
                    ts = unconvert_time(int(i))
                    if ts not in rows:
                        rows[ts] = {}
                    rows[ts][name] = res["dps"][i]
                    
        times = rows.keys()
        times.sort()                    
        
        with open(outputfilename, 'wb') as f:
            
            line = ", ".join(['Timestamp'] + cols)
            f.write(line)
            f.write("\n")
            #print(line)
            logging.debug(line)
            for t in times:
                row_array = [t.isoformat()] 
                for colname in cols:
                    if colname in rows[t]:
                        row_array += ["{}".format(rows[t][colname])]
                    else:
                        row_array += [""]
                line = ", ".join(row_array)
                f.write(line)
                f.write("\n")
                #print(line)
                logging.debug(line)
            
        
    else:
        
        log.error("error in request")
        log.error(r.text)
        
if __name__ == "__main__":
    # full download
    begin = datetime.date(2014, 8, 1)

    step = datetime.timedelta(days=1)
    end = datetime.date.today()
    
    resolutions = [None, '5m-avg', '15m-avg', '60m-avg']
    
    at = begin
    while at < end:
        for resolution in resolutions:
            logging.info("Downloading data from: {} with resolution = {}".format(at.isoformat(), resolution))
                            
            get_day(at, downsample= resolution)
            
        at += step
        
