from flask import Flask , jsonify , render_template , request , abort 
from flask_cors import CORS
import redis
import datetime
from redisclient import RedisClient
from apscheduler.schedulers.background import BackgroundScheduler
from bhaav_download_scheduler import BhaavScheduler


class DataNotFound(Exception):
    status_code = 400
    def __init__(self, message, status_code=None, payload=None):
        super().__init__()
        self.message = message
        if status_code is not None:
            self.status_code = status_code
        self.payload = payload

    def to_dict(self):
        rv = dict(self.payload or ())
        rv['message'] = self.message
        return rv

bhav_sched =  BhaavScheduler()
sched_buddy = BackgroundScheduler(daemon=True)
sched_buddy.add_job(bhav_sched.exec_job , 'cron' , day='*' , hour='18' , minute='0' , month='*')
sched_buddy.start()

#instantiate app
app = Flask(__name__ , static_folder="../dist" , static_url_path='/')

CORS(app , resources={r'/*':{'origins':'*'}})

@app.route('/' , defaults={'path':''})
@app.route('/<path:path>')
def index(path):
    return app.send_static_file("index.html")


@app.errorhandler(DataNotFound)
def invalid_api_usage(e):
    return jsonify(e.to_dict())

@app.route('/api/stockKeys' , methods=['GET'])
def get_stock_names():
    """ Return list of stock keys """
    r = RedisClient().conn
    res = [key.decode('utf-8') for key in r.keys()]
    print(res)
    if len(res) == 0:
        raise DataNotFound("Stock Data not available")
    else:
        return jsonify(res)

@app.route('/api/stockData' , methods=['GET'])
def get_stock_data():
    """ Fetch Stock data by date range provided """
    d = '}'
    ticker_name = request.args.get('ticker_name')
    start = int(request.args.get('start'))
    limit = 20
    r = RedisClient().conn
    fetch_dat = r.get(ticker_name).decode('utf-8')
    fetch_dat = [e+d for e in fetch_dat.split(d) if e]
    if fetch_dat == None:
        raise DataNotFound("Stock Data not available")
    else:
        return jsonify(paginated_results(fetch_dat , '/api/stockData'  , start , limit))

def paginated_results(data_arr , url , start , limit):
    """ Returns a paginated response from array passed """
    if len(data_arr) < start:
        raise DataNotFound("Stock Data not available")
    obj = {}
    obj['start'] = start
    obj['limit'] = limit
    obj['count'] = len(data_arr)
    #create previous url
    if start == 1:
        obj['previous'] = ''
    else:
        prev_start = max(1 , start - limit)
        limit_copy = prev_start - 1
        obj['previous'] = url + '?start=%d&limit=%d' % (prev_start, limit_copy)   
    # make next url
    if start + limit > len(data_arr):
        obj['next'] = ''
    else:
        start_copy = start + limit
        obj['next'] = url + '?start=%d&limit=%d' % (start_copy, limit)
    # finally extract result according to bounds
    obj['results'] = data_arr[(start - 1):(start - 1 + limit)]
    print(obj)
    return obj  

@app.route('/api/csvData' , methods=['GET'])
def return_all():
    """ fetch all data to convert to csv format """
    d = '}'
    ticker_name = request.args.get('ticker_name')
    r = RedisClient().conn
    fetch_dat = r.get(ticker_name).decode('utf-8')
    fetch_dat = [e+d for e in fetch_dat.split(d) if e]
    obj = {}
    obj['results'] = fetch_dat
    if fetch_dat == None:
        raise DataNotFound("Stock Data not available")
    else:
        return jsonify(obj)
if __name__ == "__main__":
    app.run(debug=True , port=int('80'))