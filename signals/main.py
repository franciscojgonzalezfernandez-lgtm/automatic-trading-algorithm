# Copyright 2021 TradersOfTheUniverse S.A. All Rights Reserved.
#
# [MobyDick Project]
#
# Authors:
#   antoniojose.luqueocana@telefonica.com
#   joseluis.roblesurquiza@telefonica.com
#   franciscojavier.gonzalezfernandez1@telefonica.com
#
# Version: 0.1
#

import logging
from re import sub

from flask import Flask, request

from core.account.account_manager import AccountManager
from core.market.analyzer5 import Analyzer5
from core.market.analyzerJ2 import AnalyzerJ2
from core.market.analyzerJ3_1m import AnalyzerJ3_1m
from core.order.order_simulator import OrderSimulator


# Loggin Config
from core.utils.redisclient import RedisClient

logging.basicConfig(
    format='[%(filename)s:%(lineno)d] %(message)s',
    datefmt='%d-%m-%Y:%H:%M:%S',
    level=logging.INFO
)

# Routing
logging.info("1.- Flask app config")
app = Flask(__name__)

# Initialize
market_analyzer5 = Analyzer5(real_orders=True)
market_analyzer3_1m = AnalyzerJ3_1m(real_orders=True)
market_analyzer2_5m = AnalyzerJ2(real_orders=True)
order_simulator = OrderSimulator()


@app.route('/')
def index():
    """ Return a friendly HTTP greeting. """
    return '<h1>MobyDick Project</h1>' \
           '<p> &copy; Copyright 2021 TradersOfTheUniverse All Rights Reserved. </p>'


@app.route('/mobydick/signals/market/analyze', methods=['GET'])
def analyze_market():
    """ Analyze market and send alerts for signals """
    try:
        logging.info("Inicio Analizer5")
        market_analyzer5.analyze_all()
        logging.info("Fin Analizer5")
    except Exception as e:
        logging.exception(e)
    return "OK"


@app.route('/mobydick/signals/market/analyzerJ2_5m', methods=['GET'])
def analyzer_j2_5m():
    """ Analyze market and send alerts for signals """
    try:
        logging.info("Inicio Analyzer2_5m")
        market_analyzer2_5m.real_orders()
        logging.info("Fin Analizer2_5m")
    except Exception as e:
        logging.exception(e)
    return "OK"


@app.route('/mobydick/signals/market/analyzerJ3_1m', methods=['GET'])
def analyzer_j3_1m():
    """ Analyze market and send alerts for signals """
    try:
        logging.info("Inicio Analizer3_1m")
        market_analyzer3_1m.real_orders()
        logging.info("Fin Analizer3_1m")
    except Exception as e:
        logging.exception(e)
    return "OK"


@app.route('/mobydick/signals/simulation/refresh', methods=['POST'])
def check_profit():
    """ Check stoploss and trailing stop of a opened simulated order """
    try:
        order_simulator.refresh_order(request.get_json())
    except Exception as e:
        logging.exception(e)
        raise e

    return "OK"


@app.route('/mobydick/admin/redis/flushall', methods=['GET'])
def flushall():
    """ Carga en BQ de candle sticks """
    redis_client = RedisClient()
    redis_client.flushall()
    return "OK"


@app.route('/mobydick/admin/show/account', methods=['GET'])
def show_account():
    """ Muestra la informacion de la cuenta """
    account = request.args.get("account", default=None, type=str)
    start = request.args.get("start", default=None, type=str)
    end = request.args.get("end", default=None, type=str)
    upload_to_bq = request.args.get("upload_to_bq", default=False, type=bool)

    account_manager = AccountManager(account=account).load()
    account_summary = account_manager.build_account_summary(start, end, upload_to_bq).replace("\n", "<br>")
    account_summary = sub('([+]\d+(\.\d+)?%)', r'<font color="ForestGreen"><b>\1</b></font>', account_summary)
    account_summary = sub('([-]\d+(\.\d+)?%)', r'<font color="Red"><b>\1</b></font>', account_summary)
    return account_summary


@app.errorhandler(500)
def server_error(e):
    logging.exception('An error occurred during a request. ' + repr(e))
    return """
    An internal error occurred: <pre>{}</pre>
    See logs for full stacktrace.
    """.format(e), 500


if __name__ == '__main__':
    # This is used when running locally. Gunicorn is used to run the
    # application on Google App Engine. See entrypoint in predict.yaml.
    app.run(host="127.0.0.1", port=8080, debug=True, threaded=True)