import requests
import logging
import time

# API Setup
API_KEY = {'X-API-Key': 'MW0YJ28H'}
s = requests.Session()
s.headers.update(API_KEY)

# Logging Setup
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")

# Constants
ORDER_LIMIT = 30
TRANSPORT_LIMIT = 10
ARBITRAGE_THRESHOLD = 1000  # Minimum profit for arbitrage
CRACK_SPREAD_THRESHOLD = 500  # Minimum crack spread profit
PIPELINE_COST = 30000  # Example pipeline lease cost
REFINERY_COST_PER_BARREL = 20  # Example refinery cost per barrel
ELASTICITY = 1  # $1 change per 1M barrel surprise

# Helper Functions
def lease_asset(ticker, quantity=1):
    """Lease storage, pipelines, or refineries."""
    for _ in range(quantity):
        resp = s.post('http://localhost:9999/v1/leases', params={'ticker': ticker})
        if resp.status_code == 200:
            logging.info(f"Leased one unit of {ticker}.")
        else:
            logging.error(f"Failed to lease {ticker}. Response: {resp.text}")

def place_order(ticker, quantity, action):
    """Place a market order."""
    resp = s.post('http://localhost:9999/v1/orders', params={
        'ticker': ticker, 'type': 'MARKET', 'quantity': quantity, 'action': action
    })
    if resp.status_code == 200:
        logging.info(f"Placed {action} order for {quantity} of {ticker}.")
    else:
        logging.error(f"Failed to place {action} order for {ticker}. Response: {resp.text}")

def calculate_price_impact(surprise, elasticity=ELASTICITY):
    """Calculate price impact based on inventory surprise."""
    return surprise * elasticity

# Strategies
def fundamental_news_trading():
    """Trade based on news events."""
    resp = s.get('http://localhost:9999/v1/news')
    if resp.status_code == 200:
        news_items = resp.json()
        for item in news_items:
            if not item['read']:
                headline = item['headline']
                if "DRAW" in headline or "BUILD" in headline:
                    logging.info(f"Supply disruption detected: {headline}")
                    surprise = int(headline.split()[-2])  # Parse the numerical value
                    impact = calculate_price_impact(surprise)
                    if impact > 0:
                        place_order('CL', ORDER_LIMIT, 'BUY')
                    else:
                        place_order('CL', ORDER_LIMIT, 'SELL')
                    # Hedge
                    place_order('CL-2F', ORDER_LIMIT, 'SELL' if impact > 0 else 'BUY')
                s.post(f"http://localhost:9999/v1/news/{item['id']}", params={'read': True})
    else:
        logging.error(f"Failed to fetch news. Response: {resp.text}")

def transportation_arbitrage():
    """Evaluate and execute transportation arbitrage."""
    resp = s.get('http://localhost:9999/v1/securities')
    if resp.status_code == 200:
        securities = resp.json()
        cl_ak = next((s for s in securities if s['ticker'] == 'CL-AK'), None)
        cl = next((s for s in securities if s['ticker'] == 'CL'), None)

        if cl_ak and cl:
            profit = (cl['bid'] - cl_ak['ask']) * TRANSPORT_LIMIT - PIPELINE_COST
            if profit > ARBITRAGE_THRESHOLD:
                logging.info(f"Executing transport arbitrage. Expected profit: ${profit:.2f}")
                lease_asset('AK-STORAGE', 1)
                lease_asset('CL-STORAGE', 1)
                place_order('CL-AK', TRANSPORT_LIMIT, 'BUY')
                lease_asset('AK-CS-PIPE', 1)
            else:
                logging.info("Transportation arbitrage not profitable.")
    else:
        logging.error(f"Failed to fetch securities. Response: {resp.text}")

def refinery_arbitrage():
    """Evaluate and execute refinery trades."""
    resp = s.get('http://localhost:9999/v1/securities')
    if resp.status_code == 200:
        securities = resp.json()
        cl = next((s for s in securities if s['ticker'] == 'CL'), None)
        rb = next((s for s in securities if s['ticker'] == 'RB'), None)
        ho = next((s for s in securities if s['ticker'] == 'HO'), None)

        if cl and rb and ho:
            crack_spread = ((rb['bid'] + ho['bid']) / 3) - cl['ask'] - REFINERY_COST_PER_BARREL
            if crack_spread > CRACK_SPREAD_THRESHOLD:
                logging.info(f"Executing refinery arbitrage. Crack spread: ${crack_spread:.2f}")
                lease_asset('CL-STORAGE', 1)
                lease_asset('CL-REFINERY', 1)
                place_order('CL', ORDER_LIMIT, 'BUY')
                # Process crude oil
                resp = s.get('http://localhost:9999/v1/leases')
                lease_id = next((l['id'] for l in resp.json() if l['ticker'] == 'CL-REFINERY'), None)
                if lease_id:
                    s.post(f'http://localhost:9999/v1/leases/{lease_id}', params={
                        'from1': 'CL', 'quantity1': TRANSPORT_LIMIT
                    })
                    logging.info(f"Processed {TRANSPORT_LIMIT} barrels of CL into refined products.")
    else:
        logging.error(f"Failed to fetch securities. Response: {resp.text}")

def spot_futures_arbitrage():
    """Execute spot-futures arbitrage."""
    resp = s.get('http://localhost:9999/v1/securities')
    if resp.status_code == 200:
        securities = resp.json()
        cl_spot = next((s for s in securities if s['ticker'] == 'CL'), None)
        cl_futures = next((s for s in securities if s['ticker'] == 'CL-2F'), None)

        if cl_spot and cl_futures:
            carry_cost = PIPELINE_COST / TRANSPORT_LIMIT
            mispricing = cl_futures['bid'] - cl_spot['ask']
            if mispricing > carry_cost:
                logging.info(f"Executing spot-futures arbitrage. Mispricing: ${mispricing:.2f}")
                place_order('CL', TRANSPORT_LIMIT, 'BUY')
                place_order('CL-2F', TRANSPORT_LIMIT, 'SELL')
            elif mispricing < -carry_cost:
                logging.info(f"Reversing spot-futures arbitrage. Mispricing: ${mispricing:.2f}")
                place_order('CL', TRANSPORT_LIMIT, 'SELL')
                place_order('CL-2F', TRANSPORT_LIMIT, 'BUY')
    else:
        logging.error(f"Failed to fetch securities. Response: {resp.text}")

# Main Execution Loop
if __name__ == "__main__":
    try:
        while True:
            fundamental_news_trading()
            transportation_arbitrage()
            refinery_arbitrage()
            spot_futures_arbitrage()
            time.sleep(5)
    except KeyboardInterrupt:
        logging.info("Trading strategies terminated.")
