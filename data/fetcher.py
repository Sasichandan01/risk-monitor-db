import json
import base64
import csv
import logging
import os
import time
import threading
import requests
from datetime import datetime, timezone, timedelta
from pathlib import Path

import boto3
import upstox_client
from botocore.config import Config
from botocore.exceptions import ClientError, BotoCoreError

logger = logging.getLogger(__name__)

IST = timezone(timedelta(hours=5, minutes=30))


class StockDataFetcher:

    def __init__(
        self,
        config,
        aws_region='ap-south-1',
        instruments_file='/mnt/tmpfs/nse_instruments.csv',
        s3_bucket='nse-instruments-data',
        s3_key='instruments/nse_instruments.csv'
    ):
        self.config = config
        self.aws_region = aws_region
        self.instruments_file = instruments_file
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key

        self.access_token = None
        self.instrument_key = 'NSE_INDEX|Nifty 50'
        self.subscribed_instruments = []
        self.instrument_lookup = {}
        self.nifty_spot = 24000
        self.running = True
        self._api_client = None
        self._s3 = None
        self._instruments_map = None
        self._streamer = None

    @property
    def s3(self):
        if self._s3 is None:
            try:
                my_config = Config(region_name=self.aws_region, signature_version='s3v4')
                self._s3 = boto3.client('s3', config=my_config)
                
                self._s3.head_bucket(Bucket=self.s3_bucket)
                logger.info("S3 bucket %s accessible", self.s3_bucket)
            except ClientError as e:
                error_code = e.response['Error']['Code']
                if error_code == '403':
                    logger.error("S3 Access Denied to bucket %s", self.s3_bucket)
                elif error_code == '404':
                    logger.error("S3 bucket %s not found", self.s3_bucket)
                else:
                    logger.error("S3 error: %s", e)
                raise
            except (BotoCoreError, Exception) as e:
                logger.error("Failed to initialize S3 client: %s", e)
                raise
        return self._s3

    def load_token(self):
        try:
            token = self.config.ACCESS_TOKEN
            if not token:
                logger.warning("No token found in configuration")
                return False

            parts = token.split('.')
            if len(parts) < 2:
                logger.error("Invalid JWT format encountered")
                return False

            payload_b64 = parts[1] + '=' * (4 - len(parts[1]) % 4)
            payload = json.loads(base64.b64decode(payload_b64).decode())
            exp_time = datetime.fromtimestamp(payload['exp'])

            if datetime.now() < exp_time:
                self.access_token = token
                logger.info("Loaded token from config (expires %s)", exp_time)
                return True
            else:
                logger.warning("Token has expired")
                return False
        except (json.JSONDecodeError, KeyError, ValueError) as e:
            logger.error("Failed to decode token: %s", e)
            return False

    def save_token(self):
        try:
            if self.config.save_access_token(self.access_token):
                logger.info("Token successfully saved to SSM")
                return True
            else:
                logger.error("Failed to save token to SSM")
                return False
        except (ClientError, BotoCoreError, ValueError, TypeError) as e:
            logger.error("Token save error: %s", e)
            return False

    def login(self):
        headers = {
            'accept': 'application/json',
            'Content-Type': 'application/x-www-form-urlencoded'
        }
        data = {
            'code': self.config.CODE,
            'client_id': self.config.API_KEY,
            'client_secret': self.config.API_SECRET,
            'redirect_uri': 'http://127.0.0.1',
            'grant_type': "authorization_code"
        }
        try:
            resp = requests.post(
                'https://api.upstox.com/v2/login/authorization/token',
                data=data,
                headers=headers,
                timeout=10,
            )
            resp.raise_for_status()
            self.access_token = resp.json()['access_token']
            logger.info("Login success")
            self.save_token()
            self.load_nse_fo_map()
            return True
        except requests.exceptions.RequestException as e:
            logger.error("Login network/HTTP error: %s", e)
            return False
        except (KeyError, json.JSONDecodeError) as e:
            logger.error("Login data parsing error: %s", e)
            return False

    def download_instruments_from_s3(self):
        local_path = Path(self.instruments_file)
        try:
            local_path.parent.mkdir(parents=True, exist_ok=True)
            logger.info("Downloading from s3://%s/%s to %s", self.s3_bucket, self.s3_key, local_path)
            
            self.s3.download_file(self.s3_bucket, self.s3_key, str(local_path))
            
            if local_path.exists():
                size_kb = local_path.stat().st_size / 1024
                logger.info("Downloaded successfully - size: %.1f KB", size_kb)
                return True
            else:
                logger.error("Download completed but file not found at %s", local_path)
                return False
        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code == '403':
                logger.error("S3 Access Denied - bucket: %s, key: %s", self.s3_bucket, self.s3_key)
            elif error_code == '404':
                logger.error("S3 Object not found - bucket: %s, key: %s", self.s3_bucket, self.s3_key)
            else:
                logger.error("S3 download failed: %s", e)
            return False

    def ensure_instruments_csv(self):
        csv_path = Path(self.instruments_file)
        try:
            if csv_path.exists():
                logger.info("Using existing instruments file at %s", csv_path)
                return
            logger.info("Instruments file not found - downloading from S3")
            if not self.download_instruments_from_s3():
                raise FileNotFoundError("Failed to download from s3://%s/%s" % (self.s3_bucket, self.s3_key))
        except (OSError, IOError) as e:
            logger.error("File system error: %s", e)
            raise

    def load_nse_fo_map(self):
        if self._instruments_map is not None:
            logger.info("Using cached instruments map - %d entries", len(self._instruments_map))
            return self._instruments_map
        try:
            logger.info("Loading NSE FO instrument mapping")
            self.ensure_instruments_csv()
            path = Path(self.instruments_file)
            
            if not path.exists():
                raise FileNotFoundError("Instruments file not found at %s" % path)
            
            mapping = {}
            with path.open(newline='', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    key = (
                        row["tradingsymbol"].strip(),
                        row["option_type"].strip(),
                        row["exchange"].strip()
                    )
                    mapping[key] = {
                        "instrument_key": row["instrument_key"].strip(),
                        "exchange_token": int(row["exchange_token"].strip()),
                        "symbol": row["tradingsymbol"].strip(),
                        "option_type": row["option_type"].strip(),
                        "exchange": row["exchange"].strip(),
                    }
            self._instruments_map = mapping
            logger.info("Loaded %d instruments into memory", len(mapping))
            return mapping
        except (OSError, csv.Error, KeyError, ValueError) as e:
            logger.error("Load instruments error: %s", e)
            raise

    def get_all_expiry_dates_api(self, instrument_key, count=4):
        url = "https://api.upstox.com/v2/option/contract?instrument_key=%s" % instrument_key
        headers = {
            "Authorization": f"Bearer {self.access_token}",
            "Accept": "application/json"
        }
        try:
            r = requests.get(url, headers=headers, timeout=10)
            r.raise_for_status()
            contracts = r.json().get("data", [])
            expiries = set()
            for c in contracts:
                exp = c.get("expiry")
                if exp:
                    expiries.add(exp)
            today = datetime.now().strftime('%Y-%m-%d')
            upcoming = sorted([e for e in expiries if e >= today])
            return upcoming[:count]
        except (requests.exceptions.RequestException, json.JSONDecodeError, KeyError) as e:
            logger.error("Get expiry dates error: %s", e)
            return []

    def get_filtered_option_instruments(self, atm_range=15):
        try:
            if self._api_client is None:
                configuration = upstox_client.Configuration()
                configuration.access_token = self.access_token
                self._api_client = upstox_client.ApiClient(configuration)
            
            configuration = upstox_client.Configuration()
            configuration.access_token = self.access_token
            api_client = upstox_client.ApiClient(configuration)
            options_api = upstox_client.OptionsApi(api_client)

            index_key = "NSE_INDEX|Nifty 50"
            expiry_dates = self.get_all_expiry_dates_api(index_key)
            if not expiry_dates:
                raise ValueError("Could not determine expiry dates")

            all_call, all_put, all_lookup = [], [], {}
            nifty_spot, atm_strike = None, None

            for expiry_str in expiry_dates:
                logger.info("Fetching option chain: %s", expiry_str)
                resp = options_api.get_put_call_option_chain(index_key, expiry_str)
                data_obj = resp.to_dict() if hasattr(resp, 'to_dict') else resp
                data = data_obj.get('data', [])

                if not data:
                    continue

                if atm_strike is None:
                    nifty_spot = float(data[0].get('underlying_spot_price', 24000))
                    atm_strike = round(nifty_spot / 50) * 50
                    logger.info("Nifty Spot: %.2f ATM: %d", nifty_spot, atm_strike)

                min_strike = atm_strike - (atm_range * 50)
                max_strike = atm_strike + (atm_range * 50)

                for row in data:
                    strike = float(row.get('strike_price', 0))
                    if strike < min_strike or strike > max_strike:
                        continue

                    for side_key, side_label in [('call_options', 'CE'), ('put_options', 'PE')]:
                        opt_data = row.get(side_key)
                        if opt_data:
                            instr_key = opt_data.get('instrument_key')
                            symbol = opt_data.get('tradingsymbol', '')
                            
                            if instr_key:
                                if side_label == 'CE':
                                    all_call.append(instr_key)
                                else:
                                    all_put.append(instr_key)

                                all_lookup[instr_key] = {
                                    'symbol': symbol,
                                    'strike': strike,
                                    'expiry': expiry_str,
                                    'option_type': side_label
                                }

            logger.info("Total filtered instruments: %d", len(all_call) + len(all_put))
            logger.info("Instrument lookup populated with %d entries", len(all_lookup))
            
            return all_call, all_put, {
                'nifty_spot': nifty_spot,
                'atm_strike': atm_strike,
                'instrument_lookup': all_lookup
            }
        except (ValueError, TypeError, upstox_client.rest.ApiException) as e:
            logger.error("Filtered instruments processing error: %s", e)
            return [], [], {}

    def _start_subscription_refresher(self, atm_range=15):
        def refresher():
            while self.running:
                time.sleep(1800)
                if not self.running:
                    break

                try:
                    logger.info("Refreshing WebSocket subscription...")
                    call_instr, put_instr, meta = self.get_filtered_option_instruments(atm_range=atm_range)

                    if not call_instr and not put_instr:
                        logger.warning("Subscription refresh — no instruments found")
                        continue

                    self.instrument_lookup = meta.get('instrument_lookup', {})
                    self.nifty_spot = meta.get('nifty_spot', 24000)

                    instruments = [self.instrument_key] + call_instr + put_instr
                    self.subscribed_instruments = instruments

                    if self._streamer:
                        self._streamer.unsubscribe(self.subscribed_instruments)
                        self._streamer.subscribe(instruments, "full")
                        logger.info("Subscription refreshed: %d instruments", len(instruments))

                except (ValueError, TypeError, upstox_client.rest.ApiException) as e:
                    logger.error("Subscription refresh error: %s", e)

        threading.Thread(target=refresher, daemon=True).start()

    def start_polling(self, on_message_callback, atm_range=15):
        try:
            if not self.access_token:
                logger.error("No access token available")
                return

            call_instr, put_instr, meta = self.get_filtered_option_instruments(atm_range=atm_range)
            if not call_instr and not put_instr:
                logger.error("No option instruments found")
                return

            self.instrument_lookup = meta.get('instrument_lookup', {})
            self.nifty_spot = meta.get('nifty_spot', 24000)

            instruments = [self.instrument_key] + call_instr + put_instr
            self.subscribed_instruments = instruments

            logger.info("Starting WebSocket stream (%d instruments)...", len(instruments))

            configuration = upstox_client.Configuration()
            configuration.access_token = self.access_token
            self._api_client = upstox_client.ApiClient(configuration)
            self._streamer = upstox_client.MarketDataStreamerV3(
                upstox_client.ApiClient(configuration)
            )

            def on_open():
                logger.info("WebSocket connected — subscribing to %d instruments", len(instruments))
                try:
                    self._streamer.subscribe(self.subscribed_instruments, "full")
                    logger.info("Subscribed to %d instruments", len(self.subscribed_instruments))
                except upstox_client.rest.ApiException as e:
                    logger.error("Subscription error: %s", e)

            def on_message(data):
                try:
                    on_message_callback(data)
                except (KeyError, ValueError, TypeError) as e:
                    logger.error("Message callback error: %s", e)

            def on_error(error):
                logger.error("WebSocket error: %s", error)

            def on_close():
                logger.warning("WebSocket closed")

            self._streamer.on("open", on_open)
            self._streamer.on("message", on_message)
            self._streamer.on("error", on_error)
            self._streamer.on("close", on_close)

            self._start_subscription_refresher(atm_range=atm_range)

            try:
                self._streamer.connect()
            except KeyboardInterrupt:
                logger.info("WebSocket interrupted by user")
                self._streamer.disconnect()
            except AttributeError as e:
                if 'pool' in str(e):
                    pass
                else:
                    raise
            except Exception as e:
                logger.error("WebSocket connection error: %s", e)
                raise

        except KeyboardInterrupt:
            logger.info("Polling interrupted by user")
            self.running = False
        except (ValueError, RuntimeError, upstox_client.rest.ApiException) as e:
            logger.error("Start polling error: %s", e)
            raise

    def get_instrument_lookup(self, instrument_key):
        return self.instrument_lookup.get(instrument_key, {})