import json
import base64
import csv
import logging
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
    """
    Handles Upstox API communication, WebSocket streaming, and instrument management.
    Subscribes to option chain via WebSocket and refreshes subscription every 30 minutes.
    """

    def __init__(
        self,
        config,
        aws_profile='Absc',
        aws_region='ap-south-1',
        instruments_file='/mnt/tmpfs/nse_instruments.csv',
        s3_bucket='nse-instruments-data',
        s3_key='instruments/nse_instruments.csv'
    ):
        self.config = config
        self.aws_profile = aws_profile
        self.aws_region = aws_region
        self.instruments_file = instruments_file
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key

        self.access_token = None
        self.instrument_key = 'NSE_INDEX|Nifty 50'
        self.subscribed_instruments = []
        self.instrument_metadata = {}
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
                # session = boto3.Session(profile_name=self.aws_profile)
                my_config = Config(region_name=self.aws_region, signature_version='s3v4')
                self._s3 = boto3.client('s3', config=my_config)
            except (BotoCoreError, ClientError) as e:
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
            logger.info("Downloading instruments from S3 bucket: %s", self.s3_bucket)
            self.s3.download_file(self.s3_bucket, self.s3_key, str(local_path))
            logger.info("Downloaded to %s", local_path)
            return True
        except ClientError as e:
            logger.error("S3 download failed: %s", e)
            return False

    def ensure_instruments_csv(self):
        csv_path = Path(self.instruments_file)
        try:
            if csv_path.exists():
                file_age_hours = (time.time() - csv_path.stat().st_mtime) / 3600
                if file_age_hours < 24:
                    logger.info("Using cached instruments (age: %.1fh)", file_age_hours)
                    return
            if not self.download_instruments_from_s3():
                raise FileNotFoundError("Failed to download instruments from S3 bucket %s" % self.s3_bucket)
        except (OSError, IOError) as e:
            logger.error("File system error while ensuring instruments: %s", e)
            raise

    def load_nse_fo_map(self):
        if self._instruments_map is not None:
            return self._instruments_map
        try:
            self.ensure_instruments_csv()
            path = Path(self.instruments_file)
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

            all_call, all_put, all_metadata = [], [], {}
            nifty_spot, atm_strike = self.nifty_spot, None

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
                            if instr_key:
                                if side_label == 'CE':
                                    all_call.append(instr_key)
                                else:
                                    all_put.append(instr_key)

                                all_metadata[instr_key] = {
                                    'symbol': opt_data.get('tradingsymbol', ''),
                                    'strike': strike,
                                    'expiry': expiry_str,
                                    'option_type': side_label,
                                    'underlying_spot': nifty_spot
                                }

            logger.info("Total filtered instruments: %d", len(all_call) + len(all_put))
            return all_call, all_put, {
                'nifty_spot': nifty_spot,
                'atm_strike': atm_strike,
                'instrument_metadata': all_metadata
            }
        except (ValueError, TypeError, upstox_client.rest.ApiException) as e:
            logger.error("Filtered instruments processing error: %s", e)
            return [], [], {}

    def _start_subscription_refresher(self, atm_range=15):
        """
        Background thread — refreshes WebSocket subscription every 30 minutes
        with updated ATM strikes.
        """
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

                    self.instrument_metadata = meta.get('instrument_metadata', {})
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
        """
        Entry point called by analyzer.
        Starts WebSocket stream with 30-minute subscription refresh.
        """
        try:
            if not self.access_token:
                logger.error("No access token available")
                return

            # Initial instrument load
            call_instr, put_instr, meta = self.get_filtered_option_instruments(atm_range=atm_range)
            if not call_instr and not put_instr:
                logger.error("No option instruments found")
                return

            self.instrument_metadata = meta.get('instrument_metadata', {})
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

            # Start subscription refresher thread
            self._start_subscription_refresher(atm_range=atm_range)

            try:
                self._streamer.connect()
            except KeyboardInterrupt:
                logger.info("WebSocket interrupted by user")
                self._streamer.disconnect()
            except AttributeError as e:
                if 'pool' in str(e):
                    pass  # Upstox SDK cleanup bug
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

    def get_instrument_metadata(self, instrument_key):
        return self.instrument_metadata.get(instrument_key, {})