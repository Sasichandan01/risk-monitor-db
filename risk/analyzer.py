import threading
import time
import logging
import json
from datetime import datetime, timezone, timedelta
from botocore.exceptions import ClientError
from concurrent.futures import ThreadPoolExecutor

import boto3

from risk.calculator import RiskCalculator
from data.cleaner import DataCleaner

logger = logging.getLogger(__name__)

IST = timezone(timedelta(hours=5, minutes=30))
MIN_VALID_TS = datetime(2025, 1, 1, tzinfo=IST).timestamp()

sqs = boto3.client('sqs', region_name='ap-south-1')
INSERT_QUEUE_URL = 'https://sqs.ap-south-1.amazonaws.com/079975324269/OptionRiskQueue'


class OptionsRiskAnalyzer:

    def __init__(self, fetcher, config):
        self.fetcher = fetcher
        self.config = config
        self.risk_calculator = RiskCalculator()
        self.instrument_metadata = {}
        self.metadata_lock = threading.Lock()
        self.running = True
        self.last_spot_ssm_write = 0
        self.executor = ThreadPoolExecutor(max_workers=20, thread_name_prefix="RiskCalc")
        self.stats = {
            'total_received': 0,
            'invalid_data': 0,
            'stale_skipped': 0,
            'not_subscribed': 0,
            'processed': 0,
            'batches_sent': 0,
            'nifty_updates': 0,
            'errors': 0
        }

    def start(self):
        try:
            logger.info("Starting Options Risk Analyzer...")
            t = threading.Thread(target=self._batch_writer, daemon=True)
            t.start()
            logger.info("Batch writer thread alive: %s", t.is_alive())
            self.fetcher.start_polling(self.on_message_handler)
        except (RuntimeError, ValueError, TypeError) as e:
            logger.error("Analyzer start failed: %s", e)
            raise

    def _batch_writer(self):
        logger.info("Batch writer started — aligning to 30s market boundaries")

        while self.running:
            now = datetime.now(IST)
            if now.hour > 9 or (now.hour == 9 and now.minute >= 15):
                break
            logger.info("Waiting for market open — current time: %s", now.strftime('%H:%M:%S'))
            time.sleep(10)

        now = datetime.now(IST)
        remainder = now.second % 30
        sleep_secs = 30 - remainder if remainder != 0 else 30
        logger.info("Aligning to next 30s boundary — sleeping %ds", sleep_secs)
        time.sleep(sleep_secs)

        logger.info("Batch writer aligned — starting at %s", datetime.now(IST).strftime('%H:%M:%S'))

        while self.running:
            try:
                now = datetime.now(IST)
                if now.hour > 15 or (now.hour == 15 and now.minute >= 30):
                    logger.info("Market closed at %s — stopping batch writer", now.strftime('%H:%M:%S'))
                    break

                self._flush_batch()
                time.sleep(30)

            except Exception as e:
                logger.error("Batch writer error — continuing: %s", e)
                self.stats['errors'] += 1
                time.sleep(30)

    def _flush_batch(self):
        snapshot = {}
        try:
            with self.metadata_lock:
                if not self.instrument_metadata:
                    logger.info("Nothing to flush at %s", datetime.now(IST).strftime('%H:%M:%S'))
                    return
                snapshot = dict(self.instrument_metadata)
                self.instrument_metadata.clear()

            records = list(snapshot.values())
            total_records = len(records)
            logger.info("Flushing %d records at %s", total_records, datetime.now(IST).strftime('%H:%M:%S'))

            MAX_BATCH_SIZE_KB = 500
            batches = []
            current_batch = []
            current_size = 0

            for record in records:
                record_size = len(json.dumps(record, default=str).encode('utf-8')) / 1024
                
                if current_size + record_size > MAX_BATCH_SIZE_KB and current_batch:
                    batches.append(current_batch)
                    current_batch = [record]
                    current_size = record_size
                else:
                    current_batch.append(record)
                    current_size += record_size

            if current_batch:
                batches.append(current_batch)

            logger.info("Split into %d batches", len(batches))

            for batch_num, batch in enumerate(batches, 1):
                payload = json.dumps({
                    'batch_time': datetime.now(IST).isoformat(),
                    'batch_number': batch_num,
                    'total_batches': len(batches),
                    'records': batch
                }, default=str)

                payload_kb = len(payload.encode('utf-8')) / 1024
                logger.info("Batch %d/%d - size: %.1fKB - records: %d", 
                           batch_num, len(batches), payload_kb, len(batch))

                try:
                    sqs.send_message(
                        QueueUrl=INSERT_QUEUE_URL,
                        MessageBody=payload
                    )
                    self.stats['batches_sent'] += 1
                    logger.info("Batch %d/%d sent to SQS", batch_num, len(batches))
                except ClientError as e:
                    logger.error("SQS send failed for batch %d/%d: %s", batch_num, len(batches), e)
                    with self.metadata_lock:
                        for record in batch:
                            key = record.get('instrument_key')
                            if key and key not in self.instrument_metadata:
                                self.instrument_metadata[key] = record
                    self.stats['errors'] += 1

            logger.info("Flushed %d records in %d batches", total_records, len(batches))

        except (ValueError, TypeError) as e:
            logger.error("Flush error: %s", e)
            self.stats['errors'] += 1

    def _is_valid_timestamp(self, ltt_ms):
        if ltt_ms is None:
            return False
        try:
            ltt_ts = int(ltt_ms) / 1000
            now_ts = datetime.now(IST).timestamp()
            if ltt_ts < MIN_VALID_TS:
                return False
            if now_ts - ltt_ts > 60:
                return False
            return True
        except (ValueError, TypeError):
            return False

    def _next_boundary(self, ltt_ms):
        feed_time = datetime.fromtimestamp(int(ltt_ms) / 1000, tz=IST)
        second = feed_time.second
        next_sec = (second // 30 + 1) * 30
        if next_sec == 60:
            return feed_time.replace(second=0, microsecond=0) + timedelta(minutes=1)
        return feed_time.replace(second=next_sec, microsecond=0)

    def on_message_handler(self, data):
        try:
            feeds = data.get("feeds", {})
            if not feeds:
                return

            for instrument_key, feed_info in feeds.items():
                try:
                    if 'NSE_INDEX|Nifty 50' in instrument_key or 'Nifty 50' in instrument_key:
                        self._update_nifty_spot(feed_info)
                        continue

                    full_feed = feed_info.get("fullFeed", {}).get("marketFF", {})
                    if not full_feed:
                        continue

                    ltt = full_feed.get("ltpc", {}).get("ltt")
                    if not self._is_valid_timestamp(ltt):
                        self.stats['stale_skipped'] += 1
                        continue

                    metadata = self.fetcher.get_instrument_lookup(instrument_key)
                    if not metadata:
                        self.stats['not_subscribed'] += 1
                        continue

                    full_feed['instrument_key'] = instrument_key

                    self.executor.submit(self._process_feed, instrument_key, full_feed, metadata, ltt)

                except (KeyError, ValueError, TypeError) as e:
                    logger.error("Feed queueing error for %s: %s", instrument_key, e)
                    self.stats['errors'] += 1

        except (KeyError, ValueError, TypeError) as e:
            logger.error("Message handler error: %s", e)
            self.stats['errors'] += 1

    def _process_feed(self, instrument_key, full_feed, metadata, ltt):
        try:
            flat = self.extract_flat(full_feed, metadata, ltt)
            if not flat:
                return

            with self.metadata_lock:
                self.instrument_metadata[instrument_key] = flat

            self.stats['processed'] += 1

        except (KeyError, ValueError, TypeError) as e:
            logger.error("Feed processing error for %s: %s", instrument_key, e)
            self.stats['errors'] += 1

    def _update_nifty_spot(self, feed_info):
        try:
            full_feed = feed_info.get("fullFeed", {}).get("indexFF", {}) or \
                        feed_info.get("fullFeed", {}).get("marketFF", {})
            spot_price = full_feed.get("ltpc", {}).get('ltp')

            if spot_price and spot_price > 0:
                self.risk_calculator.update_spot_price(spot_price)
                self.stats['nifty_updates'] += 1

                if time.time() - self.last_spot_ssm_write > 300:
                    self.config.save_nifty_spot(spot_price)
                    self.last_spot_ssm_write = time.time()
                    logger.info("Nifty spot saved to SSM: %s", spot_price)

        except (KeyError, ValueError, TypeError) as e:
            logger.error("Nifty spot update error: %s", e)
            self.stats['errors'] += 1

    def extract_flat(self, feed_data, metadata, ltt):
        try:
            ltpc = feed_data.get("ltpc", {})
            greeks = feed_data.get("optionGreeks", {})
            ohlc_list = feed_data.get("marketOHLC", {}).get("ohlc", [])
            daily_ohlc = next((item for item in ohlc_list if item.get("interval") == "1d"), {})

            instrument_key = feed_data.get('instrument_key', '')
            trading_symbol = metadata.get('symbol', '')

            if not trading_symbol:
                strike = int(metadata.get('strike', 0))
                opt_type = metadata.get('option_type', '')
                trading_symbol = "NIFTY%d%s" % (strike, opt_type)

            expiry_date = metadata.get('expiry')
            if not expiry_date:
                self.stats['invalid_data'] += 1
                return None

            insertion_time = self._next_boundary(ltt)

            raw_data = {
                'time': insertion_time,
                'symbol': trading_symbol,
                'instrument_key': instrument_key,
                'strike': metadata.get('strike'),
                'expiry': metadata.get('expiry'),
                'option_type': metadata.get('option_type'),
                'ltp': ltpc.get('ltp'),
                'open': daily_ohlc.get('open'),
                'high': daily_ohlc.get('high'),
                'low': daily_ohlc.get('low'),
                'close': daily_ohlc.get('close'),
                'volume': daily_ohlc.get('vol'),
                'oi': feed_data.get('oi'),
                'iv': feed_data.get('iv'),
                'delta': greeks.get('delta'),
                'theta': greeks.get('theta'),
                'gamma': greeks.get('gamma'),
                'vega': greeks.get('vega'),
                'rho': greeks.get('rho')
            }

            cleaned = DataCleaner.clean_option_data(raw_data, self.risk_calculator.nifty_spot)
            if not cleaned:
                self.stats['invalid_data'] += 1
                return None

            risk = self.risk_calculator.calculate_risk_metrics(cleaned)

            flat = {
                'time':               insertion_time.isoformat(),
                'symbol':             cleaned['symbol'],
                'instrument_key':     cleaned['instrument_key'],
                'strike':             cleaned['strike'],
                'expiry':             str(cleaned['expiry']),
                'option_type':        cleaned['option_type'],
                'ltp':                cleaned.get('ltp', 0),
                'delta':              cleaned.get('delta', 0),
                'gamma':              cleaned.get('gamma', 0),
                'theta':              cleaned.get('theta', 0),
                'vega':               cleaned.get('vega', 0),
                'iv':                 cleaned.get('iv', 0),
                'oi':                 int(cleaned.get('oi', 0)),
                'volume':             int(cleaned.get('volume', 0)),
                'overall_risk_score': risk.get('overall_risk_score', 0),
                'recommendation':     risk.get('recommendation', 'HOLD'),
                'var_1day':           risk.get('var_1day', 0),
                'risk_pct':           risk.get('risk_pct', 0),
                'time_risk':          risk.get('time_risk', 0),
                'theta_burn_pct':     risk.get('theta_burn_pct', 0),
                'moneyness':          risk.get('moneyness', 0),
                'liquidity_score':    risk.get('liquidity_score', 0),
                'dte':                risk.get('dte', 0),
                'expected_move':      risk.get('expected_move', 0)
            }

            self.stats['total_received'] += 1
            return flat

        except (KeyError, ValueError, TypeError, IndexError) as e:
            logger.error("Extract error for %s: %s", metadata.get('symbol', 'unknown'), e)
            self.stats['invalid_data'] += 1
            self.stats['errors'] += 1
            return None

    def print_stats(self):
        try:
            logger.info("=" * 50)
            logger.info("STATISTICS")
            logger.info("Received: %d | Processed: %d | Invalid: %d",
                        self.stats['total_received'], self.stats['processed'], self.stats['invalid_data'])
            logger.info("Stale: %d | Not Subscribed: %d | Batches: %d | Errors: %d",
                        self.stats['stale_skipped'], self.stats['not_subscribed'], 
                        self.stats['batches_sent'], self.stats['errors'])
            logger.info("Nifty: %.2f | Metadata: %d",
                        self.risk_calculator.nifty_spot, len(self.instrument_metadata))
            logger.info("=" * 50)
        except (KeyError, ValueError) as e:
            logger.error("Stats error: %s", e)

    def shutdown(self):
        logger.info("Shutting down analyzer...")
        self.running = False
        self.executor.shutdown(wait=True, cancel_futures=False)
        time.sleep(1)

        with self.metadata_lock:
            if self.instrument_metadata:
                logger.info("Flushing %d remaining records on shutdown...", len(self.instrument_metadata))
                self._flush_batch()

        logger.info("Analyzer shutdown complete")