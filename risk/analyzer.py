import threading
import time
import logging
import json
import os
import psycopg2
from datetime import datetime, timezone, timedelta
from botocore.exceptions import ClientError

import boto3

from risk.calculator import RiskCalculator
from data.cleaner import DataCleaner

logger = logging.getLogger(__name__)

IST = timezone(timedelta(hours=5, minutes=30))
MIN_VALID_TS = datetime(2025, 1, 1, tzinfo=IST).timestamp()

sqs = boto3.client('sqs', region_name='ap-south-1')
INSERT_QUEUE_URL = 'https://sqs.ap-south-1.amazonaws.com/103371257687/OptionDataInsertQueue'

BACKUP_FILE = '/tmp/latest_data_backup.json'


class OptionsRiskAnalyzer:

    def __init__(self, fetcher, config):
        self.fetcher = fetcher
        self.config = config
        self.risk_calculator = RiskCalculator()
        self.latest_data = {}
        self.latest_data_lock = threading.Lock()
        self.running = True
        self.last_spot_ssm_write = 0
        self.empty_flush_count = 0
        self._restarting = False
        self.stats = {
            'total_received': 0,
            'invalid_data': 0,
            'stale_skipped': 0,
            'processed': 0,
            'batches_sent': 0,
            'nifty_updates': 0,
            'errors': 0,
            'ws_restarts': 0
        }
        self._restore_backup()

    def _restore_backup(self):
        """On startup, restore any records saved during last restart"""
        if os.path.exists(BACKUP_FILE):
            try:
                with open(BACKUP_FILE, 'r') as f:
                    restored = json.load(f)
                with self.latest_data_lock:
                    for key, record in restored.items():
                        if key not in self.latest_data:
                            self.latest_data[key] = record
                logger.info("Restored %d records from backup file", len(restored))
                os.remove(BACKUP_FILE)
            except (OSError, ValueError, json.JSONDecodeError) as e:
                logger.error("Backup restore failed: %s", e)

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
                # Catch all — batch writer must never die
                logger.error("Batch writer error — continuing: %s", e)
                self.stats['errors'] += 1
                time.sleep(30)

    def _flush_batch(self):
        snapshot = {}
        try:
            with self.latest_data_lock:
                if not self.latest_data:
                    self.empty_flush_count += 1
                    logger.warning(
                        "Nothing to flush at %s — empty count: %d/2",
                        datetime.now(IST).strftime('%H:%M:%S'),
                        self.empty_flush_count
                    )
                    if self.empty_flush_count >= 2 and not self._restarting:
                        logger.warning("60s of no data — triggering WebSocket restart")
                        self.empty_flush_count = 0
                        threading.Thread(
                            target=self._restart_websocket,
                            daemon=True,
                            name="ws-restart"
                        ).start()
                    return

                # Data is flowing — reset the counter
                self.empty_flush_count = 0
                snapshot = dict(self.latest_data)
                self.latest_data.clear()

            records = list(snapshot.values())
            logger.info("Flushing %d records at %s", len(records), datetime.now(IST).strftime('%H:%M:%S'))

            payload = json.dumps({
                'batch_time': datetime.now(IST).isoformat(),
                'records': records
            }, default=str)

            payload_kb = len(payload.encode('utf-8')) / 1024
            logger.info("Payload size: %.1fKB", payload_kb)

            sqs.send_message(
                QueueUrl=INSERT_QUEUE_URL,
                MessageBody=payload
            )

            self.stats['batches_sent'] += 1
            logger.info("Batch sent to SQS — %d records", len(records))

        except ClientError as e:
            logger.error("SQS send failed — returning records to buffer: %s", e)
            with self.latest_data_lock:
                for key, record in snapshot.items():
                    if key not in self.latest_data:
                        self.latest_data[key] = record
            self.stats['errors'] += 1

        except (ValueError, TypeError) as e:
            logger.error("Flush error: %s", e)
            self.stats['errors'] += 1

    def _restart_websocket(self):
        self._restarting = True
        try:
            logger.warning("WebSocket restart initiated — attempt #%d",
                           self.stats['ws_restarts'] + 1)

            # Step 1 — backup latest_data so no records are lost
            with self.latest_data_lock:
                if self.latest_data:
                    try:
                        with open(BACKUP_FILE, 'w') as f:
                            json.dump(self.latest_data, f, default=str)
                        logger.info("Backed up %d records to %s",
                                    len(self.latest_data), BACKUP_FILE)
                    except (OSError, ValueError) as e:
                        logger.error("Backup write failed: %s", e)

            # Step 2 — disconnect existing WebSocket
            try:
                if self.fetcher._streamer:
                    self.fetcher._streamer.disconnect()
                    logger.info("Streamer disconnected")
            except Exception as e:
                logger.error("Streamer disconnect error: %s", e)

            time.sleep(5)  # let the connection fully close

            # Step 3 — restore backup into latest_data
            if os.path.exists(BACKUP_FILE):
                try:
                    with open(BACKUP_FILE, 'r') as f:
                        restored = json.load(f)
                    with self.latest_data_lock:
                        for key, record in restored.items():
                            if key not in self.latest_data:
                                self.latest_data[key] = record
                    logger.info("Restored %d records from backup", len(restored))
                    os.remove(BACKUP_FILE)
                except (OSError, ValueError, json.JSONDecodeError) as e:
                    logger.error("Backup restore failed: %s", e)

            # Step 4 — reconnect
            self.stats['ws_restarts'] += 1
            logger.info("Restarting WebSocket polling...")
            self.fetcher.start_polling(self.on_message_handler)

        except Exception as e:
            logger.error("WebSocket restart failed: %s", e)
            self.stats['errors'] += 1
        finally:
            self._restarting = False

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
                        continue

                    full_feed['instrument_key'] = instrument_key

                    flat = self.extract_flat(full_feed, metadata, ltt)
                    if not flat:
                        continue

                    with self.latest_data_lock:
                        self.latest_data[instrument_key] = flat

                    self.stats['processed'] += 1

                except (KeyError, ValueError, TypeError) as e:
                    logger.error("Feed processing error for %s: %s", instrument_key, e)
                    self.stats['errors'] += 1

        except (KeyError, ValueError, TypeError) as e:
            logger.error("Message handler error: %s", e)
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
            logger.info("Stale Skipped: %d | Batches Sent: %d | Errors: %d",
                        self.stats['stale_skipped'], self.stats['batches_sent'], self.stats['errors'])
            logger.info("Nifty: %.2f | Latest Data: %d | WS Restarts: %d",
                        self.risk_calculator.nifty_spot, len(self.latest_data), self.stats['ws_restarts'])
            logger.info("=" * 50)
        except (KeyError, ValueError) as e:
            logger.error("Stats error: %s", e)

    def shutdown(self):
        logger.info("Shutting down analyzer...")
        self.running = False
        time.sleep(1)

        with self.latest_data_lock:
            if self.latest_data:
                logger.info("Flushing %d remaining records on shutdown...", len(self.latest_data))
                self._flush_batch()

        logger.info("Analyzer shutdown complete")