from datetime import datetime, timezone, timedelta
import time
import logging

logger = logging.getLogger(__name__)

class DataCleaner:

    @staticmethod
    def clean_option_data(option_data, nifty_spot=24000):
        if not option_data:
            logger.warning("REJECTED: Received empty data dictionary.")
            return None

        try:
            critical_fields = ['symbol', 'strike', 'expiry', 'option_type']
            for field in critical_fields:
                if field not in option_data or option_data[field] is None:
                    logger.warning("REJECTED: Missing critical field '%s' in %s",
                                   field, option_data.get('symbol', 'unknown'))
                    return None

            try:
                strike = float(option_data['strike'])
                nifty_spot = float(nifty_spot)
            except (ValueError, TypeError) as e:
                logger.error("REJECTED: Numeric conversion error for %s: %s",
                             option_data.get('symbol'), e)
                return None

            if strike <= 0 or strike > 100000:
                logger.warning("REJECTED: Invalid strike price %s for %s",
                               strike, option_data.get('symbol'))
                return None
            option_data['strike'] = strike

            try:
                expiry_dt = datetime.strptime(option_data['expiry'], '%Y-%m-%d')
                if expiry_dt.date() < datetime.now().date():
                    logger.warning("REJECTED: Expired instrument %s (Expiry: %s)",
                                   option_data.get('symbol'), option_data['expiry'])
                    return None
            except (ValueError, TypeError) as e:
                logger.error("REJECTED: Date format error for %s: %s",
                             option_data.get('symbol'), e)
                return None

            if option_data['option_type'] not in ['CE', 'PE']:
                logger.warning("REJECTED: Invalid option_type %s for %s",
                               option_data['option_type'], option_data.get('symbol'))
                return None

            cleaned = option_data.copy()

            ist_now = datetime.now(timezone(timedelta(hours=5, minutes=30)))
            cleaned['time'] = option_data.get('time', ist_now)

            cleaned['ltp'] = DataCleaner._fill_ltp(cleaned)
            if cleaned['ltp'] <= 0:
                cleaned['ltp'] = 0.05

            cleaned = DataCleaner._fill_numeric_fields(cleaned, nifty_spot)
            return cleaned

        except KeyError as e:
            logger.error("REJECTED: Missing key %s for %s",
                         e, option_data.get('symbol', 'unknown'))
            return None
        except Exception as e:
            logger.exception("REJECTED: Unexpected error in clean_option_data for %s: %s",
                             option_data.get('symbol', 'unknown'), e)
            return None

    @staticmethod
    def _fill_ltp(data):
        try:
            ltp = data.get('ltp', 0)
            if ltp is not None and float(ltp) > 0:
                return float(ltp)

            close = data.get('close', 0)
            if close is not None and float(close) > 0:
                return float(close)

            high = data.get('high', 0)
            low = data.get('low', 0)
            if high is not None and low is not None:
                return (float(high) + float(low)) / 2

            return 0.0
        except (ValueError, TypeError):
            return 0.0

    @staticmethod
    def _fill_numeric_fields(data, nifty_spot):
        try:
            strike = float(data['strike'])
            option_type = data['option_type']
            ltp = float(data['ltp'])
            spot = float(nifty_spot)

            if option_type == 'CE':
                moneyness = spot - strike
            else:
                moneyness = strike - spot

            is_deep_itm = moneyness > 500
            is_itm      = 0 < moneyness <= 500
            is_atm      = abs(moneyness) < 100
            is_otm      = -500 <= moneyness < 0
            is_deep_otm = moneyness < -500

            def get_val(key, default):
                try:
                    val = data.get(key)
                    if val is None:
                        return default
                    f = float(val)
                    if f <= 0:
                        return default
                    return f
                except (ValueError, TypeError):
                    return default

            # ── IV ──────────────────────────────────────────────────────────
            # Upstox may send IV as decimal (0.18) or percentage (18.0)
            # Detect by checking if value < 2.0 → treat as decimal and multiply by 100
            raw_iv = data.get('iv')
            if raw_iv is None:
                data['iv'] = 18.0 if is_atm else (25.0 if is_deep_otm else 15.0)
            else:
                try:
                    iv_val = float(raw_iv)
                    if iv_val <= 0:
                        data['iv'] = 18.0 if is_atm else (25.0 if is_deep_otm else 15.0)
                    elif iv_val < 2.0:
                        # decimal form e.g. 0.18 → 18.0
                        data['iv'] = round(iv_val * 100, 4)
                        logger.debug("IV converted from decimal %.4f → %.4f for %s",
                                     iv_val, data['iv'], data.get('symbol'))
                    else:
                        data['iv'] = iv_val
                except (ValueError, TypeError):
                    data['iv'] = 18.0 if is_atm else (25.0 if is_deep_otm else 15.0)
            data['iv'] = max(0.0, min(200.0, data['iv']))

            # ── Delta ────────────────────────────────────────────────────────
            delta = data.get('delta')
            if delta is None:
                if is_deep_itm:   d = 0.9
                elif is_itm:      d = 0.7
                elif is_atm:      d = 0.5
                elif is_otm:      d = 0.3
                else:             d = 0.1
                data['delta'] = d if option_type == 'CE' else -d
            else:
                try:
                    data['delta'] = max(-1.0, min(1.0, float(delta)))
                except (ValueError, TypeError):
                    data['delta'] = 0.5 if option_type == 'CE' else -0.5

            # ── Gamma ────────────────────────────────────────────────────────
            data['gamma'] = get_val(
                'gamma',
                0.015 if is_atm else (0.010 if abs(moneyness) < 200 else 0.005)
            )

            # ── Theta ────────────────────────────────────────────────────────
            # Upstox may send theta as positive or negative depending on perspective.
            # We always store as negative (option buyer's theta decay).
            raw_theta = data.get('theta')
            if raw_theta is None:
                if is_atm:          data['theta'] = -ltp * 0.08
                elif is_deep_otm:   data['theta'] = -ltp * 0.15
                else:               data['theta'] = -ltp * 0.05
            else:
                try:
                    theta_val = float(raw_theta)
                    if theta_val > 0:
                        # Broker sent positive theta — negate it
                        logger.debug("Theta sign corrected: %.4f → %.4f for %s",
                                     theta_val, -theta_val, data.get('symbol'))
                        theta_val = -theta_val
                    data['theta'] = max(-1000.0, min(0.0, theta_val))
                except (ValueError, TypeError):
                    data['theta'] = -ltp * 0.05

            # ── Vega ─────────────────────────────────────────────────────────
            data['vega'] = get_val(
                'vega',
                ltp * 0.15 if is_atm else (ltp * 0.12 if abs(moneyness) < 200 else ltp * 0.08)
            )

            # ── OI / Volume ──────────────────────────────────────────────────
            data['oi']     = get_val('oi', 100.0)
            data['volume'] = get_val('volume', 10.0)

            # ── OHLC ─────────────────────────────────────────────────────────
            for field in ['open', 'high', 'low', 'close']:
                data[field] = get_val(field, ltp)

            # ── Rho ──────────────────────────────────────────────────────────
            data['rho'] = get_val('rho', 0.01 * ltp)

            return data

        except (ValueError, TypeError, KeyError) as e:
            logger.error("Error filling numeric fields for %s: %s",
                         data.get('symbol', 'unknown'), e)
            return data

    @staticmethod
    def detect_stale_data(last_update_time):
        try:
            current_time = time.time()
            last_ts = float(last_update_time)

            if current_time - last_ts > 300:
                now = datetime.now()
                # Fixed: was "now.hour < 58" which is always True
                market_open = (
                    (now.hour == 9 and now.minute >= 15) or
                    (9 < now.hour < 15) or
                    (now.hour == 15 and now.minute < 30)
                )
                if market_open:
                    return True
            return False
        except (ValueError, TypeError) as e:
            logger.debug("Failed to check data staleness: %s", e)
            return False

    @staticmethod
    def is_data_fresh(timestamp, max_age_seconds=5):
        try:
            if timestamp is None:
                return False
            current_time = time.time()
            age = current_time - float(timestamp)
            return age <= max_age_seconds
        except (ValueError, TypeError) as e:
            logger.debug("Failed to check data freshness: %s", e)
            return False