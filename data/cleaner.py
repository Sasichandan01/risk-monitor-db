from datetime import datetime, timezone, timedelta
import time
import logging
import mibian
from datetime import datetime
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

            try:
                expiry_str = data.get('expiry', '')
                expiry_dt  = datetime.strptime(expiry_str, '%Y-%m-%d')
                dte        = max(1, (expiry_dt - datetime.now()).days)
            except (ValueError, TypeError):
                dte = 1

            # ── IV ──────────────────────────────────────────────────────────
            # Priority 1: raw feed (* 100 if decimal)
            # Priority 2: back-calculate from LTP using mibian implied volatility
            # Priority 3: NULL
            raw_iv = data.get('iv')
            if raw_iv is not None:
                try:
                    iv_val = float(raw_iv)
                    if iv_val <= 0:
                        data['iv'] = None
                    elif iv_val < 2.0:
                        # Upstox sends decimal e.g. 0.4878 → 48.78
                        data['iv'] = round(iv_val * 100, 4)
                    else:
                        data['iv'] = iv_val
                except (ValueError, TypeError):
                    data['iv'] = None
            else:
                data['iv'] = None

            # IV is None — try back-calculating from LTP
            if data['iv'] is None:
                try:
                    if spot > 0 and strike > 0 and ltp > 0 and dte > 0:
                        if option_type == 'CE':
                            model = mibian.BS(
                                [spot, strike, 6.5, dte],
                                callPrice=ltp
                            )
                        else:
                            model = mibian.BS(
                                [spot, strike, 6.5, dte],
                                putPrice=ltp
                            )
                        iv_implied = model.impliedVolatility
                        if iv_implied and iv_implied > 0:
                            data['iv'] = round(iv_implied, 4)
                            logger.debug(
                                "IV back-calculated for %s: %.4f",
                                data.get('symbol'), data['iv']
                            )
                        else:
                            data['iv'] = None
                except Exception as e:
                    logger.warning("IV back-calculation failed for %s: %s", data.get('symbol'), e)
                    data['iv'] = None

            if data['iv'] is not None:
                data['iv'] = max(0.0, min(200.0, data['iv']))

            # ── Compute BS greeks if any greek is missing ────────────────────
            # Only run mibian if at least one greek is None AND we have valid IV
            needs_bs = any(data.get(g) is None for g in ['delta', 'gamma', 'theta', 'vega'])
            bs_greeks = {}

            if needs_bs and data['iv'] is not None and spot > 0 and strike > 0 and dte > 0:
                try:
                    model = mibian.BS(
                        [spot, strike, 6.5, dte],
                        volatility=data['iv']
                    )
                    if option_type == 'CE':
                        bs_greeks = {
                            'delta': model.callDelta,
                            'gamma': model.gamma,
                            'theta': model.callTheta,
                            'vega':  model.vega / 100
                        }
                    else:
                        bs_greeks = {
                            'delta': model.putDelta,
                            'gamma': model.gamma,
                            'theta': model.putTheta,
                            'vega':  model.vega / 100
                        }
                    logger.debug(
                        "BS greeks computed for %s | delta=%.4f gamma=%.6f theta=%.4f vega=%.4f",
                        data.get('symbol'),
                        bs_greeks['delta'], bs_greeks['gamma'],
                        bs_greeks['theta'], bs_greeks['vega']
                    )
                except Exception as e:
                    logger.warning("BS computation failed for %s: %s", data.get('symbol'), e)
                    bs_greeks = {}

            # ── Delta ────────────────────────────────────────────────────────
            raw_delta = data.get('delta')
            if raw_delta is None:
                data['delta'] = bs_greeks.get('delta', None)
            else:
                try:
                    data['delta'] = float(raw_delta)
                except (ValueError, TypeError):
                    data['delta'] = bs_greeks.get('delta', None)

            # ── Gamma ────────────────────────────────────────────────────────
            raw_gamma = data.get('gamma')
            if raw_gamma is None:
                data['gamma'] = bs_greeks.get('gamma', None)
            else:
                try:
                    data['gamma'] = float(raw_gamma)
                except (ValueError, TypeError):
                    data['gamma'] = bs_greeks.get('gamma', None)

            # ── Theta ────────────────────────────────────────────────────────
            # Upstox sends correct daily theta — store raw as-is
            raw_theta = data.get('theta')
            if raw_theta is None:
                data['theta'] = bs_greeks.get('theta', None)
            else:
                try:
                    data['theta'] = float(raw_theta)
                except (ValueError, TypeError):
                    data['theta'] = bs_greeks.get('theta', None)

            # ── Vega ─────────────────────────────────────────────────────────
            raw_vega = data.get('vega')
            if raw_vega is None:
                data['vega'] = bs_greeks.get('vega', None)
            else:
                try:
                    data['vega'] = float(raw_vega)
                except (ValueError, TypeError):
                    data['vega'] = bs_greeks.get('vega', None)

            # ── OI / Volume ──────────────────────────────────────────────────
            # No formula for these — raw feed or NULL
            def get_val_or_none(key):
                try:
                    val = data.get(key)
                    if val is None:
                        return None
                    f = float(val)
                    return f if f > 0 else None
                except (ValueError, TypeError):
                    return None

            data['oi']     = get_val_or_none('oi')
            data['volume'] = get_val_or_none('volume')

            # ── OHLC ─────────────────────────────────────────────────────────
            # Use ltp as fallback for OHLC — these are always present for active strikes
            def get_val(key, default):
                try:
                    val = data.get(key)
                    if val is None:
                        return default
                    f = float(val)
                    return f if f > 0 else default
                except (ValueError, TypeError):
                    return default

            for field in ['open', 'high', 'low', 'close']:
                data[field] = get_val(field, ltp)

            # ── Rho ──────────────────────────────────────────────────────────
            data['rho'] = get_val_or_none('rho')

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