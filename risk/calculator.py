import logging
import math
import time
from datetime import datetime

logger = logging.getLogger(__name__)

# Risk-free rate (annualised) — approximate 91-day T-bill rate for India
RISK_FREE_RATE = 0.065


class RiskCalculator:

    def __init__(self):
        self.nifty_spot = 24000
        """
        Initialize RiskCalculator instance.

        Sets default Nifty spot price to 24000 and last spot update time to 0.

        Attributes:
            nifty_spot (int): Last known Nifty spot price
            last_spot_update (float): Last update time for Nifty spot price
        """
        self.last_spot_update = 0

    def update_spot_price(self, spot):
        """
        Update Nifty spot price with latest value from index feed.

        Args:
            spot (int|float): Latest Nifty spot price from index feed

        Returns:
            None

        Raises:
            ValueError: If spot is not a valid number
            TypeError: If spot is not an int or float
        """
        try:
            spot = float(spot)
            if 10000 < spot < 50000:
                self.nifty_spot = spot
                self.last_spot_update = time.time()
            else:
                logger.warning("Invalid spot price: %s", spot)
        except (ValueError, TypeError) as e:
            logger.error("Spot update error: %s", e)

    # ------------------------------------------------------------------
    # Black-76 fair value
    # Correct model for Nifty — European options priced on index futures
    # ------------------------------------------------------------------
    def black76_price(self, option_type: str, forward: float, strike: float,
                      iv_pct: float, dte: int, r: float = RISK_FREE_RATE) -> float:
        """
        Compute Black-76 theoretical price for a European option on a futures/index.

        Black-76 treats the forward/futures price as the underlying directly,
        discounting by e^(-rT). This is the correct model for NSE index options
        (Nifty/BankNifty) which are European-style settled against futures.

        Args:
            option_type (str): 'CE' for call, 'PE' for put
            forward (float): Forward price = spot * e^(r*T)
            strike (float): Option strike price
            iv_pct (float): Implied volatility in percent (e.g. 15 means 15%)
            dte (int): Days to expiry
            r (float): Risk-free rate annualised (default RISK_FREE_RATE)

        Returns:
            float: Theoretical option price, or 0.0 on any input failure
        """
        try:
            if dte <= 0 or iv_pct <= 0 or forward <= 0 or strike <= 0:
                return 0.0

            T = dte / 365.0
            sigma = iv_pct / 100.0
            discount = math.exp(-r * T)

            d1 = (math.log(forward / strike) + 0.5 * sigma ** 2 * T) / (sigma * math.sqrt(T))
            d2 = d1 - sigma * math.sqrt(T)

            def norm_cdf(x):
                return 0.5 * (1.0 + math.erf(x / math.sqrt(2)))

            if option_type == 'CE':
                price = discount * (forward * norm_cdf(d1) - strike * norm_cdf(d2))
            else:
                price = discount * (strike * norm_cdf(-d2) - forward * norm_cdf(-d1))

            return max(0.0, round(price, 2))

        except (ValueError, TypeError, ZeroDivisionError) as e:
            logger.error("Black-76 pricing error: %s", e)
            return 0.0

    def _mispricing_score(self, ltp: float, fair_value: float) -> float:
        """
        Convert absolute LTP vs fair-value gap into a 0-100 risk score.

        Mispricing < 5%  → near zero risk contribution (fairly priced)
        Mispricing > 20% → high risk contribution (stale feed / illiquidity / trap)

        Args:
            ltp (float): Last traded price from feed
            fair_value (float): Black-76 theoretical price

        Returns:
            float: Mispricing risk score in range [0, 100]
        """
        try:
            if fair_value <= 0 or ltp <= 0:
                return 0.0
            mispricing_pct = abs(ltp - fair_value) / fair_value * 100
            # Scale: 0% gap → 0 score, 20%+ gap → 100 score
            return min(100.0, mispricing_pct * 5)
        except (ValueError, TypeError, ZeroDivisionError):
            return 0.0

    def calculate_risk_metrics(self, option_data):
        """
        Calculate risk metrics for an option.

        Composite risk score weights (total 100%):
            - Time risk (DTE buckets)   : 25%   [was 30%]
            - Theta burn %              : 25%   [unchanged]
            - VaR risk %                : 25%   [unchanged]
            - Black-76 mispricing       : 15%   [NEW]
            - Liquidity (inverted)      : 10%   [was 20%]

        Args:
            option_data (dict): Keys — symbol, strike, option_type, expiry,
                                delta, gamma, theta, vega, ltp, iv, oi, volume

        Returns:
            dict: Calculated risk metrics including fair_value and mispricing_pct.
        """
        try:
            delta = float(option_data.get('delta', 0))
            gamma = float(option_data.get('gamma', 0))
            theta = float(option_data.get('theta', 0))
            vega = float(option_data.get('vega', 0))
            ltp = float(option_data.get('ltp', 0))
            iv = float(option_data.get('iv', 15))
            oi = float(option_data.get('oi', 0))
            volume = float(option_data.get('volume', 0))
            strike = float(option_data.get('strike', 0))
            option_type = option_data.get('option_type', 'CE')
            expiry_str = option_data.get('expiry', '')

            # ---- DTE ----
            try:
                expiry_date = datetime.strptime(expiry_str, '%Y-%m-%d')
                dte = max(0, (expiry_date - datetime.now()).days)
            except (ValueError, TypeError):
                dte = 1

            # ---- Expected move (1-day, 95th percentile) ----
            try:
                if iv > 0 and self.nifty_spot > 0:
                    expected_move = self.nifty_spot * (iv / 100) * math.sqrt(1 / 252) * 1.65
                else:
                    expected_move = self.nifty_spot * 0.01
            except (ValueError, TypeError):
                expected_move = self.nifty_spot * 0.01

            # ---- VaR (1-day) ----
            try:
                price_pnl = delta * expected_move + 0.5 * gamma * (expected_move ** 2)
                var_1day = abs(price_pnl) + abs(vega * 5) + abs(theta)
            except (ValueError, TypeError):
                var_1day = ltp * 0.05

            # ---- Risk % ----
            try:
                risk_pct = (var_1day / ltp * 100) if ltp > 0 else 0
            except (ValueError, TypeError, ZeroDivisionError):
                risk_pct = 0

            # ---- Moneyness ----
            try:
                if self.nifty_spot > 0 and strike > 0:
                    if option_type == 'CE':
                        moneyness = (self.nifty_spot - strike) / self.nifty_spot * 100
                    else:
                        moneyness = (strike - self.nifty_spot) / self.nifty_spot * 100
                else:
                    moneyness = 0
            except (ValueError, TypeError):
                moneyness = 0

            # ---- Time risk (DTE buckets) ----
            if dte > 15:
                time_risk = 20
            elif dte >= 8:
                time_risk = 40
            elif dte >= 3:
                time_risk = 60
            elif dte >= 1:
                time_risk = 80
            else:
                time_risk = 100

            # ---- Theta burn % ----
            try:
                theta_burn_pct = (abs(theta) / ltp * 100) if ltp > 0 else 0
            except (ValueError, TypeError, ZeroDivisionError):
                theta_burn_pct = 0

            # ---- Liquidity score ----
            try:
                if oi > 0 and volume > 0:
                    liquidity_score = min(100, math.log10(oi * volume + 1) * 10)
                else:
                    liquidity_score = 0
            except (ValueError, TypeError):
                liquidity_score = 0

            # ---- Black-76 fair value + mispricing ----
            try:
                T = dte / 365.0
                forward = self.nifty_spot * math.exp(RISK_FREE_RATE * T)
                fair_value = self.black76_price(option_type, forward, strike, iv, dte)
                mispricing_pct = (
                    abs(ltp - fair_value) / fair_value * 100
                    if fair_value > 0 else 0.0
                )
                mispricing_score = self._mispricing_score(ltp, fair_value)
            except (ValueError, TypeError) as e:
                logger.warning("Black-76 block error: %s", e)
                fair_value = 0.0
                mispricing_pct = 0.0
                mispricing_score = 0.0

            # ---- Composite risk score ----
            # Weights: time 25% | theta 25% | var 25% | mispricing 15% | liquidity 10%
            try:
                overall_risk = (
                    time_risk                  * 0.25 +
                    min(100, theta_burn_pct)   * 0.25 +
                    min(100, risk_pct)         * 0.25 +
                    mispricing_score           * 0.15 +
                    (100 - liquidity_score)    * 0.10
                )
            except (ValueError, TypeError):
                overall_risk = 50

            # ---- Recommendation ----
            try:
                if overall_risk > 75:
                    recommendation = 'EXIT'
                elif overall_risk > 50:
                    recommendation = 'REDUCE'
                elif overall_risk < 30:
                    if moneyness > 0:
                        recommendation = 'BUY'
                    else:
                        recommendation = 'HOLD'
                else:
                    recommendation = 'HOLD'
            except (ValueError, TypeError):
                recommendation = 'HOLD'

            return {
                'var_1day':           round(var_1day, 2),
                'risk_pct':           round(risk_pct, 2),
                'moneyness':          round(moneyness, 2),
                'time_risk':          round(time_risk, 2),
                'theta_burn_pct':     round(theta_burn_pct, 2),
                'liquidity_score':    round(liquidity_score, 2),
                'fair_value':         round(fair_value, 2),
                'mispricing_pct':     round(mispricing_pct, 2),
                'overall_risk_score': round(overall_risk, 2),
                'recommendation':     recommendation,
                'dte':                dte,
                'expected_move':      round(expected_move, 2)
            }

        except (ValueError, TypeError, KeyError) as e:
            logger.error("Risk calculation error: %s", e)
            return {
                'var_1day': 0, 'risk_pct': 0, 'moneyness': 0,
                'time_risk': 50, 'theta_burn_pct': 0, 'liquidity_score': 0,
                'fair_value': 0, 'mispricing_pct': 0,
                'overall_risk_score': 50, 'recommendation': 'ERROR',
                'dte': 0, 'expected_move': 0
            }