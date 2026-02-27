import logging
import math
import time
from datetime import datetime

logger = logging.getLogger(__name__)


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

    def calculate_risk_metrics(self, option_data):
        """
        Calculate risk metrics for an option.

        Args:
            option_data (dict): A dictionary containing the following keys:
                - symbol (str)
                - strike (int|float)
                - option_type (str)
                - expiry (str)
                - delta (float)
                - gamma (float)
                - theta (float)
                - vega (float)
                - ltp (float)
                - iv (float)
                - oi (float)
                - volume (float)

        Returns:
            dict: Calculated risk metrics.
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

            try:
                expiry_date = datetime.strptime(expiry_str, '%Y-%m-%d')
                dte = max(0, (expiry_date - datetime.now()).days)
            except (ValueError, TypeError):
                dte = 1

            try:
                if iv > 0 and self.nifty_spot > 0:
                    expected_move = self.nifty_spot * (iv / 100) * math.sqrt(1 / 252) * 1.65
                else:
                    expected_move = self.nifty_spot * 0.01
            except (ValueError, TypeError):
                expected_move = self.nifty_spot * 0.01

            try:
                price_pnl = delta * expected_move + 0.5 * gamma * (expected_move ** 2)
                var_1day = abs(price_pnl) + abs(vega * 5) + abs(theta)
            except (ValueError, TypeError):
                var_1day = ltp * 0.05

            try:
                risk_pct = (var_1day / ltp * 100) if ltp > 0 else 0
            except (ValueError, TypeError, ZeroDivisionError):
                risk_pct = 0

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

            try:
                theta_burn_pct = (abs(theta) / ltp * 100) if ltp > 0 else 0
            except (ValueError, TypeError, ZeroDivisionError):
                theta_burn_pct = 0

            try:
                if oi > 0 and volume > 0:
                    liquidity_score = min(100, math.log10(oi * volume + 1) * 10)
                else:
                    liquidity_score = 0
            except (ValueError, TypeError):
                liquidity_score = 0

            try:
                overall_risk = (
                    time_risk * 0.30 +
                    min(100, theta_burn_pct) * 0.25 +
                    min(100, risk_pct) * 0.25 +
                    (100 - liquidity_score) * 0.20
                )
            except (ValueError, TypeError):
                overall_risk = 50

            try:
                if overall_risk > 75:
                    recommendation = 'EXIT'
                elif overall_risk > 50:
                    recommendation = 'REDUCE'
                elif overall_risk < 30:
                    if option_type == 'CE' and moneyness > 0:
                        recommendation = 'BUY'
                    elif option_type == 'PE' and moneyness > 0:
                        recommendation = 'BUY'
                    else:
                        recommendation = 'HOLD'
                else:
                    recommendation = 'HOLD'
            except (ValueError, TypeError):
                recommendation = 'HOLD'

            return {
                'var_1day': round(var_1day, 2),
                'risk_pct': round(risk_pct, 2),
                'moneyness': round(moneyness, 2),
                'time_risk': round(time_risk, 2),
                'theta_burn_pct': round(theta_burn_pct, 2),
                'liquidity_score': round(liquidity_score, 2),
                'overall_risk_score': round(overall_risk, 2),
                'recommendation': recommendation,
                'dte': dte,
                'expected_move': round(expected_move, 2)
            }

        except (ValueError, TypeError, KeyError) as e:
            logger.error("Risk calculation error: %s", e)
            return {
                'var_1day': 0, 'risk_pct': 0, 'moneyness': 0,
                'time_risk': 50, 'theta_burn_pct': 0, 'liquidity_score': 0,
                'overall_risk_score': 50, 'recommendation': 'ERROR',
                'dte': 0, 'expected_move': 0
            }