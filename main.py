import sys
import signal
import logging
from config import config
from data.fetcher import StockDataFetcher
from risk.analyzer import OptionsRiskAnalyzer

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(name)s: %(message)s'
)
logger = logging.getLogger(__name__)

def main():
    """
    Main entry point for the Options Risk Analyzer.

    Initializes the StockDataFetcher and OptionsRiskAnalyzer,
    and starts the analyzer. Also sets up signal handlers for
    SIGTERM and SIGINT to gracefully shut down the analyzer.
    """
    fetcher = StockDataFetcher(config)

    if not (fetcher.load_token() or fetcher.login()):
        logger.error("✗ Login failed")
        sys.exit(1)

    logger.info("✓ Ready")

    analyzer = OptionsRiskAnalyzer(fetcher, config)

    def shutdown():
        logger.info("Shutting down...")
        analyzer.shutdown()
        sys.exit(0)

    signal.signal(signal.SIGTERM, shutdown)
    signal.signal(signal.SIGINT, shutdown)

    analyzer.start()

if __name__ == "__main__":
    main()