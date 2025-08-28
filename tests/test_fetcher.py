"""
Test script for Sora's data fetching and analysis functionality
"""

import asyncio
import os
import sys
from datetime import datetime
from pathlib import Path

# Add src to path before importing local modules
sys.path.append(str(Path(__file__).parent.parent))

import pandas as pd
from loguru import logger
from src.data.fetcher import DataFetcher

# Initialize paths
TEST_DIR = Path(__file__).parent
LOG_DIR = TEST_DIR.parent / "logs"
CACHE_DIR = TEST_DIR.parent / "data" / "cache"
LOG_FILE = LOG_DIR / "test_fetcher.log"

# Test configuration
TEST_FUNDS = ["510300", "510500", "159915"]  # 沪深300ETF, 中证500ETF, 创业板ETF

async def test_current_data(fetcher):
    """Test fetching current market data"""
    logger.info("\n=== Testing Current Data Fetching ===")
    try:
        current_data = fetcher.batch_get_current_data(TEST_FUNDS)
        results = {}
        for code, data in current_data.items():
            if data:
                logger.info(f"Current data for {code}:")
                logger.info(f"  Name: {getattr(data, 'name', 'N/A')}")
                logger.info(f"  Price: {getattr(data, 'current_price', 'N/A')}")
                logger.info(f"  Change: {getattr(data, 'change_percent', 'N/A'):.2f}%")
                logger.info(f"  Volume: {getattr(data, 'volume', 'N/A'):,}")
                results[code] = True
            else:
                logger.warning(f"Failed to get data for {code}")
                results[code] = False
        
        success_rate = sum(results.values()) / len(results) * 100
        logger.info(f"Current data fetch success rate: {success_rate:.1f}%")
        return success_rate >= 80
    except Exception as e:
        logger.error(f"Error in test_current_data: {str(e)}")
        return False

async def run_tests():
    """Run all tests and report results"""
    test_results = {}
    
    try:
        # Initialize data fetcher with cache
        fetcher = DataFetcher(
            fund_codes=TEST_FUNDS,
            request_timeout=15,
            max_retries=3,
            cache_dir=str(CACHE_DIR)
        )
        logger.info("DataFetcher initialized successfully")
        
        # Run tests
        test_results['current_data'] = await test_current_data(fetcher)
        
        # Calculate overall success
        overall_success = all(test_results.values())
        test_results['overall'] = overall_success
        
        # Log final results
        logger.info("\n=== Test Results ===")
        for test_name, passed in test_results.items():
            status = "PASSED" if passed else "FAILED"
            logger.info(f"{test_name.upper():<20} {status}")
        
        return overall_success
        
    except Exception as e:
        logger.error(f"Test failed with error: {str(e)}")
        return False

if __name__ == "__main__":
    # Create necessary directories
    os.makedirs(LOG_DIR, exist_ok=True)
    os.makedirs(CACHE_DIR, exist_ok=True)
    
    # Configure logger
    logger.add(LOG_FILE, rotation="10 MB", level="INFO")
    logger.info("=" * 50)
    logger.info(f"Starting Sora Data Fetcher Test at {datetime.now()}")
    
    # Run tests
    success = asyncio.run(run_tests())
    
    # Exit with appropriate status code
    exit_code = 0 if success else 1
    logger.info(f"\nTest completed with exit code: {exit_code}")
    exit(exit_code)
