import argparse
import logging
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class AirQualityPipeline:
    def __init__(
        self,
        mode: str = "realtime",
        days: int = 3,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        countries: list = ["THA", "LAO"],
        base_dir: Optional[str] = None
    ):
        """
        Initialize the Air Quality Pipeline.
        
        Args:
            mode: Processing mode ("realtime" or "historical")
            days: Number of days to collect for realtime mode
            start_date: Start date for historical mode (YYYY-MM-DD)
            end_date: End date for historical mode (YYYY-MM-DD)
            countries: List of country codes to process
            base_dir: Base directory for the project
        """
        self.mode = mode
        self.days = days
        self.start_date = start_date
        self.end_date = end_date
        self.countries = countries
        self.base_dir = Path(base_dir) if base_dir else Path.cwd()
        
        # Validate parameters
        self._validate_parameters()
        
        # Set up directories
        self.setup_directories()
        
    def _validate_parameters(self):
        """Validate input parameters."""
        if self.mode not in ["realtime", "historical"]:
            raise ValueError("Mode must be either 'realtime' or 'historical'")
            
        if self.mode == "historical":
            if not (self.start_date and self.end_date):
                raise ValueError("Historical mode requires both start_date and end_date")
            try:
                datetime.strptime(self.start_date, "%Y-%m-%d")
                datetime.strptime(self.end_date, "%Y-%m-%d")
            except ValueError:
                raise ValueError("Dates must be in YYYY-MM-DD format")
                
    def setup_directories(self):
        """Set up necessary directories."""
        # Create directory structure
        self.data_dir = self.base_dir / "data"
        self.raw_dir = self.data_dir / "raw"
        self.processed_dir = self.data_dir / "processed"
        
        # Create directories if they don't exist
        for dir_path in [self.data_dir, self.raw_dir, self.processed_dir]:
            dir_path.mkdir(parents=True, exist_ok=True)
            
    def collect_openaq_data(self):
        """Collect OpenAQ data based on mode."""
        logger.info("Starting OpenAQ data collection...")
        
        try:
            if self.mode == "historical":
                from src.data_processors.s3_historical_data import main as collect_historical
                sys.argv = [
                    "",
                    "--years", self.start_date[:4],
                    "--country-codes", "111",  # Thailand
                    "--output-dir", str(self.raw_dir / "openaq" / "historical")
                ]
                collect_historical()
            else:
                from src.collect_openaq_data import main as collect_realtime
                sys.argv = [
                    "",
                    "--mode", "realtime",
                    "--days", str(self.days)
                ]
                collect_realtime()
                
            logger.info("OpenAQ data collection completed successfully")
        except Exception as e:
            logger.error(f"Error collecting OpenAQ data: {str(e)}")
            raise
            
    def collect_airgradient_data(self):
        """Collect AirGradient data based on mode."""
        logger.info("Starting AirGradient data collection...")
        
        try:
            from src.collect_airgradient_data import main as collect_airgradient
            
            args = [""]
            if self.mode == "historical":
                args.extend([
                    "--mode", "historical",
                    "--start-date", self.start_date,
                    "--end-date", self.end_date
                ])
            else:
                args.extend([
                    "--mode", "realtime"
                    # Note: AirGradient realtime mode is hardcoded to collect past 2 days
                ])
                
            sys.argv = args
            collect_airgradient()
            logger.info("AirGradient data collection completed successfully")
        except Exception as e:
            logger.error(f"Error collecting AirGradient data: {str(e)}")
            raise
            
    def process_air_quality(self):
        """Process collected air quality data."""
        logger.info("Starting air quality processing...")
        
        try:
            from src.data_processors.process_air_quality import main as process_data
            
            args = [""]
            if self.mode == "historical":
                args.extend([
                    "--mode", "historical",
                    "--start-date", self.start_date,
                    "--end-date", self.end_date
                ])
            else:
                args.extend([
                    "--mode", "realtime",
                    "--hours", str(self.days * 24)
                ])
                
            args.extend(["--countries"] + self.countries)
            sys.argv = args
            
            process_data()
            logger.info("Air quality processing completed successfully")
        except Exception as e:
            logger.error(f"Error processing air quality data: {str(e)}")
            raise
            
    def run_pipeline(self):
        """Run the complete air quality pipeline."""
        try:
            logger.info(f"Starting air quality pipeline in {self.mode} mode")
            
            # Step 1: Collect OpenAQ data
            self.collect_openaq_data()
            
            # Step 2: Collect AirGradient data
            self.collect_airgradient_data()
            
            # Step 3: Process combined data
            self.process_air_quality()
            
            logger.info("Air quality pipeline completed successfully")
            return True
        except Exception as e:
            logger.error(f"Pipeline failed: {str(e)}")
            return False

def main():
    parser = argparse.ArgumentParser(description="Integrated Air Quality Pipeline")
    parser.add_argument("--mode", choices=["realtime", "historical"], default="realtime",
                      help="Processing mode")
    parser.add_argument("--days", type=int, default=3,
                      help="Days to process for realtime mode")
    parser.add_argument("--start-date", type=str,
                      help="Start date for historical mode (YYYY-MM-DD)")
    parser.add_argument("--end-date", type=str,
                      help="End date for historical mode (YYYY-MM-DD)")
    parser.add_argument("--countries", nargs="+", default=["THA", "LAO"],
                      help="Country codes to process")
    parser.add_argument("--base-dir", type=str,
                      help="Base directory for the project")
    
    args = parser.parse_args()
    
    pipeline = AirQualityPipeline(
        mode=args.mode,
        days=args.days,
        start_date=args.start_date,
        end_date=args.end_date,
        countries=args.countries,
        base_dir=args.base_dir
    )
    
    success = pipeline.run_pipeline()
    sys.exit(0 if success else 1)

if __name__ == "__main__":
    main() 