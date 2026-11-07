
import logging
import random
import time
from pathlib import Path

# Store logs inside the project's logs directory.
BASE_DIR = Path(__file__).resolve().parent
LOG_DIR = BASE_DIR / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)

LOG_FILE = LOG_DIR / "demo_app.log"

# Configure application logging.
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
    ],
)

logger = logging.getLogger("demo_app")

def simulate_event():
    """Generate one realistic application event."""
    events = [
        ("INFO", "User login successful"),
        ("INFO", "API request completed with status 200"),
        ("INFO", "Database query completed"),
        ("WARNING", "API response time is above threshold"),
        ("WARNING", "Database connection pool is nearly full"),
        ("ERROR", "Database connection failed"),
        ("ERROR", "API request failed with status 500"),
    ]

    level, message = random.choice(events)

    if level == "INFO":
        logger.info(message)
    elif level == "WARNING":
        logger.warning(message)
    else:
        logger.error(message)

def main():
    print(f"Demo application is generating logs in: {LOG_FILE}")
    print("Press Ctrl+C to stop.")

    try:
        while True:
            simulate_event()
            time.sleep(2)
    except KeyboardInterrupt:
        print("\nDemo application stopped.")

if __name__ == "__main__":
    main()
