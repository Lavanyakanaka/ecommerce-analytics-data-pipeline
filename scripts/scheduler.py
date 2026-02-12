import os
import time

from scripts.pipeline_orchestrator import run_pipeline


def main():
    interval_minutes = int(os.getenv("PIPELINE_SCHEDULE_MINUTES", "1440"))
    interval_seconds = interval_minutes * 60

    while True:
        run_pipeline()
        time.sleep(interval_seconds)


if __name__ == "__main__":
    main()
