import os
from pathlib import Path

RAW_DEFAULT_FILE = os.getenv(
    "UNILEVER_RAW_FILE",
    r"/Users/hamzacharmaqe/Documents/Unilever/Backup/April/UL_Raw_Data_APR_Backup.xlsx"
)

INBOUND_DIR = Path(
    os.getenv("UNILEVER_INBOUND_DIR", r"/Users/hamzacharmaqe/Documents/Unilever/inbound/April")
)

ARCHIVE_SUCCESS_DIR = Path(
    os.getenv("UNILEVER_ARCHIVE_SUCCESS", r"/Users/hamzacharmaqe/Documents/Unilever/archive/success")
)

ARCHIVE_FAILED_DIR = Path(
    os.getenv("UNILEVER_ARCHIVE_FAILED", r"/Users/hamzacharmaqe/Documents/Unilever/archive/failed")
)