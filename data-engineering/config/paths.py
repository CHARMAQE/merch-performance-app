import os
from pathlib import Path

RAW_DEFAULT_FILE = os.getenv(
    "UNILEVER_RAW_FILE",
    r"C:\Users\hamza\CHARMAQE\Unilever\Backup\April\UL_Raw_Data_APR_Backup.xlsx"
)

INBOUND_DIR = Path(
    os.getenv("UNILEVER_INBOUND_DIR", r"C:\Users\hamza\CHARMAQE\Unilever\inbound\April")
)

ARCHIVE_SUCCESS_DIR = Path(
    os.getenv("UNILEVER_ARCHIVE_SUCCESS", r"C:\Users\hamza\CHARMAQE\Unilever\archive\success")
)

ARCHIVE_FAILED_DIR = Path(
    os.getenv("UNILEVER_ARCHIVE_FAILED", r"C:\Users\hamza\CHARMAQE\Unilever\archive\failed")
)