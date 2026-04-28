from loguru import logger
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
from loaders.postgres_loader import mark_duplicates, get_all_raw_jobs

# These functions are now imported from postgres_loader
# but we keep backward compatibility by re-exporting them

__all__ = ['mark_duplicates', 'get_all_raw_jobs']