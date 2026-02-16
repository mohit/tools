import sys
from pathlib import Path

# Allow importing from parent directory without sys.path hacks in each test
sys.path.insert(0, str(Path(__file__).parent.parent))
