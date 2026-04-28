"""pytest conftest — adds project root to sys.path so imports work from tests/."""
import sys
from pathlib import Path

# Add the project root directory to sys.path
# This lets all test files do `from pipelines.xxx import ...` directly
sys.path.insert(0, str(Path(__file__).parent))
