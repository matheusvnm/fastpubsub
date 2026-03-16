import sys
import types
from pathlib import Path

repo_root = Path(__file__).resolve().parents[2]
if str(repo_root) not in sys.path:
    sys.path.insert(0, str(repo_root))

docs_dir = repo_root / "docs"
docs_module = sys.modules.get("docs")
if docs_module is None:
    docs_module = types.ModuleType("docs")
    docs_module.__path__ = [str(docs_dir)]
    sys.modules["docs"] = docs_module
else:
    docs_paths = list(getattr(docs_module, "__path__", []))
    if str(docs_dir) not in docs_paths:
        docs_paths.insert(0, str(docs_dir))
        docs_module.__path__ = docs_paths
