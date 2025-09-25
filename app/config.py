# Centralized app configuration and theming
import os
from dataclasses import dataclass

@dataclass(frozen=True)
class Brand:
    name: str = "Pastas Frescas"
    primary: str = "#1e3a8a"   # azul profundo
    accent: str = "#b91c1c"    # rojo
    success: str = "#059669"   # verde
    warn: str = "#b45309"      # naranja
    muted: str = "#374151"     # gris texto

BRAND = Brand()

DB_FILENAME = "cajas_plasticas.db"

# Streamlit page config constants
PAGE_TITLE = "Pastas Frescas — Control de Cajas"
PAGE_ICON = "📦"


def get_db_path() -> str:
    """Return absolute path for the sqlite db both local and Streamlit Cloud."""
    base_dir = os.environ.get("STREAMLIT_CLOUD", None)
    if base_dir:
        return os.path.join("/mount/src", DB_FILENAME)
    return os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", DB_FILENAME)
