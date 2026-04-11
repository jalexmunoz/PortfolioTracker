import os
import shutil
from datetime import datetime



def create_backup(db_path: str, backup_root: str) -> str:
    if not db_path:
        raise ValueError("No active DB configured")

    if not os.path.exists(db_path):
        raise FileNotFoundError(f"Active DB does not exist: {db_path}")

    os.makedirs(backup_root, exist_ok=True)

    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    db_name = os.path.basename(db_path)
    stem, ext = os.path.splitext(db_name)
    ext = ext or ".db"
    backup_name = f"{stem}_backup_{stamp}{ext}"
    backup_path = os.path.join(backup_root, backup_name)

    shutil.copy2(db_path, backup_path)
    return os.path.abspath(backup_path)
