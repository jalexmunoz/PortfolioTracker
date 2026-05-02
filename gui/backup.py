import os
import shutil
from datetime import datetime
from typing import Optional



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


def create_auto_backup(db_path: str, backup_root: str, operation: str) -> Optional[str]:
    """Create a pre-write automatic backup of the active DB.

    Returns the absolute backup path on success, or None if the source DB does
    not exist (nothing to back up — caller may proceed). Raises on copy errors;
    callers must abort the write if this raises.
    """
    if not db_path or not os.path.exists(db_path):
        return None

    os.makedirs(backup_root, exist_ok=True)

    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    db_name = os.path.basename(db_path)
    stem, ext = os.path.splitext(db_name)
    ext = ext or ".db"
    op_token = (operation or "write").strip().replace(" ", "_") or "write"

    base = f"{stem}_{stamp}_before_{op_token}{ext}"
    backup_path = os.path.join(backup_root, base)
    counter = 0
    while os.path.exists(backup_path):
        counter += 1
        backup_path = os.path.join(
            backup_root,
            f"{stem}_{stamp}_before_{op_token}_{counter:02d}{ext}",
        )

    shutil.copy2(db_path, backup_path)
    return os.path.abspath(backup_path)
