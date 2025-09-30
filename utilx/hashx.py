import pandas as pd
import hashlib
import uuid
from typing import List

def generate_hash_key(df: pd.DataFrame, columns: List[str]) -> pd.Series:
    """從多個欄位產生 SHA256 Hash Key (UUID)"""
    # 確保所有欄位都是字串且無空值
    composite_key = df[columns].astype(str).fillna('').agg(''.join, axis=1)
    
    def to_uuid(x):
        sha256_hash = hashlib.sha256(x.encode()).digest()
        return uuid.UUID(bytes=sha256_hash[:16])

    return composite_key.apply(to_uuid)
    