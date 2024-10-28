import base64
import hashlib
import os


def sha256(value, secret=None):
    m = hashlib.new('sha256')
    value_bytes = bytes(value, 'utf-8')
    m.update(value_bytes)
    if secret is None:
        secret = base64.b64encode(os.urandom(32))
    m.update(secret)
    value_hash = m.hexdigest()
    return value_hash, secret
