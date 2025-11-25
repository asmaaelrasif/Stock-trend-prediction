import hashlib

def md5_of_file(path, chunk_size=8192):
    """Return hex md5 of a file."""
    h = hashlib.md5()
    with open(path, 'rb') as f:
        for chunk in iter(lambda: f.read(chunk_size), b''):
            h.update(chunk)
    return h.hexdigest()
