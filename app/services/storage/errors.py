class StorageError(Exception):
    pass

class StorageNotFoundError(StorageError):
    pass

class StoragePermissionError(StorageError):
    pass

class StorageTransientError(StorageError):
    pass
