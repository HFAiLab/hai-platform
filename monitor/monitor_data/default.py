

def get_container_monitor_stats():              return None
async def async_get_container_monitor_stats():  return None


def get_node_monitor_stats():               return None
async def async_get_node_monitor_stats():   return None


def get_storage_usage(*args, **kwargs):                 return None
async def async_get_storage_usage(*args, **kwargs):     return None
async def async_get_storage_usage_at(*args, **kwargs):   return None


StorageTypeToMeasurement = {}
StorageTypes = []
DefaultStorage = None
StorageLimits = {}
