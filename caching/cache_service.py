import redis
import json

class CacheService:
    name = "cache_service"

    def __init__(self):
        self.redis_client = redis.StrictRedis(host='localhost', port=6379)

    def cache_data(self, key, value, expiration=None):
        serialized_value = json.dumps(value)
        self.redis_client.set(key, serialized_value, ex=expiration)

    def retrieve_cached_data(self, key):
        cached_data = self.redis_client.get(key)
        if cached_data is not None:
            return json.loads(cached_data)
        
        return None
    
    def remove_from_cache(self, key):
        return self.redis_client.delete(key)