import os
import redis
import hashlib
from ethereumetl.constants import constants

class RedisConnector:
    def __init__(self):
        self.ttl = os.environ['REDIS_LIVE_MESSAGE_TTL']
        
        redis_host = os.environ['REDIS_HOST']
        redis_port = os.environ['REDIS_PORT']
        redis_database = os.environ['REDIS_DB']
        
        self.redis_client = redis.StrictRedis(host=redis_host, port=redis_port, db=redis_database)

    # utility functions to be used in "live" sync_mode
    def exists_in_set(self, key, value):
        key = self.create_key(key, value, constants.REDIS_LIVE_MODE_PREFIX)
        return self.redis_client.exists(key)
    
    def add_to_set(self, key, value):
        key = self.create_key(key, value, constants.REDIS_LIVE_MODE_PREFIX)
        return self.redis_client.setex(key, self.ttl, '1')
    
    # utility functions to be used in "backfill" sync_mode
    def exists_in_bf(self, key, value):
        # TODO: add logic
        pass
        
    def add_to_bf(self, key, value):
        # TODO: add logic
        pass
    
    def create_bf(self, key):
        # TODO: add logic
        pass
    
    def create_key(self, key, value, mode):
        hashed_data = hashlib.sha1(f"{key}_{value}".encode()).hexdigest()
        return f"{mode}_{hashed_data}"
    
    def close(self):
        self.redis_client.close()
