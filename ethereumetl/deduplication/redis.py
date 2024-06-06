import os
import hashlib
import redis
from ethereumetl.constants import constants

class RedisConnector:
    def __init__(self):
        self.ttl = os.environ['REDIS_MESSAGE_TTL']
        
        redis_host = os.environ['REDIS_HOST']
        redis_port = os.environ['REDIS_PORT']
        redis_database = os.environ['REDIS_DB']
        
        self.redis_client = redis.StrictRedis(host=redis_host, port=redis_port, db=redis_database)

    def exists_in_set(self, key, value):
        return self.redis_client.exists(self.create_key(key, value))
    
    def add_to_set(self, key, value):
        return self.redis_client.setex(self.create_key(key, value), self.ttl, '1')
    
    def create_key(self, key, value):
        hashed_data = hashlib.sha1(f"{key}_{value}".encode()).hexdigest()
        return f"{constants.REDIS_PREFIX}_{hashed_data}"
    
    def close(self):
        self.redis_client.close()
