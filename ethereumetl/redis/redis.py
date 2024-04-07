import os
import redis
import logging
from redisbloom.client import Client

class RedisConnector:
    def __init__(self):
        redis_host = os.environ['REDIS_HOST']
        redis_port = os.environ['REDIS_PORT']
        redis_database = os.environ['REDIS_DB']

        self.redis_client = redis.StrictRedis(host=redis_host, port=redis_port, db=redis_database)
        self.redis_bf = Client(host=redis_host, port=redis_port, db=redis_database)

    # utility functions to be used in "live" sync_mode
    def exists_in_set(self, key, value):
        return self.redis_client.sismember(key, value)
    
    def add_to_set(self, key, value):
        return self.redis_client.sadd(key, value)
    
    # utility functions to be used in "backfill" sync_mode
    def exists_in_bf(self, key, value):
        key = f"bf_{key}"
        
        if not self.redis_client.exists(key):
            self.create_bf(key)
            return False # as BF was not present
        
        return self.redis_bf.bfExists(key, value)
        
    def add_to_bf(self, key, value):
        key = f"bf_{key}"
        
        if not self.redis_client.exists(key):
            self.create_bf(key)
        
        return self.redis_bf.bfAdd(key, value)
    
    def create_bf(self, key):
        # TODO: take these from envs
        self.redis_bf.bfCreate(key, 0.001, 1000000)
    
    def close(self):
        self.redis_client.close()
        self.redis_bf.close()
