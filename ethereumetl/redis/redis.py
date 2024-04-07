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

    def exists_in_set(self, key, value):
        return self.redis_client.sismember(key, value)
    
    def add_to_set(self, key, value):
        return self.redis_client.sadd(key, value)
    
    def close(self):
        self.redis_client.close()
        self.redis_bf.close()
