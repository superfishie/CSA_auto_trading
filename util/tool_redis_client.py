# -*- coding:utf-8 -*-
from redis import StrictRedis
import json
from configparser import ConfigParser  # for Python 3


class ToolRedisClient(StrictRedis):
    def __init__(self, cfg_fn):
        self.__read_configure(cfg_fn)
        StrictRedis.__init__(self, host=self._redis_host, port=self._redis_port, password=self._redis_passwd,
                             db=self._redis_db)
        self.redis_client = StrictRedis(host=self._redis_host, port=self._redis_port, password=self._redis_passwd, db=self._redis_db)

    def __read_configure(self, cfg_fn="redis.cfg"):
        cf = ConfigParser()
        cf.read(filenames=cfg_fn)
        # redis配置
        self._redis_host = cf.get("redis", "redis_host")
        self._redis_port = cf.getint("redis", "redis_port")
        self._redis_passwd = cf.get("redis", "redis_passwd")
        self._redis_db = cf.get("redis", "redis_db")

    def get_redis_client(self):
        return self.redis_client

    def set(self, name, value, ex=None, px=None, nx=False, xx=False):
        json_str = json.dumps(value, ensure_ascii=False)
        return self.redis_client.set(name, json_str, ex, px, nx, xx)

    def get(self, name):
        value = self.redis_client.get(name)
        if value:
            return json.loads(value)
        return value

    def lpush(self, name, *values):
        tmp_list = []
        for tmp_value in values:
            msg_str = json.dumps(tmp_value, ensure_ascii=False)
            tmp_list.append(msg_str)
        return self.redis_client.lpush(name, *tmp_list)

    def rpush(self, name, *values):
        tmp_list = []
        for tmp_value in values:
            msg_str = json.dumps(tmp_value, ensure_ascii=False)
            tmp_list.append(msg_str)
        return self.redis_client.rpush(name, *tmp_list)

    def brpop(self, keys, timeout=0):
        msg = self.redis_client.brpop(keys, timeout)
        if msg and msg[1]:
            return json.loads(msg[1])
        return msg

    def rpop(self, name):
        msg = self.redis_client.rpop(name)
        if msg:
            return json.loads(msg)
        return msg

    def lpop(self, name):
        msg = self.redis_client.lpop(name)
        if msg:
            return json.loads(msg)
        return msg

    def lrange(self, name, start, end):
        result = []
        tmp_list = self.redis_client.lrange(name, start, end)
        for value in tmp_list:
            tmp = json.loads(value)
            result.append(tmp)
        return result

    def lrem(self, name, value, count=0):
        value = json.dumps(value)
        self.redis_client.lrem(name, count, value)

    def hset(self, name, key, value):
        value = json.dumps(value, ensure_ascii=False)
        self.redis_client.hset(name=name, key=key, value=value)

    def hgetall(self, name):
        hdict = {}
        hdata = self.redis_client.hgetall(name)  # dict
        if hdata:
            for k, v in hdata.iteritems():
                hdict[k] = json.loads(v)
        return hdata

    def hmset(self, name, mapping):
        for k, v in mapping.iteritems():
            mapping[k] = json.dumps(v, ensure_ascii=False)
        self.redis_client.hmset(name=name, mapping=mapping)
