# Copyright 2021 TradersOfTheUniverse S.A. All Rights Reserved.
#
# [Generic functions and imports related to Redis]
# #
# Authors:
#   antoniojose.luqueocana@telefonica.com
#   joseluis.roblesurquiza@telefonica.com
#   franciscojavier.gonzalezfernandez1@telefonica.com
#
# Version: 0.1
#

import bson
import json

from redis import StrictRedis
from os import environ


class RedisClient:
    """Clase singleton creada para gestionar la interaccion Redis"""

    __instance = None
    __init_called = False

    def __new__(cls):

        """ Generate singleton """

        if RedisClient.__instance is None:
            RedisClient.__instance = object.__new__(cls)
        return RedisClient.__instance

    def __init__(self):

        """ Initialize variables """

        if not self.__init_called:
            self.__init_called = True

            self.__redis_client = StrictRedis(
                host=environ.get('REDISHOST', 'localhost'),
                port=int(environ.get('REDISPORT', 6379)),
                decode_responses=False
            )

    def increase_count(self, key):

        """ Increases by 1 a number value stored in redis
        Args:
            key (string): Key from redis
        """

        return self.__redis_client.incr(key)

    def reset_count(self, key):

        """ Sets the value 0 to a key
        Args:
            key (string): Key from redis
        """

        return self.__redis_client.set(key, 0)

    def get_value(self, key):

        """ Returns a value stored in redis with a given key
        Args:
            key (string): Key to get value from redis
        Returns:
            The value gotten from redis, any type is possible
        """

        return self.__redis_client.get(key)

    def get_values(self, keys):

        """ Returns several values stored in redis with given keys
        Args:
            keys (list of string): Keys to get values from redis
        Returns:
            The list of values gotten from redis, any type is possible
        """

        return self.__redis_client.mget(keys)

    def save_value(self, key, value, expiration_seconds=None):

        """ Store a value in redis
        Args:
            key (string): Key to save value in redis
            value (any type): Value to be saved in redis
            expiration_seconds (int or None): After this number of seconds, this key-value will be erased from redis
        """

        self.__redis_client.set(
            name=key,
            value=value,
            ex=expiration_seconds
        )

    def save_values(self, name_value_ex_records):

        """ Store several values in redis
        Args:
            name_value_ex_records (list): List of dicts with name, value and ex
        """

        pipe = self.__redis_client.pipeline()
        for name_value_ex in name_value_ex_records:
            pipe.set(
                name=name_value_ex["name"],
                value=name_value_ex["value"],
                ex=name_value_ex["ex"]
            )
        pipe.execute()

    def get_dict(self, key, serialization="bson"):

        """ Returns a dict stored in redis with a given key
        Args:
            key (string): Key to get value from redis
            serialization (string): "bson" or "json", format to deserialize
        Returns:
            The value gotten from redis, a dict
        """

        value = self.get_value(key)
        if value:
            if serialization == "bson":
                return bson.loads(value)
            elif serialization == "json":
                return json.loads(value)

        return dict()

    def get_dicts(self, keys, serialization="bson"):

        """ Returns several dicts stored in redis with given keys
        Args:
            keys (list of string): Keys to get values from redis
            serialization (string): "bson" or "json", format to deserialize
        Returns:
            The values gotten from redis, a list of dicts
        """

        values = self.get_values(keys)
        if serialization == "bson":
            values = [bson.loads(f) if f else dict() for f in values]
        elif serialization == "json":
            values = [json.loads(f) if f else dict() for f in values]

        return values

    def save_dict(self, key, value, expiration_seconds=None, serialization="bson"):

        """ Store a dict in redis
        Args:
            key (string): Key to save value in redis
            value (dict): Value to be saved in redis
            expiration_seconds (int or None): After this number of seconds, this key-value will be erased from redis
            serialization (string): "bson" or "json", format to serialize
        """

        if serialization == "bson":
            value = bson.dumps(value)
        elif serialization == "json":
            value = json.dumps(value)

        self.save_value(key, value, expiration_seconds)

    def save_dicts(self, name_value_ex_records, serialization="bson"):

        """ Store a dict in redis
        Args:
            name_value_ex_records (list): List of dicts with name, value and ex
            serialization (string): "bson" or "json", format to serialize
        """

        for name_value_ex in name_value_ex_records:
            if serialization == "bson":
                name_value_ex["value"] = bson.dumps(name_value_ex["value"])
            else:
                name_value_ex["value"] = json.dumps(name_value_ex["value"])

        self.save_values(name_value_ex_records)

    def get_file(self, key, path):

        """ Write in a given path the file stored in redis with a given key
        Args:
            key (string): Key to get value from redis
            path (string): Path in disk to write the file gotten
        """

        value = self.get_value(key)
        if value:
            destination = open(path, 'wb')
            destination.write(value)
            destination.close()

    def save_file(self, key, path, expiration_seconds=None):

        """ Store in redis a file located in given path with a given key
        Args:
            key (string): Key to save value in redis
            path (string): Path in disk where is located the file to save in redis
            expiration_seconds (int or None): After this number of seconds, this key-value will be erased from redis
        """

        with open(path, "rb") as file:
            value = file.read()

            self.save_value(key, value, expiration_seconds)

    def clear_key(self, key):

        """ Delete a key-value from Redis
        Args:
            key (string): Key to be deleted
        """

        self.__redis_client.delete(key)

    def save_scored_value(self, key, value, score, expiration_seconds=None):

        """ Store a value in redis with a specific score
        Args:
            key (string): Key to save value in redis
            value (any type): Value to be saved in redis
            score (int): Score associated to this value
            expiration_seconds (int or None): After this number of seconds, this key-value will be erased from redis
        """

        # Possible ampliations:
        # self.__redis_client.zremrangebyscore(key, 0, score)  # Avoid duplicate Scores and identical data
        # Do stuff: zadd, expire...
        # self.__redis_client.zremrangebyrank(key, 0, -2) # Delete all the entries but the highest

        self.__redis_client.zadd(key, {value: score})
        if expiration_seconds:
            self.__redis_client.expire(key, expiration_seconds)

    def get_highest_scored_value(self, key, pop=False):

        """ Returns the highest scored value stored in redis with a given key
        Args:
            key (string): Key to get value from redis
            pop (bool): True for delete key from redis
        Returns:
            The value gotten from redis, any type is possible
        """

        values = self.__redis_client.zrange(key, 0, 0, desc=True)

        if not values:
            return None

        if pop:
            self.clear_key(key)

        return values[0]

    def get_all_scored_values(self, key, pop=False):
        """ Returns all scored value stored in redis with a given key
        Args:
            key (string): Key to get values from redis
            pop (bool): True for delete key from redis
        Returns:
            The values gotten from redis, any type is possible
        """

        values = self.__redis_client.zrange(key, 0, -1, desc=False)

        if values and pop:
            self.clear_key(key)

        return values

    def get_values_between_scores(self, key, min_score, max_score, pop=False):

        """ Returns all values between two scores stored in redis with a given key
        Args:
            key (string): Key to get values from redis
            min_score (int): Minimum score to filter
            max_score (int): Maximum score to filter
            pop (bool): True for delete gotten values from redis
        Returns:
            The values gotten from redis, any type is possible
        """

        values = self.__redis_client.zrangebyscore(key, min_score, max_score)

        if values and pop:
            self.__redis_client.zremrangebyscore(key, min_score, max_score)

        return values

    def clear_between_scores(self, key, min_score, max_score):
        """TODO doc"""
        self.__redis_client.zremrangebyscore(key, min_score, max_score)

    def is_first_time_this_key_is_checked(self, key, expiration_seconds=None):
        """ Returns True is this is the first time this function has been called for this key
        Args:
            key (string): The key to check
            expiration_seconds (int): Time to forget the key
        Returns:
            True or False
        """

        previous_value = self.__redis_client.getset(name=key, value=0)
        self.__redis_client.expire(name=key, time=expiration_seconds)

        return previous_value is None

    def get_pubsub(self):
        """ Create a pubsub to subscribe to notifications
        Returns:
            A pubsub ready to subscribe and be and listened
        """

        return self.__redis_client.pubsub()

    def change_expiration_milliseconds(self, key, expiration_milliseconds):

        """ Change the number of milliseconds of a key to expire
        Args:
            key (string): The key to whose expiration is going to be changed
            expiration_milliseconds (int): New milliseconds time to expire
        """

        self.__redis_client.pexpire(key, expiration_milliseconds)

    def flushall(self):
        """ Delete all keys in all databases on the current host"""
        self.__redis_client.flushall()