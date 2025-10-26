package redisdb

import (
	"fmt"

	"github.com/alexkalak/go_market_analyze/src/helpers/envhelper"
	"github.com/redis/go-redis/v9"
)

type RedisDatabase struct {
	rdb *redis.Client
}

var singleton *RedisDatabase

func (d *RedisDatabase) GetDB() (*redis.Client, error) {
	if d.rdb == nil {
		dbStruct, err := New()
		if err != nil {
			return nil, err
		}
		d.rdb = dbStruct.rdb
		return d.rdb, nil
	}

	return d.rdb, nil
}

func New() (*RedisDatabase, error) {
	if singleton != nil {
		return singleton, nil
	}

	env, err := envhelper.GetEnv()
	if err != nil {
		return nil, err
	}

	singleton = &RedisDatabase{}

	fmt.Println("redis server: ", env.REDIS_SERVER)
	rdb := redis.NewClient(&redis.Options{
		Addr: env.REDIS_SERVER,
	})

	singleton.rdb = rdb
	return singleton, nil
}
