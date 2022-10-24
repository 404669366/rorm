package rorm

import (
	"context"
	"errors"
	"fmt"
	"github.com/adjust/rmq/v5"
	"github.com/go-redis/redis/v8"
	"strings"
	"time"
)

var Expire = 86400
var ExpireClear = 2
var ExpireRuntimeName = "RormExpireRuntime"
var QueueTag = "Rorm"
var QueueName = "RormQueue"

type runtime struct {
	Client *redis.Client
	Queue  rmq.Queue
}

func (r *runtime) parse(model interface{}) (*parse, error) {
	if r == nil {
		return nil, errors.New("please call init first")
	}
	p := &parse{
		Runtime: r,
		Model:   model,
		HashKey: "",
		LineKey: "",
	}
	err := p.parse()
	return p, err
}

func (r *runtime) clear() {
	if Expire > 0 && ExpireClear > 0 {
		ticker := time.NewTicker(time.Duration(ExpireClear) * time.Second)
		defer ticker.Stop()
		for range ticker.C {
			zr := &redis.ZRangeBy{
				Min: "-inf",
				Max: fmt.Sprintf("%v", time.Now().Unix()),
			}
			keys, _ := r.Client.ZRangeByScore(context.Background(), ExpireRuntimeName, zr).Result()
			_, _ = r.Client.ZRemRangeByScore(context.Background(), ExpireRuntimeName, zr.Min, zr.Max).Result()
			hash := make(map[string][]string)
			for _, key := range keys {
				temp := strings.Split(key, "-")
				if len(temp) == 2 {
					hash[temp[0]] = append(hash[temp[0]], temp[1])
				}
			}
			for hashKey, linKeys := range hash {
				r.Client.HDel(context.Background(), hashKey, linKeys...)
			}
		}
	}
}

func (r *runtime) queue() (err error) {
	conn, err := rmq.OpenConnectionWithRedisClient(QueueTag, r.Client, nil)
	if err == nil {
		r.Queue, err = conn.OpenQueue(QueueName)
	}
	return err
}
