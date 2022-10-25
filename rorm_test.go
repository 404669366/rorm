package rorm

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/adjust/rmq/v5"
	"github.com/go-redis/redis/v8"
	"testing"
	"time"
)

var RDB *redis.Client

func init() {
	RDB = redis.NewClient(&redis.Options{
		Addr:        "192.168.0.22:6379",
		Password:    "123456",
		DB:          0,
		IdleTimeout: 300,
		PoolSize:    6,
	})
	_, err := RDB.Ping(context.Background()).Result()
	if err != nil {
		panic("Connect rdb error:\n" + err.Error())
	}

	Init(RDB)
}

type User struct {
	Id     int `rorm:"index"`
	Name   string
	Sex    int
	Age    int
	Status int `json:"-"`
}

func TestInit(t *testing.T) {
	Init(RDB)
}

func TestSave(t *testing.T) {
	user := &User{
		Id:     123,
		Name:   "awda",
		Sex:    1,
		Age:    1,
		Status: 1,
	}
	if err := Save(user); err != nil {
		fmt.Printf("err.Error(): %v\n", err.Error())
	}
}

func TestLoad(t *testing.T) {
	user := new(User)
	if err := Load(123, user); err != nil {
		fmt.Printf("err.Error(): %v\n", err.Error())
	}
	fmt.Printf("user: %v\n", user)
}

func TestDelete(t *testing.T) {
	Delete("123", &User{})
}

func TestConsume(t *testing.T) {
	err := Consume("Consumer", func(delivery rmq.Delivery) {
		msg := Parse(delivery)
		fmt.Printf("msg: %v\n", msg)
		user := new(User)
		if err := json.Unmarshal(msg.Data, user); err != nil {
			fmt.Printf("err.Error(): %v\n", err.Error())
			delivery.Reject()
		}
		fmt.Printf("user: %v\n", user)
		delivery.Ack()
	})
	if err != nil {
		fmt.Printf("err.Error(): %v\n", err.Error())
	}
	for true {
		time.Sleep(10 * time.Second)
		TestSave(nil)
	}
}
