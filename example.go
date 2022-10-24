package rorm

import (
	"context"
	"fmt"
	"github.com/go-redis/redis/v8"
	"sync"
)

type User struct {
	Id     int `rorm:"index"`
	Name   string
	Sex    int
	Age    int
	Status int `json:"-"`
}

func main() {
	wait := &sync.WaitGroup{}
	wait.Add(1)
	InitRDB()
	Init(GetRDB())
	userA := &User{
		Id:     123,
		Name:   "awda",
		Sex:    1,
		Age:    1,
		Status: 1,
	}
	if err := Save(userA); err != nil {
		fmt.Printf("err.Error(): %v\n", err.Error())
	}
	userB := &User{
		Id:     456,
		Name:   "awda",
		Sex:    1,
		Age:    1,
		Status: 1,
	}
	if err := Save(userB); err != nil {
		fmt.Printf("err.Error(): %v\n", err.Error())
	}
	userC := &User{
		Id:     789,
		Name:   "awda",
		Sex:    1,
		Age:    1,
		Status: 1,
	}
	if err := Save(userC); err != nil {
		fmt.Printf("err.Error(): %v\n", err.Error())
	}

	Delete(789, &User{})

	//
	//userB := new(model.User)
	//if err := rorm.Load(456, userB); err != nil {
	//	fmt.Printf("err.Error(): %v\n", err.Error())
	//}
	//fmt.Printf("userB: %v\n", userB)
	wait.Wait()
}

var rdb *redis.Client

func InitRDB() {
	rdb = redis.NewClient(&redis.Options{
		Addr:        "192.168.0.22:6379",
		Password:    "123456",
		DB:          0,
		IdleTimeout: 300,
		PoolSize:    6,
	})
	_, err := rdb.Ping(context.Background()).Result()
	if err != nil {
		panic("Connect rdb error:\n" + err.Error())
	}
}

func GetRDB() *redis.Client {
	return rdb
}
