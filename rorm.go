package rorm

import (
	"errors"
	"fmt"
	"github.com/adjust/rmq/v5"
	"github.com/go-redis/redis/v8"
	"time"
)

var running *runtime

func Init(redis *redis.Client) {
	if running == nil {
		running = &runtime{
			Client: redis,
		}
		if err := running.queue(); err != nil {
			panic(fmt.Sprintf("rorm: %v", err.Error()))
		}
		go running.clear()
	}
}

func Save(model interface{}, queue ...bool) error {
	table, err := running.parse(model)
	if err != nil {
		return err
	}
	if len(queue) > 0 {
		return table.save(queue[0])
	}
	return table.save(true)
}

func Load(index, model interface{}) error {
	table, err := running.parse(model)
	if err != nil {
		return err
	}
	return table.load(index)
}

func Delete(index, model interface{}) {
	if table, err := running.parse(model); err == nil {
		table.delete(index)
	}
}

func Consume(name string, callback func(delivery rmq.Delivery)) error {
	if running == nil {
		return errors.New("please call init first")
	}
	if err := running.Queue.StartConsuming(1000, 100*time.Millisecond); err != nil {
		return err
	}
	if _, err := running.Queue.AddConsumerFunc(name, callback); err != nil {
		return err
	}
	return nil
}

func Parse(delivery rmq.Delivery) *Event {
	event := new(Event)
	_ = event.decode(delivery.Payload())
	return event
}
