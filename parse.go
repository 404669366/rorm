package rorm

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/go-redis/redis/v8"
	"reflect"
	"time"
)

type parse struct {
	Runtime *runtime
	Model   interface{}
	HashKey string
	LineKey string
}

func (p *parse) parse() error {
	if p.Runtime.Client == nil {
		return errors.New("client must be initialized")
	}
	modelV := reflect.ValueOf(p.Model)
	if modelV.Kind() != reflect.Ptr {
		return errors.New("model must be used pointer")
	}
	modelV = modelV.Elem()
	modelT := modelV.Type()
	p.HashKey = "Rorm" + modelT.Name()
	for i := 0; i < modelT.NumField(); i++ {
		if modelT.Field(i).Tag.Get("rorm") == "index" {
			if modelV.Field(i).CanInterface() {
				p.LineKey = fmt.Sprintf("%v", modelV.Field(i).Interface())
				return nil
			}
		}
	}
	return errors.New(`model be set index tag eg: rorm:"index"`)
}

func (p *parse) save(queue bool) error {
	temp, err := json.Marshal(p.Model)
	if err != nil {
		return err
	}
	err = p.expire()
	if err != nil {
		return err
	}
	_, err = p.Runtime.Client.HSet(context.Background(), p.HashKey, p.LineKey, temp).Result()
	if queue {
		p.queue(temp)
	}
	return err
}

func (p *parse) load(index interface{}) error {
	p.LineKey = fmt.Sprintf("%v", index)
	_ = p.expire()
	temp, err := p.Runtime.Client.HGet(context.Background(), p.HashKey, p.LineKey).Result()
	if err != nil {
		return err
	}
	return json.Unmarshal([]byte(temp), p.Model)
}

func (p *parse) delete(index interface{}) {
	p.LineKey = fmt.Sprintf("%v", index)
	p.Runtime.Client.HDel(context.Background(), p.HashKey, p.LineKey)
	p.Runtime.Client.ZRem(context.Background(), ExpireRuntimeName, fmt.Sprintf("%v-%v", p.HashKey, p.LineKey))
}

func (p *parse) expire() error {
	_, err := p.Runtime.Client.ZAdd(context.Background(), ExpireRuntimeName, &redis.Z{
		Score:  float64(time.Now().Unix() + int64(Expire)),
		Member: fmt.Sprintf("%v-%v", p.HashKey, p.LineKey),
	}).Result()
	return err
}

func (p *parse) queue(data []byte) {
	event := &Event{
		Cmd:  p.HashKey,
		Data: data,
	}
	_ = p.Runtime.Queue.PublishBytes(event.encode())
}

type Event struct {
	Cmd  string
	Data []byte
}

func (e *Event) encode() []byte {
	temp, _ := json.Marshal(e)
	return temp
}

func (e *Event) decode(data string) error {
	return json.Unmarshal([]byte(data), e)
}
