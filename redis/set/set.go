package set

import (
	"github.com/jageros/db/redis"
	"log"
)

type RedisSet struct {
	SetName string
}

func NewRedisSet(name string) *RedisSet {
	return &RedisSet{
		SetName: name,
	}
}

func (s *RedisSet) AddItem(item string) {
	redis.Do("SADD", s.SetName, item)
}

func (s *RedisSet) PopRandomItem() string {
	result, err := redis.Strings("SPOP", s.SetName, 1)
	if err != nil {
		log.Printf("Redis set get item err=%v", err)
		return ""
	}
	return result[0]
}
