package redis

import (
	"github.com/gomodule/redigo/redis"
	"log"
	"time"
)

var rd *redisDB

type redisDB struct {
	pool redis.Pool
}

func (r *redisDB) init(maxIdle, maxActive, idleTimeout, dbIndex, connectTimeout, readTimeout, writeTimeout int, connType, addr, password string) {
	r.pool = redis.Pool{
		MaxIdle:     maxIdle,
		MaxActive:   maxActive,
		IdleTimeout: time.Duration(idleTimeout) * time.Second,
		Wait:        true,
		//TestOnBorrow:    nil,
		//MaxConnLifetime: 0,
		Dial: func() (redis.Conn, error) {
			con, err := redis.Dial(connType, addr,
				redis.DialPassword(password),
				redis.DialDatabase(dbIndex),
				redis.DialConnectTimeout(time.Duration(connectTimeout)*time.Second),
				redis.DialReadTimeout(time.Duration(readTimeout)*time.Second),
				redis.DialWriteTimeout(time.Duration(writeTimeout)*time.Second))
			if err != nil {
				return nil, err
			}
			return con, nil
		},
	}
}

func (r *redisDB) connCall(callback func(conn redis.Conn)) {
	conn := r.pool.Get()
	defer conn.Close()
	callback(conn)
}

func (r *redisDB) close() {
	err := r.pool.Close()
	if err != nil {
		log.Printf("redis close err: %v", err)
	}
}

func Do(commandName string, args ...interface{}) (result interface{}, err error) {
	rd.connCall(func(conn redis.Conn) {
		result, err = conn.Do(commandName, args...)
	})
	return
}

func Int64(commandName string, args ...interface{}) (result int64, err error) {
	rd.connCall(func(conn redis.Conn) {
		result, err = redis.Int64(conn.Do(commandName, args...))
	})
	return
}

func Int(commandName string, args ...interface{}) (result int, err error) {
	rd.connCall(func(conn redis.Conn) {
		result, err = redis.Int(conn.Do(commandName, args...))
	})
	return
}

func Int64s(commandName string, args ...interface{}) (result []int64, err error) {
	rd.connCall(func(conn redis.Conn) {
		result, err = redis.Int64s(conn.Do(commandName, args...))
	})
	return
}

func String(commandName string, args ...interface{}) (result string, err error) {
	rd.connCall(func(conn redis.Conn) {
		result, err = redis.String(conn.Do(commandName, args...))
	})
	return
}

func Strings(commandName string, args ...interface{}) (result []string, err error) {
	rd.connCall(func(conn redis.Conn) {
		result, err = redis.Strings(conn.Do(commandName, args...))
	})
	return
}

func Initialize(maxIdle, maxActive, idleTimeout, dbIndex, connectTimeout, readTimeout, writeTimeout int, connType, addr, password string) {
	rd = &redisDB{}
	rd.init(maxIdle, maxActive, idleTimeout, dbIndex, connectTimeout, readTimeout, writeTimeout, connType, addr, password)
}

func Close() {
	rd.close()
}
