package ranktm

import (
	"github.com/jageros/db/redis"
	"log"
	"math"
	"strconv"
)

var offset = int64(math.Pow(10, 11))
var maxTime int64 = 9999999999

type IRank interface {
	Set(id string, score int, times int64)
	Add(id string, score int, times int64)
	Del(id string)
	GetRanking(id string) int
	GetRanks(idx1, idx2 int) []*RnkSt
	GetRanksCount() int
	GetNext(cnt int, conf func(id string) bool) []string
	Clear()
}

type ranker struct {
	rankName string
}

func (r *ranker) Set(id string, score int, times int64) {
	score += 1
	scoreVal := int64(score)*offset + maxTime - times
	_, err := redis.Do("ZADD", r.rankName, scoreVal, id)
	if err != nil {
		log.Printf("Rank Set Error: %v", err)
	}
}

func (r *ranker) Add(id string, score int, times int64) {
	oldScore, err := redis.Int64("ZSCORE", r.rankName, id)
	if err != nil {
		log.Printf("Rank Add ZSCORE Error: %v", err)
	}
	scoreVal := oldScore/offset + int64(score)
	scoreVal = scoreVal*offset + maxTime - times
	_, err = redis.Do("ZADD", r.rankName, scoreVal, id)
	if err != nil {
		log.Printf("Rank Add ZADD-INCR Error: %v", err)
	}
}

func (r *ranker) Del(id string) {
	_, err := redis.Do("ZREM", r.rankName, id)
	if err != nil {
		log.Printf("Rank Del Error: %v", err)
	}
}

func (r *ranker) Clear() {
	_, err := redis.Do("DEL", r.rankName)
	if err != nil {
		log.Printf("Rank Clear DEL error: %v", err)
	}
}

func (r *ranker) GetRanking(id string) int {
	ranking, err := redis.Int("ZREVRANK", r.rankName, id)
	if err != nil {
		//log.Printf("Rank GetRanking Error %v:", err)
	} else {
		ranking += 1
	}
	return ranking
}

func (r *ranker) GetRanks(idx1, idx2 int) []*RnkSt {
	var rts []*RnkSt
	values, err := redis.Strings("ZREVRANGE", r.rankName, idx1, idx2, "WITHSCORES")
	if err != nil {
		log.Printf("Rank GetRanks Error: %v", err)
	}
	for i := 0; i < len(values); i += 2 {
		ranking := 1
		if i == 2 {
			ranking = 2
		} else if i > 2 {
			ranking = i/2 + 1
		}
		val, _ := strconv.ParseInt(values[i+1], 10, 64)
		rts = append(rts, &RnkSt{
			ID:         values[i],
			Ranking:    ranking,
			Score:      int(val/offset) - 1,
			UpdateTime: maxTime - val%offset,
		})
	}
	return rts
}

func (r *ranker) GetRanksCount() int {
	count, err := redis.Int("ZCARD", r.rankName)
	if err != nil {
		log.Printf("GetRanksCount Error: %v", err)
	}
	return count
}

func (r *ranker) GetNext(cnt int, conf func(id string) bool) []string {
	var ids []string
	count := r.GetRanksCount()
	for i := 0; i < count; i++ {
		values, err := redis.Strings("ZREVRANGE", r.rankName, i, i)
		if err != nil {
			log.Printf("Rank GetRanks Error: %v", err)
			continue
		}
		if conf(values[0]) {
			ids = append(ids, values[0])
			if len(ids) >= cnt {
				break
			}
		}
	}
	return ids
}

func GetRanker(name string) IRank {
	return &ranker{rankName: name}
}

// =====================================

type RnkSt struct {
	ID         string
	Ranking    int
	Score      int
	UpdateTime int64
}
