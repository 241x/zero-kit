package redis

import (
	"fmt"

	"github.com/redis/go-redis/v9"
)

// Config Redis 连接配置
type Config struct {
	Host     string
	Port     int
	Password string
	DB       int
}

// New 新建Redis客户端
func New(cfg Config) *redis.Client {
	addr := fmt.Sprintf("%s:%d", cfg.Host, cfg.Port)
	client := redis.NewClient(&redis.Options{
		Addr:     addr,
		Password: cfg.Password,
		DB:       cfg.DB,
	})

	return client
}
