package mongodb

import (
	"context"
	"time"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// Config MongoDB 连接配置
type Config struct {
	URI      string
	Database string
	Enabled  bool
}

type Conn struct {
	Client *mongo.Client
	DB     *mongo.Database
}

func NewConn(cfg Config) *Conn {
	if !cfg.Enabled {
		return &Conn{}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	clientOptions := options.Client().ApplyURI(cfg.URI)
	client, err := mongo.Connect(ctx, clientOptions)
	if err != nil {
		panic(err)
	}

	// Ping the primary
	if err := client.Ping(ctx, nil); err != nil {
		panic(err)
	}

	return &Conn{
		Client: client,
		DB:     client.Database(cfg.Database),
	}
}
