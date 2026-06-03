package logger

import (
	"context"
	"fmt"
	"time"

	"github.com/rs/zerolog"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
)

// mongoWriter 日志写入器
type mongoWriter struct {
	db *mongo.Database
}

// Write 实现写入方法
func (m *mongoWriter) Write(p []byte) (n int, err error) {
	if m.db == nil {
		return 0, fmt.Errorf("mongoWriter: database is nil")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var logEntry bson.M
	if err := bson.UnmarshalExtJSON(p, false, &logEntry); err != nil {
		return 0, fmt.Errorf("mongoWriter: failed to unmarshal JSON: %w", err)
	}

	collectionName := "logs_" + time.Now().UTC().Format("20060102")
	if _, err = m.db.Collection(collectionName).InsertOne(ctx, logEntry); err != nil {
		return 0, fmt.Errorf("mongoWriter: failed to insert log: %w", err)
	}

	return len(p), nil
}

// WriteLevel 实现写入方法
func (m *mongoWriter) WriteLevel(level zerolog.Level, p []byte) (n int, err error) {
	return m.Write(p)
}
