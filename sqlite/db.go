package sqlite

import (
	"fmt"

	"github.com/glebarez/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
	"gorm.io/gorm/schema"
)

// Config SQLite 连接配置
type Config struct {
	Dsn    string
	Prefix string
}

// NewDB 获取数据库连接
func NewDB(cfg Config, logger logger.Interface) (*gorm.DB, error) {
	db, err := gorm.Open(sqlite.Open(cfg.Dsn), &gorm.Config{
		Logger: logger,

		// 在自动迁移时，忽略外键约束
		DisableForeignKeyConstraintWhenMigrating: true,

		NamingStrategy: schema.NamingStrategy{
			TablePrefix:   cfg.Prefix,
			SingularTable: true,
		},
	})

	if err != nil {
		return nil, fmt.Errorf("sqlite connect failed: %w", err)
	}

	return db, nil
}
