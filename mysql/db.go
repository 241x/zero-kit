package mysql

import (
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
	"gorm.io/gorm/schema"
)

// Config MySQL 连接配置
type Config struct {
	Dsn    string
	Prefix string
}

// NewDB 获取数据库连接
func NewDB(cfg Config, logger logger.Interface) *gorm.DB {
	db, err := gorm.Open(mysql.Open(cfg.Dsn), &gorm.Config{
		Logger: logger,

		// 在自动迁移时，忽略外键约束
		DisableForeignKeyConstraintWhenMigrating: true,

		NamingStrategy: schema.NamingStrategy{
			TablePrefix:   cfg.Prefix,
			SingularTable: true,
		},
	})

	if err != nil {
		panic(err)
	}

	return db
}
