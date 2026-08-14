package job_test

import (
	"crypto/rand"
	"encoding/hex"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

// testMySQLDSN 返回 MySQL 集成测试连接串。
// 未配置环境变量 ZEROKIT_TEST_MYSQL_DSN 时跳过测试，
// 例如：user:pass@tcp(127.0.0.1:3306)/zero_kit_test?charset=utf8mb4&parseTime=true
func testMySQLDSN(t *testing.T) string {
	t.Helper()
	dsn := os.Getenv("ZEROKIT_TEST_MYSQL_DSN")
	if dsn == "" {
		t.Skip("ZEROKIT_TEST_MYSQL_DSN not set, skipping MySQL integration tests")
	}
	return dsn
}

// openTestDB 打开测试用 MySQL 连接
func openTestDB(t *testing.T) *gorm.DB {
	t.Helper()
	db, err := gorm.Open(mysql.Open(testMySQLDSN(t)), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	require.NoError(t, err)
	return db
}

// uniqueTableName 生成唯一的测试表名，避免测试间数据污染
func uniqueTableName() string {
	b := make([]byte, 4)
	_, _ = rand.Read(b)
	return "zk_job_test_" + hex.EncodeToString(b)
}

// dropTestTable 删除测试表并关闭连接
func dropTestTable(t *testing.T, db *gorm.DB, table string) {
	t.Helper()
	if err := db.Migrator().DropTable(table); err != nil {
		t.Logf("drop test table %s failed: %v", table, err)
	}
	sqlDB, _ := db.DB()
	sqlDB.Close()
}
