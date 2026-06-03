package helper

import (
	"strings"

	"golang.org/x/crypto/bcrypt"
)

// SafeLikeString 安全的模糊查询字符串
func SafeLikeString(str string) string {
	replacer := strings.NewReplacer("\\", "\\\\", "%", "\\%", "_", "\\_")
	return replacer.Replace(str)
}

// HashPassword 密码混淆
func HashPassword(password string) (string, error) {
	bytes, err := bcrypt.GenerateFromPassword([]byte(password), bcrypt.DefaultCost)
	return string(bytes), err
}

// CheckPassword 密码校验
func CheckPassword(password, hash string) bool {
	err := bcrypt.CompareHashAndPassword([]byte(hash), []byte(password))
	return err == nil
}
