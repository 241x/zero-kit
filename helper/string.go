package helper

import (
	"crypto/md5"
	"encoding/hex"
)

// StringMd5 获取字符串的md5值
func StringMd5(data string) string {
	hash := md5.New()
	hash.Write([]byte(data))
	hashBytes := hash.Sum(nil)
	return hex.EncodeToString(hashBytes)
}
