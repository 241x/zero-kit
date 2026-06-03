package helper

import (
	"math/rand"
	"time"
)

// _rand 随机数生成器
var _rand = rand.New(rand.NewSource(time.Now().UnixNano()))

// RandomInt 获取随机数
func RandomInt(n int) int {
	return _rand.Intn(n)
}

// RandomString 随机字符串
func RandomString(n int) string {
	var str = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
	var length = 62

	b := make([]byte, n)

	for i := range b {
		b[i] = str[RandomInt(length)]
	}

	return string(b)
}

// RandomStringWithSymbols 带特殊符号的随机字符串
func RandomStringWithSymbols(n int) string {
	var str = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ!@#$%^&*()_+-=[]{}|;:,.<>?"
	var length = len(str)

	b := make([]byte, n)

	for i := range b {
		b[i] = str[RandomInt(length)]
	}

	return string(b)
}
