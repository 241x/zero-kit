package apperror

// Code 应用错误码
type Code struct {
	value      int
	name       string
	defaultMsg string
}

// NewCode 创建错误码
func NewCode(value int, name, defaultMsg string) Code {
	return Code{
		value:      value,
		name:       name,
		defaultMsg: defaultMsg,
	}
}

// Value 返回错误码数值
func (c Code) Value() int {
	return c.value
}

// String 返回错误码名称
func (c Code) String() string {
	return c.name
}

// DefaultMsg 返回默认消息
func (c Code) DefaultMsg() string {
	return c.defaultMsg
}
