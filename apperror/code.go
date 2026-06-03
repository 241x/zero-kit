package apperror

// Code 应用错误码
type Code struct {
	value    int
	name     string
	template string
}

// NewCode 创建错误码
func NewCode(value int, name, template string) Code {
	return Code{
		value:    value,
		name:     name,
		template: template,
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

// Template 返回消息模板
func (c Code) Template() string {
	return c.template
}
