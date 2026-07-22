package bind

import (
	"fmt"

	"github.com/go-playground/locales/zh"
	ut "github.com/go-playground/universal-translator"
	"github.com/go-playground/validator/v10"
	translations "github.com/go-playground/validator/v10/translations/zh"
)

// NewValidate 创建 validator 实例。
func NewValidate() *validator.Validate {
	return validator.New()
}

// NewTrans 创建中文翻译器，注册默认翻译规则。
func NewTrans(v *validator.Validate) (ut.Translator, error) {
	zt := zh.New()
	uni := ut.New(zt, zt)
	trans, found := uni.GetTranslator("zh")
	if !found {
		return nil, fmt.Errorf("translator zh not found")
	}
	if err := translations.RegisterDefaultTranslations(v, trans); err != nil {
		return nil, fmt.Errorf("register translations failed: %w", err)
	}
	return trans, nil
}

// MustNewTrans 创建中文翻译器，注册默认翻译规则，如果失败则 panic。
func MustNewTrans(v *validator.Validate) ut.Translator {
	trans, err := NewTrans(v)
	if err != nil {
		panic(err)
	}
	return trans
}
