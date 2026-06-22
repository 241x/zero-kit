// Package bind 提供 HTTP 请求参数绑定与校验能力，集成 validator 校验与中文翻译。
// 适用于 gin 框架，将 ShouldBindJSON 与 struct validate 合并为一步调用。
package bind

import (
	"errors"
	"reflect"

	"github.com/gin-gonic/gin"
	ut "github.com/go-playground/universal-translator"
	"github.com/go-playground/validator/v10"

	"github.com/241x/zero-kit/apperror"
)

// Binder 请求参数绑定器，组合 JSON 绑定与 struct 校验。
type Binder struct {
	validate *validator.Validate
	trans    ut.Translator
	errCode  apperror.Code
}

// New 创建绑定器，需注入 validator、翻译器及业务错误码。
func New(validate *validator.Validate, trans ut.Translator, errCode apperror.Code) *Binder {
	return &Binder{
		validate: validate,
		trans:    trans,
		errCode:  errCode,
	}
}

// ShouldBindJSON 将请求体 JSON 绑定到 data 并执行 struct 校验，返回首条翻译后的校验错误。
func (b *Binder) ShouldBindJSON(ctx *gin.Context, data any) error {
	if err := ctx.ShouldBindJSON(data); err != nil {
		return apperror.New(b.errCode, apperror.WithCause(err))
	}

	if err := b.validate.Struct(data); err != nil {
		if validationErrors, ok := errors.AsType[validator.ValidationErrors](err); ok {
			return apperror.New(b.errCode, apperror.WithMsg(validationErrors[0].Translate(b.trans)))
		}
		return apperror.Wrap(b.errCode, err, apperror.WithMsg("参数验证失败"))
	}

	return nil
}

// ShouldBindJSONArray 将请求体 JSON 数组绑定到 data 并逐个校验元素。
// data 必须是指向切片的指针，如 &[]SomeDto{} 或 &[]*SomeDto{}。
func (b *Binder) ShouldBindJSONArray(ctx *gin.Context, data any) error {
	if err := ctx.ShouldBindJSON(data); err != nil {
		return apperror.New(b.errCode, apperror.WithCause(err))
	}

	v := reflect.ValueOf(data)
	if v.Kind() == reflect.Ptr {
		v = v.Elem()
	}
	if v.Kind() != reflect.Slice {
		return nil
	}

	for i := 0; i < v.Len(); i++ {
		item := v.Index(i)
		if item.Kind() == reflect.Ptr && !item.IsNil() {
			item = item.Elem()
		}
		if err := b.validate.Struct(item.Interface()); err != nil {
			if validationErrors, ok := errors.AsType[validator.ValidationErrors](err); ok {
				return apperror.New(b.errCode, apperror.WithMsg(validationErrors[0].Translate(b.trans)))
			}
			return apperror.Wrap(b.errCode, err, apperror.WithMsg("参数验证失败"))
		}
	}

	return nil
}
