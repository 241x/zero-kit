package baserepo

import (
	"context"
	"errors"

	"gorm.io/gorm"
)

// Filter 筛选器接口
type Filter interface {
	Apply(db *gorm.DB) *gorm.DB
}

// Paginator 分页器接口
type Paginator interface {
	Offset() int
	Limit() int
}

// Pagination 分页参数
type Pagination struct {
	page  int
	limit int
}

// NewPagination 创建分页参数
func NewPagination(page, limit int) *Pagination {
	if page <= 0 {
		page = 1
	}
	if limit < 0 {
		limit = 0
	}
	return &Pagination{page: page, limit: limit}
}

// Offset 计算偏移量
func (p *Pagination) Offset() int {
	if p == nil || p.limit <= 0 {
		return 0
	}

	return (p.page - 1) * p.limit
}

// Limit 获取每页数量
func (p *Pagination) Limit() int {
	if p == nil {
		return 0
	}
	return p.limit
}

// Order 排序参数
type Order struct {
	Field string
	Sort  string
}

// Orders 排序参数切片
type Orders []Order

// TxConfigurer 事务配置接口
type TxConfigurer interface {
	SetTx(tx *gorm.DB)
	GetTx() *gorm.DB
}

// TxConfig 事务配置
type TxConfig struct {
	tx *gorm.DB
}

// SetTx 设置事务
func (c *TxConfig) SetTx(tx *gorm.DB) {
	c.tx = tx
}

// GetTx 获取事务
func (c *TxConfig) GetTx() *gorm.DB {
	return c.tx
}

// WithTx 添加事务
func WithTx[T TxConfigurer](tx *gorm.DB) func(T) {
	return func(c T) {
		c.SetTx(tx)
	}
}

// QueryOption 查询选项
type QueryOption func(*QueryConfig)

// ScopeFunc 筛选作用域方法
type ScopeFunc func(*gorm.DB) *gorm.DB

// QueryConfig 查询配置
type QueryConfig struct {
	TxConfig
	preloads []string
	scopes   []ScopeFunc
}

// WithPreloads 添加预加载
func WithPreloads(preloads ...string) QueryOption {
	return func(c *QueryConfig) {
		c.preloads = preloads
	}
}

// WithScopes 添加筛选
func WithScopes(scopes ...ScopeFunc) QueryOption {
	return func(c *QueryConfig) {
		c.scopes = scopes
	}
}

// CreateOption 创建选项
type CreateOption func(*CreateConfig)

// CreateConfig 创建配置
type CreateConfig struct {
	TxConfig
	batchSize int
}

// WithCreateBatchSize 设置批量创建大小
func WithCreateBatchSize(batchSize int) CreateOption {
	return func(c *CreateConfig) {
		c.batchSize = batchSize
	}
}

// UpdateOption 更新选项
type UpdateOption func(*UpdateConfig)

// UpdateConfig 更新配置
type UpdateConfig struct {
	TxConfig
}

// DeleteOption 删除选项
type DeleteOption func(*DeleteConfig)

// DeleteConfig 删除配置
type DeleteConfig struct {
	TxConfig
	ForceDelete bool
}

// WithForceDelete 强制删除选项
func WithForceDelete(force bool) DeleteOption {
	return func(c *DeleteConfig) {
		c.ForceDelete = force
	}
}

// BaseRepository 基础DAO层
type BaseRepository[T any] struct {
	Db *gorm.DB
}

// NewBaseRepository 创建基础DAO层
func NewBaseRepository[T any](db *gorm.DB) *BaseRepository[T] {
	return &BaseRepository[T]{
		Db: db,
	}
}

// Create 创建记录
func (r *BaseRepository[T]) Create(ctx context.Context, item *T, opts ...CreateOption) error {
	cfg := &CreateConfig{}

	for _, opt := range opts {
		opt(cfg)
	}

	return r.getDb(cfg).WithContext(ctx).Create(item).Error
}

// CreateBatch 批量创建记录
func (r *BaseRepository[T]) CreateBatch(ctx context.Context, item []*T, opts ...CreateOption) error {
	if len(item) == 0 {
		return nil
	}

	cfg := &CreateConfig{}

	for _, opt := range opts {
		opt(cfg)
	}

	size := 100
	if cfg.batchSize > 0 {
		size = cfg.batchSize
	}

	return r.getDb(cfg).WithContext(ctx).CreateInBatches(item, size).Error
}

// Updates 更新记录
func (r *BaseRepository[T]) Updates(ctx context.Context, item *T, updateData map[string]any, opts ...UpdateOption) error {
	cfg := &UpdateConfig{}

	for _, opt := range opts {
		opt(cfg)
	}

	return r.getDb(cfg).WithContext(ctx).Model(item).Updates(updateData).Error
}

// Delete 删除记录
func (r *BaseRepository[T]) Delete(ctx context.Context, id any, opts ...DeleteOption) error {
	cfg := &DeleteConfig{}

	for _, opt := range opts {
		opt(cfg)
	}

	query := r.getDb(cfg).WithContext(ctx)

	if filter, ok := id.(Filter); ok {
		if cfg.ForceDelete {
			return filter.Apply(query).Unscoped().Delete(new(T)).Error
		}
		return filter.Apply(query).Delete(new(T)).Error
	}

	if cfg.ForceDelete {
		return query.Unscoped().Delete(new(T), id).Error
	}

	return query.Delete(new(T), id).Error
}

// FindOne 根据主键查询记录
func (r *BaseRepository[T]) FindOne(ctx context.Context, id any, opts ...QueryOption) (*T, error) {
	item := new(T)
	query := r.buildQuery(ctx, opts...)
	var err error

	if filter, ok := id.(Filter); ok {
		err = filter.Apply(query).First(item).Error
	} else {
		err = query.First(item, id).Error
	}

	if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, err
	}

	return item, nil
}

// FindAll 查询所有记录
func (r *BaseRepository[T]) FindAll(ctx context.Context, filter Filter, pagination Paginator, orders Orders, opts ...QueryOption) ([]*T, error) {
	list := make([]*T, 0)
	query := r.buildQuery(ctx, opts...).Model(new(T))

	if filter != nil {
		query = filter.Apply(query)
	}

	if pagination != nil {
		query = query.Limit(pagination.Limit()).Offset(pagination.Offset())
	}

	for _, order := range orders {
		query = query.Order(order.Field + " " + order.Sort)
	}

	if err := query.Find(&list).Error; err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, err
	}

	return list, nil
}

// Count 统计记录数量
func (r *BaseRepository[T]) Count(ctx context.Context, filter Filter, opts ...QueryOption) (int64, error) {
	var count int64
	query := r.buildQuery(ctx, opts...).Model(new(T))

	if filter != nil {
		query = filter.Apply(query)
	}

	if err := query.Count(&count).Error; err != nil {
		return 0, err
	}

	return count, nil
}

// buildQuery 构建查询
func (r *BaseRepository[T]) buildQuery(ctx context.Context, opts ...QueryOption) *gorm.DB {
	cfg := &QueryConfig{scopes: []ScopeFunc{}}

	for _, opt := range opts {
		opt(cfg)
	}

	query := r.getDb(cfg).WithContext(ctx)

	for _, preload := range cfg.preloads {
		query = query.Preload(preload)
	}

	for _, scopeFunc := range cfg.scopes {
		if scopeFunc != nil {
			query = query.Scopes(scopeFunc)
		}
	}

	return query
}

// getDb 获取数据库连接
func (r *BaseRepository[T]) getDb(cfg TxConfigurer) *gorm.DB {
	if cfg != nil {
		if tx := cfg.GetTx(); tx != nil {
			return tx
		}
	}

	return r.Db
}
