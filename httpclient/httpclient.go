package httpclient

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"time"

	"github.com/241x/zero-kit/logger"
)

// Method 请求方法类型定义
type Method string

const (
	GET    Method = "GET"
	POST   Method = "POST"
	PUT    Method = "PUT"
	DELETE Method = "DELETE"
	PATCH  Method = "PATCH"
)

// Option 配置项
type Option func(*HttpClient)

// WithTimeout 设置超时时间
func WithTimeout(timeout time.Duration) Option {
	return func(c *HttpClient) {
		c.Timeout = timeout
	}
}

// WithUserAgent 设置User-Agent
func WithUserAgent(userAgent string) Option {
	return func(c *HttpClient) {
		c.UserAgent = userAgent
	}
}

// WithLogger 设置日志实例
func WithLogger(l logger.Logger) Option {
	return func(c *HttpClient) {
		c.logger = l
	}
}

// RequestOption 请求配置项
type RequestOption func(*http.Request)

// WithHeader 设置请求头
func WithHeader(key, value string) RequestOption {
	return func(r *http.Request) {
		r.Header.Set(key, value)
	}
}

// WithCookies 添加cookies
func WithCookies(cookies []*http.Cookie) RequestOption {
	return func(r *http.Request) {
		for _, cookie := range cookies {
			r.AddCookie(cookie)
		}
	}
}

// WithQuery 设置请求参数
func WithQuery(query url.Values) RequestOption {
	return func(req *http.Request) {
		if len(query) > 0 {
			req.URL.RawQuery = query.Encode()
		}
	}
}

// Response 响应结构体
type Response struct {
	StatusCode int
	Header     http.Header
	Body       []byte
	Request    *http.Request
	Cookies    []*http.Cookie
}

// JSON 解析响应为JSON
func (r *Response) JSON(v any) error {
	return json.Unmarshal(r.Body, v)
}

// String 解析响应为字符串
func (r *Response) String() string {
	return string(r.Body)
}

// IsSuccess 判断响应是否成功
func (r *Response) IsSuccess() bool {
	return r.StatusCode >= 200 && r.StatusCode < 300
}

// GetCookie 获取指定名称的cookie
func (r *Response) GetCookie(name string) *http.Cookie {
	for _, cookie := range r.Cookies {
		if cookie.Name == name {
			return cookie
		}
	}

	return nil
}

// HttpClient http客户端
type HttpClient struct {
	Client    *http.Client
	Timeout   time.Duration
	UserAgent string
	logger    logger.Logger
}

// New 创建一个http客户端
func New(opts ...Option) *HttpClient {
	c := &HttpClient{
		Timeout:   30 * time.Second,
		UserAgent: "Go-HTTP-Client/1.0",
	}

	for _, opt := range opts {
		opt(c)
	}

	c.Client = &http.Client{Timeout: c.Timeout}

	return c
}

// Do 发送请求
func (c *HttpClient) Do(ctx context.Context, method Method, url string, body io.Reader, opts ...RequestOption) (*Response, error) {
	req, err := http.NewRequestWithContext(ctx, string(method), url, body)
	if err != nil {
		c.log(logger.ErrorLevel, "创建请求失败", "error", err, "method", method, "url", url)
		return nil, fmt.Errorf("创建请求失败：%w", err)
	}

	c.applyOptions(req, opts...)

	// 记录发送请求日志
	c.log(logger.DebugLevel,
		"发送HTTP请求",
		"method", req.Method,
		"url", req.URL.String(),
		"headers", req.Header)

	resp, err := c.Client.Do(req)
	if err != nil {
		c.log(logger.DebugLevel, "HTTP请求失败", "error", err, "method", method, "url", url)
		return nil, fmt.Errorf("请求失败：%w", err)
	}

	respBody, err := io.ReadAll(resp.Body)
	resp.Body.Close()
	if err != nil {
		c.log(logger.ErrorLevel, "读取响应失败", "error", err, "method", method, "url", url)
		return nil, fmt.Errorf("读取响应失败：%w", err)
	}

	// 记录响应日志
	c.log(logger.DebugLevel,
		"接收HTTP响应",
		"status_code", resp.StatusCode,
		"method", req.Method,
		"url", req.URL.String(),
		"headers", resp.Header,
		"body", string(respBody),
	)

	return &Response{
		StatusCode: resp.StatusCode,
		Header:     resp.Header,
		Body:       respBody,
		Request:    req,
		Cookies:    resp.Cookies(),
	}, nil
}

// applyOptions 应用请求配置项
func (c *HttpClient) applyOptions(req *http.Request, opts ...RequestOption) {
	// 设置默认的User-Agent
	req.Header.Set("User-Agent", c.UserAgent)

	for _, opt := range opts {
		opt(req)
	}
}

// Get 发送GET请求
func (c *HttpClient) Get(ctx context.Context, url string, opts ...RequestOption) (*Response, error) {
	return c.Do(ctx, GET, url, nil, opts...)
}

// Post 发送POST请求
func (c *HttpClient) Post(ctx context.Context, url string, body io.Reader, opts ...RequestOption) (*Response, error) {
	return c.Do(ctx, POST, url, body, opts...)
}

// PostJSON 发送POST请求，请求体为JSON
func (c *HttpClient) PostJSON(ctx context.Context, url string, data any, opts ...RequestOption) (*Response, error) {
	jsonData, err := json.Marshal(data)
	if err != nil {
		c.log(logger.ErrorLevel, "JSON序列化失败", "error", err, "url", url)
		return nil, fmt.Errorf("JSON序列化失败：%w", err)
	}

	finalOpts := append([]RequestOption{WithHeader("Content-Type", "application/json")}, opts...)
	return c.Do(ctx, POST, url, bytes.NewBuffer(jsonData), finalOpts...)
}

// PostForm 发送POST请求，请求体为表单
func (c *HttpClient) PostForm(ctx context.Context, url string, data url.Values, opts ...RequestOption) (*Response, error) {
	finalOpts := append([]RequestOption{WithHeader("Content-Type", "application/x-www-form-urlencoded")}, opts...)
	return c.Do(ctx, POST, url, bytes.NewBufferString(data.Encode()), finalOpts...)
}

// PostFile 发送POST请求，请求体为文件
func (c *HttpClient) PostFile(ctx context.Context, url string, fileName, filePath string, extraFields map[string]string, opts ...RequestOption) (*Response, error) {
	file, err := os.Open(filePath)
	if err != nil {
		c.log(logger.ErrorLevel, "打开文件失败", "error", err, "file_path", filePath, "url", url)
		return nil, fmt.Errorf("打开文件失败：%w", err)
	}
	defer file.Close()

	body := &bytes.Buffer{}
	writer := multipart.NewWriter(body)

	part, err := writer.CreateFormFile(fileName, filepath.Base(filePath))
	if err != nil {
		c.log(logger.ErrorLevel, "创建表单文件失败", "error", err, "file_name", fileName, "url", url)
		return nil, fmt.Errorf("创建表单文件失败：%w", err)
	}

	_, err = io.Copy(part, file)
	if err != nil {
		c.log(logger.ErrorLevel, "写入表单文件失败", "error", err, "file_name", fileName, "url", url)
		return nil, fmt.Errorf("写入表单文件失败：%w", err)
	}

	for key, value := range extraFields {
		_ = writer.WriteField(key, value)
	}

	err = writer.Close()
	if err != nil {
		c.log(logger.ErrorLevel, "关闭表单写入失败", "error", err, "url", url)
		return nil, fmt.Errorf("关闭表单写入失败：%w", err)
	}

	finalOpts := append([]RequestOption{WithHeader("Content-Type", writer.FormDataContentType())}, opts...)
	return c.Do(ctx, POST, url, body, finalOpts...)
}

// Put 发送PUT请求
func (c *HttpClient) Put(ctx context.Context, url string, body io.Reader, opts ...RequestOption) (*Response, error) {
	return c.Do(ctx, PUT, url, body, opts...)
}

// Delete 发送DELETE请求
func (c *HttpClient) Delete(ctx context.Context, url string, opts ...RequestOption) (*Response, error) {
	return c.Do(ctx, DELETE, url, nil, opts...)
}

// log 记录日志
func (c *HttpClient) log(level logger.Level, msg string, fields ...any) {
	if c.logger == nil {
		return
	}
	c.logger.Log(level, msg, fields...)
}
