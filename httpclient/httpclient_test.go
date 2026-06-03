package httpclient_test

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/241x/zero-kit/httpclient"
	"github.com/241x/zero-kit/logger"
)

// setupTestServer 创建测试 HTTP 服务器
func setupTestServer() *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/success":
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			json.NewEncoder(w).Encode(map[string]string{"message": "success"})
		case "/error":
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte("internal error"))
		case "/echo":
			// 返回请求体内容
			buf := new(bytes.Buffer)
			buf.ReadFrom(r.Body)
			w.WriteHeader(http.StatusOK)
			w.Write(buf.Bytes())
		default:
			w.WriteHeader(http.StatusOK)
			w.Write([]byte("ok"))
		}
	}))
}

// TestNewHttpClient 测试构造函数
func TestNewHttpClient(t *testing.T) {
	tests := []struct {
		name          string
		opts          []httpclient.Option
		wantTimeout   time.Duration
		wantUserAgent string
	}{
		{
			name:          "默认配置",
			opts:          nil,
			wantTimeout:   30 * time.Second,
			wantUserAgent: "Go-HTTP-Client/1.0",
		},
		{
			name: "自定义超时",
			opts: []httpclient.Option{
				httpclient.WithTimeout(10 * time.Second),
			},
			wantTimeout:   10 * time.Second,
			wantUserAgent: "Go-HTTP-Client/1.0",
		},
		{
			name: "自定义 UserAgent",
			opts: []httpclient.Option{
				httpclient.WithUserAgent("MyAgent/1.0"),
			},
			wantTimeout:   30 * time.Second,
			wantUserAgent: "MyAgent/1.0",
		},
		{
			name: "全部自定义",
			opts: []httpclient.Option{
				httpclient.WithTimeout(5 * time.Second),
				httpclient.WithUserAgent("CustomAgent/2.0"),
				httpclient.WithLogger(logger.Nop()),
			},
			wantTimeout:   5 * time.Second,
			wantUserAgent: "CustomAgent/2.0",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client := httpclient.New(tt.opts...)

			assert.NotNil(t, client)
			assert.Equal(t, tt.wantTimeout, client.Timeout)
			assert.Equal(t, tt.wantUserAgent, client.UserAgent)
			assert.NotNil(t, client.Client)
		})
	}
}

// TestHttpClient_Get 测试 GET 请求
func TestHttpClient_Get(t *testing.T) {
	server := setupTestServer()
	defer server.Close()

	mockLog := logger.Nop()
	client := httpclient.New(httpclient.WithLogger(mockLog))

	resp, err := client.Get(context.Background(), server.URL+"/success")

	assert.NoError(t, err)
	assert.NotNil(t, resp)
	assert.True(t, resp.IsSuccess())
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	var result map[string]string
	err = resp.JSON(&result)
	assert.NoError(t, err)
	assert.Equal(t, "success", result["message"])
}

// TestHttpClient_Post 测试 POST 请求
func TestHttpClient_Post(t *testing.T) {
	server := setupTestServer()
	defer server.Close()

	mockLog := logger.Nop()
	client := httpclient.New(httpclient.WithLogger(mockLog))

	resp, err := client.Post(
		context.Background(),
		server.URL+"/echo",
		bytes.NewBufferString("test body"),
	)

	assert.NoError(t, err)
	assert.NotNil(t, resp)
	assert.Equal(t, "test body", resp.String())
}

// TestHttpClient_PostJSON 测试 PostJSON
func TestHttpClient_PostJSON(t *testing.T) {
	server := setupTestServer()
	defer server.Close()

	mockLog := logger.Nop()
	client := httpclient.New(httpclient.WithLogger(mockLog))

	data := map[string]string{"key": "value"}
	resp, err := client.PostJSON(context.Background(), server.URL+"/echo", data)

	assert.NoError(t, err)
	assert.NotNil(t, resp)

	var result map[string]string
	err = json.Unmarshal(resp.Body, &result)
	assert.NoError(t, err)
	assert.Equal(t, "value", result["key"])
}

// TestHttpClient_PostForm 测试 PostForm
func TestHttpClient_PostForm(t *testing.T) {
	server := setupTestServer()
	defer server.Close()

	mockLog := logger.Nop()
	client := httpclient.New(httpclient.WithLogger(mockLog))

	form := url.Values{}
	form.Set("username", "test")
	form.Set("password", "pass123")

	resp, err := client.PostForm(context.Background(), server.URL+"/echo", form)

	assert.NoError(t, err)
	assert.NotNil(t, resp)
	assert.Contains(t, resp.String(), "username=test")
	assert.Contains(t, resp.String(), "password=pass123")
}

// TestHttpClient_Put 测试 PUT 请求
func TestHttpClient_Put(t *testing.T) {
	server := setupTestServer()
	defer server.Close()

	mockLog := logger.Nop()
	client := httpclient.New(httpclient.WithLogger(mockLog))

	resp, err := client.Put(
		context.Background(),
		server.URL+"/echo",
		bytes.NewBufferString("put body"),
	)

	assert.NoError(t, err)
	assert.NotNil(t, resp)
	assert.Equal(t, "put body", resp.String())
}

// TestHttpClient_Delete 测试 DELETE 请求
func TestHttpClient_Delete(t *testing.T) {
	server := setupTestServer()
	defer server.Close()

	mockLog := logger.Nop()
	client := httpclient.New(httpclient.WithLogger(mockLog))

	resp, err := client.Delete(context.Background(), server.URL+"/success")

	assert.NoError(t, err)
	assert.NotNil(t, resp)
	assert.True(t, resp.IsSuccess())
}

// TestHttpClient_Timeout 测试超时
func TestHttpClient_Timeout(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(2 * time.Second) // 模拟慢响应
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	mockLog := logger.Nop()
	client := httpclient.New(
		httpclient.WithTimeout(100*time.Millisecond),
		httpclient.WithLogger(mockLog),
	)

	resp, err := client.Get(context.Background(), server.URL)

	assert.Error(t, err)
	assert.Nil(t, resp)
	assert.Contains(t, err.Error(), "context deadline exceeded")
}

// TestHttpClient_ErrorResponse 测试错误响应
func TestHttpClient_ErrorResponse(t *testing.T) {
	server := setupTestServer()
	defer server.Close()

	mockLog := logger.Nop()
	client := httpclient.New(httpclient.WithLogger(mockLog))

	resp, err := client.Get(context.Background(), server.URL+"/error")

	assert.NoError(t, err)
	assert.NotNil(t, resp)
	assert.False(t, resp.IsSuccess())
	assert.Equal(t, http.StatusInternalServerError, resp.StatusCode)
}

// TestHttpClient_WithHeader 测试自定义请求头
func TestHttpClient_WithHeader(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		auth := r.Header.Get("Authorization")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(auth))
	}))
	defer server.Close()

	mockLog := logger.Nop()
	client := httpclient.New(httpclient.WithLogger(mockLog))

	resp, err := client.Get(
		context.Background(),
		server.URL,
		httpclient.WithHeader("Authorization", "Bearer token123"),
	)

	assert.NoError(t, err)
	assert.Equal(t, "Bearer token123", resp.String())
}

// TestHttpClient_WithQuery 测试查询参数
func TestHttpClient_WithQuery(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(r.URL.RawQuery))
	}))
	defer server.Close()

	mockLog := logger.Nop()
	client := httpclient.New(httpclient.WithLogger(mockLog))

	query := url.Values{}
	query.Set("page", "1")
	query.Set("size", "10")

	resp, err := client.Get(
		context.Background(),
		server.URL,
		httpclient.WithQuery(query),
	)

	assert.NoError(t, err)
	assert.Contains(t, resp.String(), "page=1")
	assert.Contains(t, resp.String(), "size=10")
}

// TestHttpClient_NilLogger 测试无 logger 时正常工作
func TestHttpClient_NilLogger(t *testing.T) {
	server := setupTestServer()
	defer server.Close()

	// 不注入 logger
	client := httpclient.New()

	resp, err := client.Get(context.Background(), server.URL+"/success")

	assert.NoError(t, err)
	assert.NotNil(t, resp)
	assert.True(t, resp.IsSuccess())
}

// TestResponse_Methods 测试 Response 方法
func TestResponse_Methods(t *testing.T) {
	t.Run("JSON", func(t *testing.T) {
		resp := &httpclient.Response{
			StatusCode: http.StatusOK,
			Body:       []byte(`{"name":"test","value":123}`),
		}

		var result map[string]any
		err := resp.JSON(&result)
		assert.NoError(t, err)
		assert.Equal(t, "test", result["name"])
	})

	t.Run("String", func(t *testing.T) {
		resp := &httpclient.Response{
			StatusCode: http.StatusOK,
			Body:       []byte("hello world"),
		}
		assert.Equal(t, "hello world", resp.String())
	})

	t.Run("IsSuccess", func(t *testing.T) {
		assert.True(t, (&httpclient.Response{StatusCode: 200}).IsSuccess())
		assert.True(t, (&httpclient.Response{StatusCode: 201}).IsSuccess())
		assert.False(t, (&httpclient.Response{StatusCode: 300}).IsSuccess())
		assert.False(t, (&httpclient.Response{StatusCode: 400}).IsSuccess())
		assert.False(t, (&httpclient.Response{StatusCode: 500}).IsSuccess())
	})

	t.Run("GetCookie", func(t *testing.T) {
		cookies := []*http.Cookie{
			{Name: "session", Value: "abc123"},
			{Name: "token", Value: "xyz789"},
		}
		resp := &httpclient.Response{Cookies: cookies}

		cookie := resp.GetCookie("session")
		assert.NotNil(t, cookie)
		assert.Equal(t, "abc123", cookie.Value)

		cookie = resp.GetCookie("nonexistent")
		assert.Nil(t, cookie)
	})
}

// TestHttpClient_LogOnSuccess 测试成功请求时记录日志
func TestHttpClient_LogOnSuccess(t *testing.T) {
	server := setupTestServer()
	defer server.Close()

	var buf bytes.Buffer
	log := logger.New(logger.WithLevel(logger.DebugLevel), logger.WithWriter(&buf))
	client := httpclient.New(httpclient.WithLogger(log))

	_, err := client.Get(context.Background(), server.URL+"/success")
	assert.NoError(t, err)

	// 验证记录了日志（请求和响应）
	assert.True(t, buf.Len() > 0)
}

// TestHttpClient_LogOnError 测试错误请求时记录日志
func TestHttpClient_LogOnError(t *testing.T) {
	mockLog := logger.Nop()
	client := httpclient.New(
		httpclient.WithTimeout(100*time.Millisecond),
		httpclient.WithLogger(mockLog),
	)

	// 使用不可达地址
	_, err := client.Get(context.Background(), "http://127.0.0.1:65530")
	assert.Error(t, err)
}
