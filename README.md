# zero-kit

一套开箱即用的 Go 后端基础能力工具包，为 Web 应用和微服务提供数据库、缓存、日志、队列、分布式锁等通用能力的标准化封装，帮助项目快速搭建基础设施层。

## 特性

- **泛型 Repository** — 基于 GORM 泛型封装 CRUD，支持分页、排序、预加载、事务注入，减少重复 DAO 代码
- **分布式锁** — Redis 实现，Lua 脚本保证令牌安全释放，内置看门狗自动续期
- **任务队列** — 基于 Redis 的异步队列，支持延迟任务、多种重试策略、死信队列和工作线程池
- **统一日志** — 基于 zerolog，支持控制台 / 文件轮转 / MongoDB 多写入器，可通过 context 传递
- **统一错误** — 自定义错误码体系，兼容 `errors.Is` / `errors.Unwrap` 标准库协议
- **HTTP 客户端** — 链式配置，支持 JSON / Form / 文件上传，可插拔日志记录
- **邮件发送** — 基于 go-mail 封装，支持纯文本 / HTML / 抄送密送 / 附件，未配置 SMTP 时静默降级
- **数据库迁移** — SQL 脚本断点续迁，进度持久化，适合大脚本增量执行

## 安装

```bash
go get github.com/241x/zero-kit
```

> 要求 Go >= 1.26

## 模块说明

| 模块 | 说明 |
|---|---|
| `apperror` | 应用错误码体系。通过 `Code` 定义错误码（编号、名称、消息模板），用 `New` / `Wrap` 创建错误实例，支持 `WithCause` 包装原始错误、`WithMsg` 覆盖消息，完整兼容 `errors.Is`、`errors.Unwrap` 和 `%+v` 调试格式 |
| `baserepo` | 基于 GORM 的泛型 Repository。提供 `Create`、`CreateBatch`、`Updates`、`Delete`、`FindOne`、`FindAll`、`Count` 等标准方法；通过 `Filter` 接口实现动态筛选，`Pagination` 处理分页，`Orders` 处理排序；所有操作均支持通过 `TxConfig` 注入事务，通过 `QueryOption` 添加预加载和 Scope |
| `helper` | 常用工具函数集。包括：并发安全随机数（`math/rand/v2`）、随机字符串生成、bcrypt 密码哈希与校验、SQL LIKE 模糊查询安全转义、字符串 MD5 计算 |
| `httpclient` | HTTP 客户端封装。通过 `Option` 模式配置超时、UserAgent、Logger；提供 `Get`、`Post`、`PostJSON`、`PostForm`、`PostFile`、`Put`、`Delete` 等快捷方法；响应对象支持 `JSON()` 解析和 `IsSuccess()` 判断；请求级别可通过 `RequestOption` 设置 Header、Cookie、Query 参数 |
| `locker` | 分布式锁接口与 Redis 实现。`Locker` / `Lock` 双层接口设计；通过 Lua 脚本保证「令牌匹配才释放」的安全性；支持 `WithWatchDog()` 开启看门狗协程，按 TTL/3 间隔自动续期，防止业务未完成锁已过期 |
| `logger` | 统一日志接口及 zerolog 实现。定义 `Debug` / `Info` / `Warn` / `Error` / `Err` / `Log` 标准方法；支持控制台彩色输出、lumberjack 文件轮转、MongoDB 写入器，可通过 `io.MultiWriter` 组合多输出目标；提供 `WithContext` / `Ctx` 实现日志实例在 context 中的传递 |
| `mailer` | 邮件发送封装（基于 go-mail）。`Config` 配置 SMTP 并通过 `Validate()` 校验；提供 `Send`（纯文本）、`SendHTML`（HTML）、`SendMessage`（抄送 / 密送 / 附件 / 回复地址）三级发送接口；未配置 Host 时静默降级，发送操作返回 `ErrNotInitialized`；`Close()` 安全释放连接资源 |
| `migrate` | SQL 数据库迁移器。以 `-- [CHECK POINT] --` 标记分段执行 SQL 脚本；通过 `ProgressStore` 接口持久化执行进度（默认文件存储），支持断点续迁；`ScriptReader` 接口可替换为自定义脚本来源 |
| `mongodb` | MongoDB 连接管理。通过 `Config` 配置 URI 和数据库名，`Enabled` 字段控制是否启用连接；返回 `Conn` 结构体包含 `Client` 和 `DB` 实例，连接时自动 Ping 验证 |
| `mysql` | MySQL 连接管理（GORM）。支持表前缀和单数表名配置，默认禁用自动迁移外键约束；内置自定义 SQL Logger，自动从 context 提取 traceId、记录慢查询（>1s 告警）、SQL 执行耗时和行数 |
| `queue` | 基于 Redis 的任务队列系统。`Queue` 接口定义入队/出队/确认/重试/统计等完整操作；支持延迟任务（`EnqueueWithDelay`）、三种重试策略（固定/指数退避/随机）、死信队列；`WorkerPool` 工作线程池支持并发消费；`QueueManager` 统一管理多个队列和线程池的生命周期 |
| `redis` | Redis 客户端初始化。通过 `Config` 配置地址、端口、密码和数据库编号，返回 `go-redis` 客户端实例 |

## 快速示例

### 日志

```go
package main

import (
    "github.com/241x/zero-kit/logger"
)

func main() {
    // 创建日志实例：控制台 + 文件轮转
    log := logger.New(
        logger.WithLevel(logger.DebugLevel),
        logger.WithConsole(),
        logger.WithFileWithConfig(logger.FileConfig{
            Path:       "runtime/logs",
            Filename:   "app.log",
            MaxSize:    100,  // MB
            MaxAge:     30,   // 天
            MaxBackups: 3,
            Compress:   true,
        }),
    )

    log.Info("服务启动", "port", 8080)
    log.Debug("调试信息", "module", "auth")

    // 通过 context 传递日志实例
    ctx := log.WithContext(context.Background())
    logger.Ctx(ctx).Info("从 context 获取日志")
}
```

### 统一错误处理

```go
package main

import (
    "errors"
    "fmt"

    "github.com/241x/zero-kit/apperror"
)

// 定义业务错误码
var (
    ErrUserNotFound = apperror.NewCode(1001, "UserNotFound", "用户不存在")
    ErrUnauthorized = apperror.NewCode(2001, "Unauthorized", "未授权访问")
)

func main() {
    // 创建错误
    err := apperror.New(ErrUserNotFound, apperror.WithMsg("ID=123 的用户不存在"))

    // 按错误码判断
    if errors.Is(err, apperror.New(ErrUserNotFound)) {
        fmt.Println("用户未找到")
    }

    // 包装底层错误
    dbErr := fmt.Errorf("connection timeout")
    wrapped := apperror.Wrap(ErrUnauthorized, dbErr)
    fmt.Printf("%+v\n", wrapped) // 打印完整调试信息
}
```

### MySQL + BaseRepo

```go
package main

import (
    "context"

    "github.com/241x/zero-kit/baserepo"
    "github.com/241x/zero-kit/mysql"
)

type User struct {
    ID   uint   `gorm:"primaryKey"`
    Name string `gorm:"size:64"`
}

func main() {
    ctx := context.Background()

    // 初始化数据库连接
    db, err := mysql.NewDB(mysql.Config{
        Dsn:    "user:pass@tcp(127.0.0.1:3306)/mydb?parseTime=true",
        Prefix: "t_",
    }, nil)
    if err != nil {
        panic(err)
    }

    // 创建泛型 Repository
    repo := baserepo.NewBaseRepository[User](db)

    // 创建记录
    repo.Create(ctx, &User{Name: "Alice"})

    // 分页查询
    list, _ := repo.FindAll(ctx, nil, baserepo.NewPagination(1, 20), baserepo.Orders{
        {Field: "id", Sort: "desc"},
    })

    // 事务操作
    tx := db.Begin()
    repo.Create(ctx, &User{Name: "Bob"}, baserepo.WithCreateBatchSize(0))
    tx.Commit()
}
```

### 分布式锁

```go
package main

import (
    "context"
    "time"

    "github.com/241x/zero-kit/locker"
    zkitRedis "github.com/241x/zero-kit/redis"
)

func main() {
    ctx := context.Background()
    client := zkitRedis.New(zkitRedis.Config{Host: "127.0.0.1", Port: 6379})

    lk := locker.NewRedisLocker(client)

    // 获取锁，启用看门狗自动续期
    lock, err := lk.Lock(ctx, "order:123",
        locker.WithTTL(10*time.Second),
        locker.WithWatchDog(),
    )
    if err != nil {
        panic(err)
    }
    defer lock.Unlock(ctx)

    // 安全地执行需要互斥的业务逻辑
    processOrder(lock.Key())
}

func processOrder(key string) {}
```

### 任务队列

```go
package main

import (
    "context"
    "fmt"

    "github.com/241x/zero-kit/queue"
    zkitRedis "github.com/241x/zero-kit/redis"
)

func main() {
    ctx := context.Background()
    client := zkitRedis.New(zkitRedis.Config{Host: "127.0.0.1", Port: 6379})

    mgr := queue.NewQueueManager(client)

    // 注册工作线程池，配置并发数和重试策略
    config := queue.DefaultConfig().
        WithMaxConcurrency(5).
        WithMaxRetries(3).
        WithRetryDelay(queue.RetryDelayExponential).
        WithDeadLetter(true, 3)

    handler := queue.HandlerFunc(func(ctx context.Context, task *queue.Task) error {
        fmt.Printf("处理任务: %s, 类型: %s\n", task.ID, task.Type)
        return nil
    })

    pool, _ := mgr.RegisterWorkerPool("email", handler, config)
    _ = pool

    // 入队任务
    task := queue.NewTask("email", "send_welcome", []byte(`{"user_id": 1}`))
    mgr.EnqueueTask(ctx, "email", task)

    // 入队延迟任务
    delayTask := queue.NewTask("email", "send_reminder", []byte(`{"user_id": 2}`))
    mgr.EnqueueTaskWithDelay(ctx, "email", delayTask, 30*time.Minute)

    // 启动所有工作线程池
    mgr.StartAllWorkerPools(ctx)
}
```

### HTTP 客户端

```go
package main

import (
    "context"
    "fmt"
    "net/url"
    "time"

    "github.com/241x/zero-kit/httpclient"
    "github.com/241x/zero-kit/logger"
)

func main() {
    ctx := context.Background()

    // 创建客户端，配置超时和日志
    log := logger.New(logger.WithLevel(logger.DebugLevel), logger.WithConsole())
    client := httpclient.New(
        httpclient.WithTimeout(10*time.Second),
        httpclient.WithUserAgent("MyApp/1.0"),
        httpclient.WithLogger(log),
    )

    // GET 请求
    resp, err := client.Get(ctx, "https://api.example.com/users",
        httpclient.WithQuery(url.Values{"page": {"1"}}),
        httpclient.WithHeader("Authorization", "Bearer token"),
    )
    if err != nil {
        panic(err)
    }

    if resp.IsSuccess() {
        var users []map[string]any
        resp.JSON(&users)
        fmt.Println(users)
    }

    // POST JSON 请求
    resp, _ = client.PostJSON(ctx, "https://api.example.com/users",
        map[string]string{"name": "Alice"},
    )
}
```

### 邮件发送

```go
package main

import (
    "github.com/241x/zero-kit/mailer"
)

func main() {
    // 创建邮件发送器（Host 为空时静默降级，不影响程序启动）
    m := mailer.NewMailer(mailer.Config{
        Host:        "smtp.example.com",
        Port:        465,
        Username:    "user@example.com",
        Password:    "password",
        FromAddress: "user@example.com",
        FromName:    "MyApp",
        UseTLS:      true,
    }, nil)
    defer m.Close()

    // 发送纯文本邮件
    _ = m.Send([]string{"to@example.com"}, "主题", "正文内容")

    // 发送 HTML 邮件
    _ = m.SendHTML([]string{"to@example.com"}, "欢迎", "<h1>欢迎注册</h1>")

    // 发送带抄送、密送、附件的邮件
    _ = m.SendMessage(&mailer.Message{
        To:       []string{"to@example.com"},
        Cc:       []string{"cc@example.com"},
        Bcc:      []string{"bcc@example.com"},
        Subject:  "报表",
        HTMLBody: "<p>请查看附件</p>",
        ReplyTo:  "reply@example.com",
        Attachments: []mailer.Attachment{
            {Name: "report.xlsx", FilePath: "/tmp/report.xlsx"},
        },
    })
}
```

### 数据库迁移

```go
package main

import (
    "context"

    "github.com/241x/zero-kit/logger"
    "github.com/241x/zero-kit/migrate"
    "github.com/241x/zero-kit/mysql"
)

func main() {
    db, _ := mysql.NewDB(mysql.Config{Dsn: "user:pass@tcp(127.0.0.1:3306)/mydb"}, nil)
    log := logger.New(logger.WithLevel(logger.InfoLevel), logger.WithConsole())

    // 创建迁移器，指定 SQL 脚本路径
    m := migrate.NewMigrator(db, "migrations/init.sql",
        migrate.WithLogger(log),
    )

    // 执行迁移（支持断点续迁）
    if err := m.Migrate(context.Background()); err != nil {
        panic(err)
    }
}
```

## 依赖

| 库 | 用途 |
|---|---|
| [gorm](https://gorm.io) | MySQL ORM |
| [go-redis](https://github.com/redis/go-redis) | Redis 客户端 |
| [mongo-driver](https://go.mongodb.org/mongo-driver) | MongoDB 驱动 |
| [zerolog](https://github.com/rs/zerolog) | 高性能结构化日志 |
| [lumberjack](https://gopkg.in/natefinch/lumberjack.v2) | 日志文件轮转 |
| [uuid](https://github.com/google/uuid) | UUID 生成 |
| [crypto](https://golang.org/x/crypto) | bcrypt 密码哈希 |
| [go-mail](https://github.com/wneessen/go-mail) | SMTP 邮件发送 |

## 许可证

MIT
