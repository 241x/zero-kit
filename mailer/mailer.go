package mailer

import (
	"fmt"
	"os"

	"github.com/241x/zero-kit/logger"
	gomail "github.com/wneessen/go-mail"
)

// 预定义错误
var (
	ErrNotInitialized = fmt.Errorf("mailer is not initialized")
	ErrInvalidConfig  = fmt.Errorf("invalid mail config")

	errNoRecipients = fmt.Errorf("at least one recipient is required")
)

// Config 邮件配置
type Config struct {
	Host        string // SMTP 服务器地址
	Port        int    // SMTP 端口
	Username    string // SMTP 用户名
	Password    string // SMTP 密码
	FromAddress string // 发件人地址
	FromName    string // 发件人名称
	UseTLS      bool   // 是否启用 TLS
}

// Validate 校验邮件配置是否合法
func (c Config) Validate() error {
	if c.Host == "" {
		return fmt.Errorf("%w: host is required", ErrInvalidConfig)
	}
	if c.FromAddress == "" {
		return fmt.Errorf("%w: from_address is required", ErrInvalidConfig)
	}
	if c.Port <= 0 || c.Port > 65535 {
		return fmt.Errorf("%w: port must be between 1 and 65535", ErrInvalidConfig)
	}
	return nil
}

// Attachment 邮件附件
type Attachment struct {
	Name     string // 附件文件名（展示名称）
	FilePath string // 附件文件路径
}

// Message 邮件消息，支持抄送、密送、附件、回复地址等扩展功能
type Message struct {
	To          []string     // 收件人列表
	Cc          []string     // 抄送列表
	Bcc         []string     // 密送列表
	Subject     string       // 邮件主题
	TextBody    string       // 纯文本正文
	HTMLBody    string       // HTML 正文
	ReplyTo     string       // 回复地址
	Attachments []Attachment // 附件列表
}

// Mailer 邮件发送器，封装 go-mail 提供通用邮件发送能力
type Mailer struct {
	cfg    Config
	client *gomail.Client
	logger logger.Logger
}

// NewMailer 创建邮件发送器
// 当 cfg.Host 为空或配置校验失败时返回空 Mailer（发送操作返回 ErrNotInitialized）
func NewMailer(cfg Config, log logger.Logger) *Mailer {
	if log == nil {
		log = logger.Nop()
	}

	if cfg.Host == "" {
		log.Warn("mail host is not configured, email sending is disabled")
		return &Mailer{cfg: cfg, client: nil, logger: log}
	}

	if err := cfg.Validate(); err != nil {
		log.Err(err, "invalid mail config, email sending is disabled")
		return &Mailer{cfg: cfg, client: nil, logger: log}
	}

	opts := []gomail.Option{
		gomail.WithPort(cfg.Port),
	}

	// TLS/SSL 配置
	if cfg.UseTLS {
		opts = append(opts, gomail.WithSSL())
	}

	// SMTP 认证
	if cfg.Username != "" && cfg.Password != "" {
		opts = append(opts,
			gomail.WithSMTPAuth(gomail.SMTPAuthLogin),
			gomail.WithUsername(cfg.Username),
			gomail.WithPassword(cfg.Password),
		)
	}

	// HELO/EHLO 主机名
	opts = append(opts, gomail.WithHELO(getLocalHostname()))

	client, err := gomail.NewClient(cfg.Host, opts...)
	if err != nil {
		log.Err(err, "create mail client failed, email sending is disabled")
		return &Mailer{cfg: cfg, client: nil, logger: log}
	}

	return &Mailer{
		cfg:    cfg,
		client: client,
		logger: log,
	}
}

// Send 发送纯文本邮件
func (m *Mailer) Send(to []string, subject, body string) error {
	return m.sendMessage(to, nil, nil, subject, body, "", "", nil)
}

// SendHTML 发送 HTML 邮件
func (m *Mailer) SendHTML(to []string, subject, htmlBody string) error {
	return m.sendMessage(to, nil, nil, subject, "", htmlBody, "", nil)
}

// SendMessage 发送自定义邮件，支持抄送、密送、附件、回复地址等扩展功能
func (m *Mailer) SendMessage(msg *Message) error {
	if msg == nil {
		return fmt.Errorf("message is nil")
	}
	return m.sendMessage(msg.To, msg.Cc, msg.Bcc, msg.Subject, msg.TextBody, msg.HTMLBody, msg.ReplyTo, msg.Attachments)
}

// Close 关闭邮件客户端，释放连接资源。
// 关闭后 Mailer 不可再使用，再次调用 Send 将返回 ErrNotInitialized。
// 可安全多次调用。
func (m *Mailer) Close() error {
	if m.client == nil {
		return nil
	}
	err := m.client.Close()
	m.client = nil
	return err
}

// sendMessage 统一发送逻辑，所有发送方法最终汇聚于此
func (m *Mailer) sendMessage(to, cc, bcc []string, subject, textBody, htmlBody, replyTo string, attachments []Attachment) error {
	if m.client == nil {
		return ErrNotInitialized
	}
	if len(to) == 0 {
		return errNoRecipients
	}

	msg := gomail.NewMsg()

	// 发件人
	if err := msg.From(m.formatFrom()); err != nil {
		return fmt.Errorf("set from address failed: %w", err)
	}

	// 收件人
	if err := msg.To(to...); err != nil {
		return fmt.Errorf("set to address failed: %w", err)
	}

	// 抄送
	if len(cc) > 0 {
		if err := msg.Cc(cc...); err != nil {
			return fmt.Errorf("set cc address failed: %w", err)
		}
	}

	// 密送
	if len(bcc) > 0 {
		if err := msg.Bcc(bcc...); err != nil {
			return fmt.Errorf("set bcc address failed: %w", err)
		}
	}

	// 回复地址
	if replyTo != "" {
		if err := msg.ReplyTo(replyTo); err != nil {
			return fmt.Errorf("set reply-to address failed: %w", err)
		}
	}

	msg.Subject(subject)

	// 正文：HTML 优先，无 HTML 时使用纯文本
	if htmlBody != "" {
		msg.SetBodyString(gomail.TypeTextHTML, htmlBody)
	} else {
		msg.SetBodyString(gomail.TypeTextPlain, textBody)
	}

	// 附件
	for _, att := range attachments {
		msg.AttachFile(att.FilePath, gomail.WithFileName(att.Name))
	}

	return m.client.DialAndSend(msg)
}

// formatFrom 格式化发件人地址
func (m *Mailer) formatFrom() string {
	if m.cfg.FromName != "" {
		return fmt.Sprintf("%s <%s>", m.cfg.FromName, m.cfg.FromAddress)
	}
	return m.cfg.FromAddress
}

// getLocalHostname 获取本机主机名用于 HELO
func getLocalHostname() string {
	hostname, err := os.Hostname()
	if err != nil {
		return "localhost"
	}
	return hostname
}
