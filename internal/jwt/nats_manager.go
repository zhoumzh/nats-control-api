package jwt

import (
	"context"
	"fmt"
	"time"

	"gitlabee.chehejia.com/gopkg/lsego/pkg/log"

	"nats-control-api/pkg/models"

	"github.com/nats-io/jwt/v2"
	"github.com/nats-io/nkeys"
)

type NATSManager struct {
	operatorKey nkeys.KeyPair
}

func NewNATSManager(operatorKey string) (*NATSManager, error) {
	operatorKeyPair, err := nkeys.FromSeed([]byte(operatorKey))
	if err != nil {
		return nil, fmt.Errorf("invalid operator key: %w", err)
	}

	return &NATSManager{
		operatorKey: operatorKeyPair,
	}, nil
}

func (nm *NATSManager) Close() {
	// No connection to close anymore
}

// GetOperatorKey returns the operator key pair for external use
func (nm *NATSManager) GetOperatorKey() nkeys.KeyPair {
	return nm.operatorKey
}

func (nm *NATSManager) GenerateAccountKeyPair() (publicKey, nkey string, err error) {
	keyPair, err := nkeys.CreateAccount()
	if err != nil {
		return "", "", fmt.Errorf("failed to create account key pair: %w", err)
	}

	publicKey, err = keyPair.PublicKey()
	if err != nil {
		return "", "", fmt.Errorf("failed to get public key: %w", err)
	}

	seed, err := keyPair.Seed()
	if err != nil {
		return "", "", fmt.Errorf("failed to get seed: %w", err)
	}

	return publicKey, string(seed), nil
}

func (nm *NATSManager) GenerateUserKeyPair() (publicKey, nkey string, err error) {
	keyPair, err := nkeys.CreateUser()
	if err != nil {
		return "", "", fmt.Errorf("failed to create user key pair: %w", err)
	}

	publicKey, err = keyPair.PublicKey()
	if err != nil {
		return "", "", fmt.Errorf("failed to get public key: %w", err)
	}

	seed, err := keyPair.Seed()
	if err != nil {
		return "", "", fmt.Errorf("failed to get seed: %w", err)
	}

	return publicKey, string(seed), nil
}

func (nm *NATSManager) CreateAccountJWT(account *models.Account) (string, error) {
	claims := jwt.NewAccountClaims(account.PublicKey)
	claims.Name = account.Name

	// Set expiration
	claims.IssuedAt = time.Now().Unix()
	claims.Expires = time.Now().Add(365 * 24 * time.Hour).Unix() // 1 year

	// 系统账户需要完全清除所有limits和相关字段
	if account.IsSystemAccount {
		log.WithContext(context.Background()).Infof("System account %s: completely clearing all limits and NATS fields", account.Name)

		// 完全重置Limits结构为空值
		claims.Limits = jwt.OperatorLimits{}

		// 清除imports和exports
		claims.Imports = nil
		claims.Exports = nil

		// 清除其他可能的NATS特定字段
		claims.DefaultPermissions = jwt.Permissions{}
		claims.Authorization = jwt.ExternalAuthorization{}

	} else if account.Limits != nil {
		// 非系统账户正常处理所有limits

		// Connection limits - handle -1 as unlimited
		if account.Limits.MaxConnections != 0 {
			claims.Limits.Conn = account.Limits.MaxConnections
		}
		if account.Limits.MaxLeafNodes != 0 {
			claims.Limits.LeafNodeConn = account.Limits.MaxLeafNodes
		}
		if account.Limits.MaxDataValue > 0 {
			claims.Limits.Data = models.ConvertToBytes(account.Limits.MaxDataValue, account.Limits.MaxDataUnit)
		}
		if account.Limits.MaxPayloadValue > 0 {
			claims.Limits.Payload = models.ConvertToBytes(account.Limits.MaxPayloadValue, account.Limits.MaxPayloadUnit)
		}
		if account.Limits.MaxSubscriptions != 0 {
			claims.Limits.Subs = account.Limits.MaxSubscriptions
		}

		// JetStream limits - 只有在明确启用时才设置
		if account.Limits.JetStreamLimits != nil {
			// 检查是否有任何非零的JetStream限制，如果都是0则表示禁用JetStream
			hasJetStreamLimits := account.Limits.JetStreamLimits.DiskStorageValue > 0 ||
				account.Limits.JetStreamLimits.MemoryStorageValue > 0 ||
				account.Limits.JetStreamLimits.Streams > 0 ||
				account.Limits.JetStreamLimits.Consumers > 0

			if hasJetStreamLimits {
				claims.Limits.JetStreamLimits.DiskStorage = models.ConvertToBytes(account.Limits.JetStreamLimits.DiskStorageValue, account.Limits.JetStreamLimits.DiskStorageUnit)
				claims.Limits.JetStreamLimits.MemoryStorage = models.ConvertToBytes(account.Limits.JetStreamLimits.MemoryStorageValue, account.Limits.JetStreamLimits.MemoryStorageUnit)
				claims.Limits.JetStreamLimits.Streams = account.Limits.JetStreamLimits.Streams
				claims.Limits.JetStreamLimits.Consumer = account.Limits.JetStreamLimits.Consumers
			}
			// 如果所有JetStream限制都是0，则不设置任何JetStream limits，这样JWT中就不会包含JetStream配置
		}

		// Handle imports
		if len(account.Limits.Imports) > 0 {
			claims.Imports = make(jwt.Imports, len(account.Limits.Imports))
			for i, imp := range account.Limits.Imports {
				importType := jwt.Stream // default
				if string(imp.Type) == "service" {
					importType = jwt.Service
				}

				claims.Imports[i] = &jwt.Import{
					Name:    imp.Name,
					Subject: jwt.Subject(imp.Subject),
					Account: imp.Account,
					Token:   imp.Token,
					To:      jwt.Subject(imp.To),
					Type:    importType,
				}
			}
		}

		// Handle exports
		if len(account.Limits.Exports) > 0 {
			claims.Exports = make(jwt.Exports, len(account.Limits.Exports))
			for i, exp := range account.Limits.Exports {
				exportType := jwt.Stream // default
				if string(exp.Type) == "service" {
					exportType = jwt.Service
				}

				export := &jwt.Export{
					Name:                 exp.Name,
					Subject:              jwt.Subject(exp.Subject),
					Type:                 exportType,
					TokenReq:             exp.TokenReq,
					AccountTokenPosition: exp.AccountTokenPosition,
				}

				// Set response type for service exports
				if exp.Type == "service" && exp.ResponseType != "" {
					switch exp.ResponseType {
					case "Singleton":
						export.ResponseType = jwt.ResponseTypeSingleton
					case "Stream":
						export.ResponseType = jwt.ResponseTypeStream
					case "Chunked":
						export.ResponseType = jwt.ResponseTypeChunked
					}
				}

				// Add export info if provided
				if exp.Info != nil {
					export.Info = jwt.Info{
						Description: exp.Info.Description,
						InfoURL:     exp.Info.InfoURL,
					}
				}

				claims.Exports[i] = export
			}
		}
	}

	token, err := claims.Encode(nm.operatorKey)
	if err != nil {
		return "", fmt.Errorf("failed to encode account JWT: %w", err)
	}

	return token, nil
}

// DecodeAccountJWT decodes a JWT token and returns the claims
func (nm *NATSManager) DecodeAccountJWT(token string) (interface{}, error) {
	claims, err := jwt.DecodeAccountClaims(token)
	if err != nil {
		return nil, fmt.Errorf("failed to decode account JWT: %w", err)
	}
	
	return claims, nil
}

func (nm *NATSManager) CreateUserJWT(user *models.User, accountNKey string) (string, error) {
	// Parse account's private key for signing
	accountKeyPair, err := nkeys.FromSeed([]byte(accountNKey))
	if err != nil {
		return "", fmt.Errorf("failed to parse account private key: %w", err)
	}

	claims := jwt.NewUserClaims(user.PublicKey)
	claims.Name = user.Name

	// Set expiration
	claims.IssuedAt = time.Now().Unix()
	claims.Expires = time.Now().Add(365 * 24 * time.Hour).Unix() // 1 year

	// Apply user permissions if provided
	if user.Permissions != nil {
		if user.Permissions.Publish != nil {
			claims.Pub.Allow = user.Permissions.Publish.Allow
			claims.Pub.Deny = user.Permissions.Publish.Deny
		}
		if user.Permissions.Subscribe != nil {
			claims.Sub.Allow = user.Permissions.Subscribe.Allow
			claims.Sub.Deny = user.Permissions.Subscribe.Deny
		}
		if user.Permissions.Response != nil {
			claims.Resp = &jwt.ResponsePermission{
				MaxMsgs: user.Permissions.Response.MaxMessages,
				Expires: time.Duration(user.Permissions.Response.Expires) * time.Second,
			}
		}
	}

	// Apply user limits if provided
	if user.Limits != nil {
		if user.Limits.MaxDataValue > 0 {
			claims.Limits.Data = models.ConvertToBytes(user.Limits.MaxDataValue, user.Limits.MaxDataUnit)
		}
		if user.Limits.MaxPayloadValue > 0 {
			claims.Limits.Payload = models.ConvertToBytes(user.Limits.MaxPayloadValue, user.Limits.MaxPayloadUnit)
		}
		if user.Limits.MaxSubscriptions > 0 {
			claims.Limits.Subs = user.Limits.MaxSubscriptions
		}
	}

	token, err := claims.Encode(accountKeyPair)
	if err != nil {
		return "", fmt.Errorf("failed to encode user JWT: %w", err)
	}

	return token, nil
}

// GenerateAccountCreds creates a .creds file content for account
func (nm *NATSManager) GenerateAccountCreds(account *models.Account) (string, error) {
	jwtToken, err := nm.CreateAccountJWT(account)
	if err != nil {
		return "", err
	}

	credsContent := fmt.Sprintf(`-----BEGIN NATS USER JWT-----
%s
------END NATS USER JWT------

************************* IMPORTANT *************************
NKEY Seed printed below can be used to sign and prove identity.
NKEYs are sensitive and should be treated as secrets.

-----BEGIN USER NKEY SEED-----
%s
------END USER NKEY SEED------

*************************************************************
`, jwtToken, account.NKey)

	return credsContent, nil
}

// GenerateUserCreds creates a .creds file content for user
func (nm *NATSManager) GenerateUserCreds(user *models.User, accountNKey string) (string, error) {
	jwtToken, err := nm.CreateUserJWT(user, accountNKey)
	if err != nil {
		return "", err
	}

	credsContent := fmt.Sprintf(`-----BEGIN NATS USER JWT-----
%s
------END NATS USER JWT------

************************* IMPORTANT *************************
NKEY Seed printed below can be used to sign and prove identity.
NKEYs are sensitive and should be treated as secrets.

-----BEGIN USER NKEY SEED-----
%s
------END USER NKEY SEED------

*************************************************************
`, jwtToken, user.NKey)

	return credsContent, nil
}

// Note: JWT推送和删除功能现在应该通过ClusterService中的具体集群连接来实现
// 这样可以支持多集群JWT同步，每个集群使用自己的连接进行JWT管理
