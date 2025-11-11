package service

import (
	"context"
	"fmt"
	"os"
	"time"

	"nats-control-api/pkg/models"

	"github.com/google/uuid"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/nats-io/nkeys"
	"gitlabee.chehejia.com/gopkg/lsego/pkg/log"
)

type ClusterServer struct {
}

type JetStreamContext = nats.JetStreamContext

func NewClusterServer() *ClusterServer {
	return &ClusterServer{}
}

func (cs *ClusterServer) TestClusterNATSConnection(cluster *models.Cluster, systemAccount *models.Account, adminUser *models.User) error {
	if cluster.Host == "" {
		return fmt.Errorf("cluster host is required")
	}

	if cluster.SystemAccountID == "" {
		return fmt.Errorf("cluster has no system account configured, connectivity check skipped")
	}

	if systemAccount == nil {
		return fmt.Errorf("system account not found")
	}

	if adminUser == nil {
		return fmt.Errorf("no admin user found in system account '%s', cluster connectivity check skipped", systemAccount.Name)
	}

	if adminUser.JWTText == nil || *adminUser.JWTText == "" {
		return fmt.Errorf("admin user '%s' does not have valid JWT token", adminUser.Name)
	}

	opts := []nats.Option{
		nats.Name(fmt.Sprintf("control-api-test-%s", cluster.ID)),
		nats.Timeout(5 * time.Second),
		nats.MaxReconnects(0),
	}

	userJWT := *adminUser.JWTText
	keyPair, err := nkeys.FromSeed([]byte(adminUser.NKey))
	if err != nil {
		return fmt.Errorf("failed to parse admin user nkey: %w", err)
	}

	opts = append(opts, nats.UserJWT(
		func() (string, error) { return userJWT, nil },
		func(nonce []byte) ([]byte, error) {
			return keyPair.Sign(nonce)
		},
	))

	nc, err := nats.Connect(cluster.GetNATSURL(), opts...)
	if err != nil {
		return fmt.Errorf("failed to connect to NATS: %w", err)
	}
	defer nc.Close()

	if !nc.IsConnected() {
		return fmt.Errorf("NATS connection is not established")
	}

	testSubject := fmt.Sprintf("CONTROL_TEST_%s", uuid.New().String())
	testMsg := "test-connection"
	if err := nc.Publish(testSubject, []byte(testMsg)); err != nil {
		return fmt.Errorf("failed to publish test message: %w", err)
	}

	if err := nc.Flush(); err != nil {
		return fmt.Errorf("failed to flush connection: %w", err)
	}

	return nil
}

func (cs *ClusterServer) CreateConnectionWithUser(cluster *models.Cluster, user *models.User) (*nats.Conn, error) {
	if user.CredsFile == nil || *user.CredsFile == "" {
		return nil, fmt.Errorf("用户 %s 没有有效的凭证文件", user.ID)
	}

	tmpFile, err := os.CreateTemp("", fmt.Sprintf("nats-creds-%s-*.creds", user.ID))
	if err != nil {
		return nil, fmt.Errorf("创建临时凭证文件失败: %v", err)
	}
	defer os.Remove(tmpFile.Name())

	if _, err := tmpFile.WriteString(*user.CredsFile); err != nil {
		tmpFile.Close()
		return nil, fmt.Errorf("写入凭证文件失败: %v", err)
	}
	tmpFile.Close()

	opts := []nats.Option{
		nats.Name(fmt.Sprintf("cluster-service-%s-%s", cluster.ID, user.ID)),
		nats.Timeout(10 * time.Second),
		nats.ReconnectWait(time.Second),
		nats.MaxReconnects(-1),
		nats.UserCredentials(tmpFile.Name()),
	}

	return nats.Connect(cluster.GetNATSURL(), opts...)
}

func (cs *ClusterServer) CreateNatsJetStream(conn *nats.Conn) (jetstream.JetStream, error) {
	js, err := jetstream.New(conn)
	if err != nil {
		return nil, fmt.Errorf("创建JetStream对象: %v", err)
	}
	return js, nil
}

func (cs *ClusterServer) CreateJetStreamContext(conn *nats.Conn) (JetStreamContext, error) {
	jsc, err := conn.JetStream()
	if err != nil {
		return nil, fmt.Errorf("创建JetStream上下文失败: %v", err)
	}
	return jsc, nil
}

func (cs *ClusterServer) CloseConnection(conn *nats.Conn) {
	if conn != nil && !conn.IsClosed() {
		conn.Close()
	}
}

func (cs *ClusterServer) GetStreamInfo(js jetstream.JetStream, streamName string) (*jetstream.StreamInfo, error) {
	log.WithContext(context.Background()).Infof("GetStreamInfo - name: %s", streamName)
	ctx := context.Background()
	stream, err := js.Stream(ctx, streamName)
	if err != nil {
		log.WithContext(context.Background()).Errorf("GetStreamInfo failed to get stream - name: %s, error: %v", streamName, err)
		return nil, err
	}

	return stream.Info(ctx)
}
