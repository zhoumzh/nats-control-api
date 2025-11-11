package main

import (
	"database/sql"
	"flag"
	"fmt"
	"nats-control-api/internal/config"
	"nats-control-api/pkg/models"
	"log"

	_ "github.com/mattn/go-sqlite3"
)

type Migration struct {
	db *sql.DB
}

func NewMigration(db *sql.DB) *Migration {
	return &Migration{db: db}
}

func (m *Migration) MigrateAccounts() error {
	rows, err := m.db.Query("SELECT id FROM accounts")
	if err != nil {
		return fmt.Errorf("查询accounts失败: %w", err)
	}
	defer rows.Close()

	var updated int
	tx, err := m.db.Begin()
	if err != nil {
		return fmt.Errorf("开始事务失败: %w", err)
	}
	defer tx.Rollback()

	for rows.Next() {
		var oldID string
		if err := rows.Scan(&oldID); err != nil {
			return fmt.Errorf("扫描行失败: %w", err)
		}

		if len(oldID) > 3 && oldID[:3] == "ac_" {
			log.Printf("跳过已更新的Account ID: %s", oldID)
			continue
		}

		newID := models.GenID(models.AccountObjType)
		
		if _, err := tx.Exec("UPDATE accounts SET id = ? WHERE id = ?", newID, oldID); err != nil {
			return fmt.Errorf("更新account ID失败 (旧ID: %s): %w", oldID, err)
		}
		
		if _, err := tx.Exec("UPDATE users SET account_id = ? WHERE account_id = ?", newID, oldID); err != nil {
			return fmt.Errorf("更新users关联失败: %w", err)
		}
		
		if _, err := tx.Exec("UPDATE clusters SET system_account_id = ? WHERE system_account_id = ?", newID, oldID); err != nil {
			return fmt.Errorf("更新clusters关联失败: %w", err)
		}
		
		updated++
		log.Printf("更新Account: %s -> %s", oldID, newID)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("提交事务失败: %w", err)
	}

	log.Printf("成功更新 %d 条Account记录", updated)
	return nil
}

func (m *Migration) MigrateClusters() error {
	rows, err := m.db.Query("SELECT id FROM clusters")
	if err != nil {
		return fmt.Errorf("查询clusters失败: %w", err)
	}
	defer rows.Close()

	var updated int
	tx, err := m.db.Begin()
	if err != nil {
		return fmt.Errorf("开始事务失败: %w", err)
	}
	defer tx.Rollback()

	for rows.Next() {
		var oldID string
		if err := rows.Scan(&oldID); err != nil {
			return fmt.Errorf("扫描行失败: %w", err)
		}

		if len(oldID) > 3 && oldID[:3] == "cl_" {
			log.Printf("跳过已更新的Cluster ID: %s", oldID)
			continue
		}

		newID := models.GenID(models.ClusterObjType)
		
		if _, err := tx.Exec("UPDATE clusters SET id = ? WHERE id = ?", newID, oldID); err != nil {
			return fmt.Errorf("更新cluster ID失败 (旧ID: %s): %w", oldID, err)
		}
		
		if _, err := tx.Exec("UPDATE jetstreams SET cluster_id = ? WHERE cluster_id = ?", newID, oldID); err != nil {
			return fmt.Errorf("更新jetstreams关联失败: %w", err)
		}
		
		updated++
		log.Printf("更新Cluster: %s -> %s", oldID, newID)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("提交事务失败: %w", err)
	}

	log.Printf("成功更新 %d 条Cluster记录", updated)
	return nil
}

func (m *Migration) MigrateUsers() error {
	rows, err := m.db.Query("SELECT id FROM users")
	if err != nil {
		return fmt.Errorf("查询users失败: %w", err)
	}
	defer rows.Close()

	var updated int
	tx, err := m.db.Begin()
	if err != nil {
		return fmt.Errorf("开始事务失败: %w", err)
	}
	defer tx.Rollback()

	for rows.Next() {
		var oldID string
		if err := rows.Scan(&oldID); err != nil {
			return fmt.Errorf("扫描行失败: %w", err)
		}

		if len(oldID) > 3 && oldID[:3] == "us_" {
			log.Printf("跳过已更新的User ID: %s", oldID)
			continue
		}

		newID := models.GenID(models.UserObjType)
		
		if _, err := tx.Exec("UPDATE users SET id = ? WHERE id = ?", newID, oldID); err != nil {
			return fmt.Errorf("更新user ID失败 (旧ID: %s): %w", oldID, err)
		}
		
		if _, err := tx.Exec("UPDATE clusters SET system_user_id = ? WHERE system_user_id = ?", newID, oldID); err != nil {
			return fmt.Errorf("更新clusters关联失败: %w", err)
		}
		
		if _, err := tx.Exec("UPDATE jetstreams SET nats_operate_user_id = ? WHERE nats_operate_user_id = ?", newID, oldID); err != nil {
			return fmt.Errorf("更新jetstreams关联失败: %w", err)
		}
		
		if _, err := tx.Exec("UPDATE consumers SET nats_operate_user_id = ? WHERE nats_operate_user_id = ?", newID, oldID); err != nil {
			return fmt.Errorf("更新consumers关联失败: %w", err)
		}
		
		updated++
		log.Printf("更新User: %s -> %s", oldID, newID)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("提交事务失败: %w", err)
	}

	log.Printf("成功更新 %d 条User记录", updated)
	return nil
}

func (m *Migration) MigrateJetStreams() error {
	rows, err := m.db.Query("SELECT id FROM jetstreams")
	if err != nil {
		return fmt.Errorf("查询jetstreams失败: %w", err)
	}
	defer rows.Close()

	var updated int
	tx, err := m.db.Begin()
	if err != nil {
		return fmt.Errorf("开始事务失败: %w", err)
	}
	defer tx.Rollback()

	for rows.Next() {
		var oldID string
		if err := rows.Scan(&oldID); err != nil {
			return fmt.Errorf("扫描行失败: %w", err)
		}

		if len(oldID) > 3 && oldID[:3] == "js_" {
			log.Printf("跳过已更新的JetStream ID: %s", oldID)
			continue
		}

		newID := models.GenID(models.JetStreamObjType)
		
		if _, err := tx.Exec("UPDATE jetstreams SET id = ? WHERE id = ?", newID, oldID); err != nil {
			return fmt.Errorf("更新jetstream ID失败 (旧ID: %s): %w", oldID, err)
		}
		
		if _, err := tx.Exec("UPDATE consumers SET jetstream_id = ? WHERE jetstream_id = ?", newID, oldID); err != nil {
			return fmt.Errorf("更新consumers关联失败: %w", err)
		}
		
		updated++
		log.Printf("更新JetStream: %s -> %s", oldID, newID)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("提交事务失败: %w", err)
	}

	log.Printf("成功更新 %d 条JetStream记录", updated)
	return nil
}

func (m *Migration) MigrateConsumers() error {
	rows, err := m.db.Query("SELECT id FROM consumers")
	if err != nil {
		return fmt.Errorf("查询consumers失败: %w", err)
	}
	defer rows.Close()

	var updated int
	tx, err := m.db.Begin()
	if err != nil {
		return fmt.Errorf("开始事务失败: %w", err)
	}
	defer tx.Rollback()

	for rows.Next() {
		var oldID string
		if err := rows.Scan(&oldID); err != nil {
			return fmt.Errorf("扫描行失败: %w", err)
		}

		if len(oldID) > 3 && oldID[:3] == "cm_" {
			log.Printf("跳过已更新的Consumer ID: %s", oldID)
			continue
		}

		newID := models.GenID(models.ConsumerObjType)
		
		if _, err := tx.Exec("UPDATE consumers SET id = ? WHERE id = ?", newID, oldID); err != nil {
			return fmt.Errorf("更新consumer ID失败 (旧ID: %s): %w", oldID, err)
		}
		
		updated++
		log.Printf("更新Consumer: %s -> %s", oldID, newID)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("提交事务失败: %w", err)
	}

	log.Printf("成功更新 %d 条Consumer记录", updated)
	return nil
}

func main() {
	var configPath string
	var dbPath string
	flag.StringVar(&configPath, "config", "", "配置文件路径")
	flag.StringVar(&dbPath, "db", "./control.db", "数据库文件路径")
	flag.Parse()

	var dbDriver, dbDSN string

	if configPath != "" {
		cfg, err := config.Load(configPath)
		if err != nil {
			log.Fatalf("配置文件加载失败: %v", err)
		}
		dbDriver = cfg.Database.Driver
		dbDSN = cfg.Database.DSN
	} else {
		dbDriver = "sqlite3"
		dbDSN = dbPath
	}

	log.Printf("使用数据库: %s", dbDSN)

	db, err := sql.Open(dbDriver, dbDSN)
	if err != nil {
		log.Fatalf("数据库连接失败: %v", err)
	}
	defer db.Close()

	if err := db.Ping(); err != nil {
		log.Fatalf("数据库ping失败: %v", err)
	}

	log.Println("开始ID迁移...")
	
	migration := NewMigration(db)

	log.Println("\n=== 迁移Accounts ===")
	if err := migration.MigrateAccounts(); err != nil {
		log.Fatalf("Account迁移失败: %v", err)
	}

	log.Println("\n=== 迁移Clusters ===")
	if err := migration.MigrateClusters(); err != nil {
		log.Fatalf("Cluster迁移失败: %v", err)
	}

	log.Println("\n=== 迁移Users ===")
	if err := migration.MigrateUsers(); err != nil {
		log.Fatalf("User迁移失败: %v", err)
	}

	log.Println("\n=== 迁移JetStreams ===")
	if err := migration.MigrateJetStreams(); err != nil {
		log.Fatalf("JetStream迁移失败: %v", err)
	}

	log.Println("\n=== 迁移Consumers ===")
	if err := migration.MigrateConsumers(); err != nil {
		log.Fatalf("Consumer迁移失败: %v", err)
	}

	log.Println("\n✅ ID迁移完成！")
}