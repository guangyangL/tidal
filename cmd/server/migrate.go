package main

import (
	"fmt"
	"log"
	"strings"

	"github.com/jmoiron/sqlx"
)

func autoMigrate(db *sqlx.DB) {
	ddls := []string{
		`CREATE TABLE IF NOT EXISTS t_user_wallet (
			user_id       BIGINT   NOT NULL,
			balance       BIGINT   NOT NULL DEFAULT 0,
			wallet_type   TINYINT  NOT NULL DEFAULT 0,
			version       INT      NOT NULL DEFAULT 0,
			update_time   TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
			PRIMARY KEY (user_id)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`,

		`CREATE TABLE IF NOT EXISTS t_gift_config (
			gift_id     INT          NOT NULL AUTO_INCREMENT,
			name        VARCHAR(64)  NOT NULL,
			price       BIGINT       NOT NULL,
			status      TINYINT      NOT NULL DEFAULT 1,
			extra       JSON         DEFAULT NULL,
			create_time TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,
			PRIMARY KEY (gift_id)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`,

		`CREATE TABLE IF NOT EXISTS t_gift_record (
			id            BIGINT       NOT NULL AUTO_INCREMENT,
			batch_token   VARCHAR(64)  NOT NULL,
			room_id       BIGINT       NOT NULL,
			user_id       BIGINT       NOT NULL,
			anchor_id     BIGINT       NOT NULL,
			gift_id       INT          NOT NULL,
			total_amount  BIGINT       NOT NULL,
			status        TINYINT      NOT NULL DEFAULT 1,
			create_time   TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,
			PRIMARY KEY (id),
			UNIQUE KEY uk_batch_token (batch_token),
			KEY idx_room_anchor (room_id, anchor_id)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`,
	}

	for _, ddl := range ddls {
		if _, err := db.Exec(ddl); err != nil {
			log.Fatalf("auto-migrate: %v", err)
		}
	}

	seedGiftConfig(db)
	seedTestUsers(db)
}

func seedGiftConfig(db *sqlx.DB) {
	var count int
	if err := db.Get(&count, "SELECT COUNT(*) FROM t_gift_config"); err != nil {
		log.Printf("seed gift config check: %v", err)
		return
	}
	if count > 0 {
		return
	}

	gifts := []struct {
		Name  string
		Price int64
	}{
		{"荧光棒", 10},
		{"心动盲盒", 30},
		{"跑车", 100},
		{"火箭", 300},
		{"嘉年华", 1000},
	}

	for _, g := range gifts {
		_, err := db.Exec("INSERT INTO t_gift_config (name, price) VALUES (?, ?)", g.Name, g.Price)
		if err != nil {
			log.Printf("seed gift %s: %v", g.Name, err)
		}
	}
	log.Print("gift config seeded")
}

func seedTestUsers(db *sqlx.DB) {
	const minUsers = 1000
	const startUID = 10000
	const initBalance = 1_000_000

	var count int
	if err := db.Get(&count, "SELECT COUNT(*) FROM t_user_wallet WHERE user_id >= ?", startUID); err != nil {
		log.Printf("seed test users check: %v", err)
		return
	}
	if count >= minUsers {
		return
	}

	needed := minUsers - count
	// batch insert in chunks of 500
	chunk := 500
	for offset := 0; offset < needed; offset += chunk {
		end := min(offset+chunk, needed)
		var sb strings.Builder
		sb.WriteString("INSERT INTO t_user_wallet (user_id, balance, wallet_type) VALUES ")
		for i := offset; i < end; i++ {
			if i > offset {
				sb.WriteString(", ")
			}
			fmt.Fprintf(&sb, "(%d, %d, 0)", startUID+int64(count+i), initBalance)
		}
		sb.WriteString(" ON DUPLICATE KEY UPDATE balance=VALUES(balance), version=0")
		if _, err := db.Exec(sb.String()); err != nil {
			log.Printf("seed test users: %v", err)
			return
		}
	}
	log.Printf("test users seeded: %d -> %d", count, count+needed)
}
