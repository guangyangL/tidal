package repository

import (
	"fmt"

	"github.com/jmoiron/sqlx"
	"github.com/spf13/viper"
)

func InitMySQL() *sqlx.DB {
	dsn := viper.GetString("mysql.dsn")
	db, err := sqlx.Connect("mysql", dsn)
	if err != nil {
		panic(fmt.Sprintf("failed to connect to MySQL: %v", err))
	}
	db.SetMaxOpenConns(viper.GetInt("mysql.max_open_conns"))
	db.SetMaxIdleConns(viper.GetInt("mysql.max_idle_conns"))
	return db
}
