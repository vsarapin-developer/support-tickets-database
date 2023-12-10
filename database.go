package database

import (
	"database/sql"
	"log"
	"os"
)

type DBConfig struct {
	Login    string
	Password string
	Host     string
	Port     string
	Name     string
}

func LoadDBConfig() (DBConfig, bool) {
	databaseLogin, loginExists := os.LookupEnv("DATABASE_LOGIN")
	databasePassword, passwordExists := os.LookupEnv("DATABASE_PASSWORD")
	databaseHost, hostExists := os.LookupEnv("DATABASE_HOST")
	databasePort, portExists := os.LookupEnv("DATABASE_PORT")
	databaseName, nameExists := os.LookupEnv("DATABASE_NAME")

	if !loginExists || !passwordExists || !hostExists || !portExists || !nameExists {
		log.Fatal("Database configuration missing")
		return DBConfig{}, false
	}

	return DBConfig{
		Login:    databaseLogin,
		Password: databasePassword,
		Host:     databaseHost,
		Port:     databasePort,
		Name:     databaseName,
	}, true
}

func GetDBConnection(dbConfig DBConfig) (*sql.DB, error) {
	connectionString := dbConfig.Login + ":" + dbConfig.Password + "@tcp(" + dbConfig.Host + ":" + dbConfig.Port + ")/" + dbConfig.Name
	db, err := sql.Open("mysql", connectionString)
	if err != nil {
		return nil, err
	}
	return db, nil
}
