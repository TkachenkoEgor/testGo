package config

import (
	"fmt"
	"github.com/spf13/viper"
)

type Config struct {
	Service    *Service
	ClickHouse *ClickHouse
}
type Service struct {
	Port   string `envconfig:"PORT" default:"8080"`
	Addres string `envconfig:"ADDRES" default:""`
}

type ClickHouse struct {
	ConnectionString string `envconfig:"connection_string" default:""`
	DriverName       string `envconfig:"driver_name" default:""`
}

func Read(filePath string) (Config, error) {
	var config Config

	viper.SetConfigFile(filePath)

	if err := viper.ReadInConfig(); err != nil {
		return Config{}, fmt.Errorf("не удалось прочитать файл конфигурации: %v", err)
	}

	config.Service = &Service{}
	config.ClickHouse = &ClickHouse{}

	if err := viper.Unmarshal(&config); err != nil {
		return Config{}, fmt.Errorf("не удалось размаршалить конфигурацию: %v", err)
	}

	return config, nil
}
