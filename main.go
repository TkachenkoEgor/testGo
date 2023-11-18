package main

import (
	"database/sql"
	"example.com/m/v2/config"
	"example.com/m/v2/internal/model"
	"example.com/m/v2/internal/service"
	"fmt"
	_ "github.com/ClickHouse/clickhouse-go"
	"github.com/gin-gonic/gin"
	"log"
	"net/http"
	"time"
)

func main() {
	cfg, err := config.Read("config.yaml")
	if err != nil {
		fmt.Println(err)
		return
	}

	db, err := sql.Open("clickhouse", cfg.ClickHouse.ConnectionString)
	if err != nil {
		log.Fatal(err)
	}
	router := gin.Default()

	router.POST("/api/event", getEventsByTypeAndTimeRangeHandler(db))
	router.POST("/api/insert-events", insertEvents(db))
	router.GET("/api/users-with-multiple-event-types", getUsersWithMultipleEventTypes(db))
	router.Run(cfg.Service.Addres + cfg.Service.Port)

	defer db.Close()

}
func getUsersWithMultipleEventTypes(db *sql.DB) gin.HandlerFunc {
	return func(c *gin.Context) {
		users, err := service.GetUsersWithMultipleEventTypes(db, c.Request.Context())
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		}
		c.JSON(http.StatusOK, users)

	}
}

func insertEvents(db *sql.DB) gin.HandlerFunc {
	return func(c *gin.Context) {
		var events []model.Event

		if err := c.BindJSON(&events); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}

		err := service.InsertEvents(db, c.Request.Context(), events)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}
		c.JSON(http.StatusOK, gin.H{"message": "Events inserted successfully"})

	}
}
func getEventsByTypeAndTimeRangeHandler(db *sql.DB) gin.HandlerFunc {
	return func(c *gin.Context) {
		eventType := c.Query("eventType")
		startTime, err := time.Parse(time.RFC3339, c.Query("startTime"))
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid start time"})
			return
		}
		endTime, err := time.Parse(time.RFC3339, c.Query("endTime"))
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid end time"})
			return
		}

		events, err := service.GetEventsByTypeAndTimeRange(db, c.Request.Context(), eventType, startTime, endTime)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}

		c.JSON(http.StatusOK, events)
	}
}
