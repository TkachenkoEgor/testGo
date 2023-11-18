package service

import (
	"context"
	"database/sql"
	"example.com/m/v2/internal/model"
	"log"
	"time"
)

func GetUnicMoreThousand(db *sql.DB, ctx context.Context) (events []model.Event, err error) {
	query := `
		SELECT eventType
		FROM events
		GROUP BY eventType
		HAVING count() > 1000
	`

	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var event model.Event

		if err := rows.Scan(&event.EventType); err != nil {
			return nil, err
		}
		events = append(events, event)
	}

	return events, nil
}

func GetFirstDayOfMonthEvents(db *sql.DB, ctx context.Context) (events []model.Event, err error) {
	query := `
		SELECT *
		FROM events
		WHERE toStartOfDay(eventTime) = toStartOfMonth(eventTime)
	`

	// Выполняем запрос
	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		log.Fatal(err)
		return nil, err
	}
	defer rows.Close()

	// Обработка результатов
	for rows.Next() {
		var event model.Event
		if err := rows.Scan(&event.EventID, &event.EventType, &event.UserID, &event.EventTime, &event.Payload); err != nil {
			log.Fatal(err)
			return nil, err
		}
	}

	return events, nil
}

func GetUsersWithMultipleEventTypes(db *sql.DB, ctx context.Context) (events []model.Event, err error) {
	query := `
		SELECT UserID
		FROM events
		GROUP BY UserID
		HAVING count(DISTINCT eventType) > 3
	`
	rows, err := db.Query(query)
	if err != nil {
		log.Fatal(err)
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var event model.Event
		if err := rows.Scan(&event.EventID, &event.EventType, &event.UserID, &event.EventTime, &event.Payload); err != nil {
			log.Fatal(err)
			return nil, err
		}
	}

	return events, nil
}

func InsertEvents(db *sql.DB, ctx context.Context, events []model.Event) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			tx.Rollback()
			return
		}
		err = tx.Commit()
	}()

	stmt, err := tx.PrepareContext(ctx, "INSERT INTO events (EventID, EventType, UserID, EventTime, Payload) VALUES (?, ?, ?, ?, ?)")
	if err != nil {
		return err
	}
	defer stmt.Close()

	for _, e := range events {
		_, err := stmt.ExecContext(ctx, e.EventID, e.EventType, e.UserID, e.EventTime, e.Payload)
		if err != nil {
			return err
		}
	}

	return nil
}

func GetEventsByTypeAndTimeRange(db *sql.DB, ctx context.Context, eventType string, startTime, endTime time.Time) ([]model.Event, error) {
	query := `
		SELECT EventID, EventType, UserID, EventTime, Payload
		FROM events
		WHERE EventType = ? AND EventTime BETWEEN ? AND ?
	`

	rows, err := db.QueryContext(ctx, query, eventType, startTime, endTime)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var events []model.Event
	for rows.Next() {
		var event model.Event

		if err := rows.Scan(&event.EventID, &event.EventType, &event.UserID, &event.EventTime, &event.Payload); err != nil {
			return nil, err
		}
		events = append(events, event)
	}

	return events, nil
}
