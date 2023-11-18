CREATE TABLE events (
                        eventID Int64,
                        eventType String,
                        userID Int64,
                        eventTime DateTime,
                        payload String
) ENGINE = MergeTree
ORDER BY (eventID, eventTime);
