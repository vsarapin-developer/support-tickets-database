package database

import (
	"log"
	"slices"

	"github.com/cheggaaa/pb/v3"
)

func GetTicketsCount(dbConfig DBConfig, progress *pb.ProgressBar) ([]string, map[string][4]uint) {
	db, err := GetDBConnection(dbConfig)
	if err != nil {
		log.Fatal(err.Error())
	}
	defer db.Close()

	rows, err := db.Query(`
        WITH RECURSIVE date_series AS (
            SELECT '2024-05-26' AS created_date
            UNION ALL
            SELECT created_date + INTERVAL 1 DAY
            FROM date_series
            WHERE created_date <= '2024-06-25'
        ), hour_series AS (
            SELECT 0 AS hour_start, 6 AS hour_end
            UNION ALL
            SELECT 6 AS hour_start, 12 AS hour_end
            UNION ALL
            SELECT 12 AS hour_start, 18 AS hour_end
            UNION ALL
            SELECT 18 AS hour_start, 24 AS hour_end
        )
        SELECT
            COALESCE(COUNT(DISTINCT id), 0) AS tickets_count,
            date_series.created_date,
            hour_series.hour_start,
            hour_series.hour_end
        FROM
            date_series
        CROSS JOIN
            hour_series
        LEFT JOIN
            tickets ON DATE_FORMAT(tickets.created_at, '%Y-%m-%d') = date_series.created_date
                    AND recipient_id = 1 AND is_inbox = 0
                    AND HOUR(tickets.created_at) >= hour_series.hour_start
                    AND HOUR(tickets.created_at) < hour_series.hour_end
        GROUP BY
            date_series.created_date, hour_series.hour_start, hour_series.hour_end
        ORDER BY
            date_series.created_date, hour_series.hour_start;
    `)
	if err != nil {
		panic(err.Error())
	}
	defer rows.Close()

	result := make(map[string][4]uint)
	var dates []string

	for rows.Next() {
		var ticketsCount uint
		var createdAtStr string
		var hourStart, hourEnd int

		err = rows.Scan(&ticketsCount, &createdAtStr, &hourStart, &hourEnd)
		if err != nil {
			panic(err)
		}

		if !slices.Contains(dates, createdAtStr) {
			dates = append(dates, createdAtStr)
		}
		dateKey := createdAtStr
		tempArray := result[dateKey]
		index := (hourStart / 6) % 4
		tempArray[index] += ticketsCount
		result[dateKey] = tempArray
	}
	progress.Increment()
	return dates, result
}

func GetContactUsCount(dbConfig DBConfig, progress *pb.ProgressBar) map[string][4]uint {
	db, err := GetDBConnection(dbConfig)
	if err != nil {
		log.Fatal(err.Error())
	}
	defer db.Close()

	rows, err := db.Query(`
		WITH RECURSIVE date_series AS (
			SELECT '2024-05-26' AS created_date
			UNION ALL
			SELECT created_date + INTERVAL 1 DAY
			FROM date_series
			WHERE created_date <= '2024-06-25'
		), hour_series AS (
			SELECT 0 AS hour_start, 6 AS hour_end
			UNION ALL
			SELECT 6 AS hour_start, 12 AS hour_end
			UNION ALL
			SELECT 12 AS hour_start, 18 AS hour_end
			UNION ALL
			SELECT 18 AS hour_start, 24 AS hour_end
		)
		SELECT
			COALESCE(COUNT(DISTINCT id), 0) AS inboxes_count,
			date_series.created_date,
			hour_series.hour_start,
			hour_series.hour_end
		FROM
			date_series
		CROSS JOIN
			hour_series
		LEFT JOIN
			inboxes ON DATE_FORMAT(inboxes.created_at, '%Y-%m-%d') = date_series.created_date
					AND inboxes.created_at >= '2024-05-26' AND inboxes.created_at <= '2024-06-25'
					AND HOUR(inboxes.created_at) >= hour_series.hour_start
                    AND HOUR(inboxes.created_at) < hour_series.hour_end
		GROUP BY
			date_series.created_date, hour_series.hour_start, hour_series.hour_end
		ORDER BY
			date_series.created_date, hour_series.hour_start;
	`)
	if err != nil {
		panic(err.Error())
	}
	defer rows.Close()

	result := make(map[string][4]uint)

	for rows.Next() {
		var inboxesCount uint
		var createdAtStr string
		var hourStart, hourEnd int

		err = rows.Scan(&inboxesCount, &createdAtStr, &hourStart, &hourEnd)
		if err != nil {
			panic(err)
		}

		dateKey := createdAtStr
		tempArray := result[dateKey]
		index := (hourStart / 6) % 4
		tempArray[index] += inboxesCount
		result[dateKey] = tempArray

	}
	progress.Increment()
	return result
}

func GetTicketsMessagesCount(dbConfig DBConfig, progress *pb.ProgressBar) map[string][4]uint {
	db, err := GetDBConnection(dbConfig)
	if err != nil {
		log.Fatal(err.Error())
	}
	defer db.Close()

	rows, err := db.Query(`
		WITH RECURSIVE date_series AS (
			SELECT '2024-05-26' AS created_date
			UNION ALL
			SELECT created_date + INTERVAL 1 DAY
			FROM date_series
			WHERE created_date <= '2024-06-25'
		), hour_series AS (
			SELECT 0 AS hour_start, 6 AS hour_end
			UNION ALL
			SELECT 6 AS hour_start, 12 AS hour_end
			UNION ALL
			SELECT 12 AS hour_start, 18 AS hour_end
			UNION ALL
			SELECT 18 AS hour_start, 24 AS hour_end
		)
		SELECT
			COALESCE(COUNT(DISTINCT tm.id), 0) AS messages_count,
			date_series.created_date,
			hour_series.hour_start,
			hour_series.hour_end
		FROM
			date_series
		CROSS JOIN
			hour_series
		LEFT JOIN
			tickets_messages tm ON DATE_FORMAT(tm.created_at, '%Y-%m-%d') = date_series.created_date
							AND tm.author_id != 1
							AND tm.created_at >= '2024-05-26'
							AND tm.created_at <= '2024-06-25'
							AND HOUR(tm.created_at) >= hour_series.hour_start
                    		AND HOUR(tm.created_at) < hour_series.hour_end
		LEFT JOIN
        	tickets t ON tm.ticket_id = t.id
		WHERE
			t.recipient_id = 1 AND t.is_inbox = 0
		GROUP BY
			date_series.created_date, hour_series.hour_start, hour_series.hour_end
		ORDER BY
			date_series.created_date, hour_series.hour_start;
	`)
	if err != nil {
		panic(err.Error())
	}
	defer rows.Close()

	result := make(map[string][4]uint)

	for rows.Next() {
		var messagesCount uint
		var createdAtStr string
		var hourStart, hourEnd int

		err = rows.Scan(&messagesCount, &createdAtStr, &hourStart, &hourEnd)
		if err != nil {
			panic(err)
		}

		dateKey := createdAtStr
		tempArray := result[dateKey]
		index := (hourStart / 6) % 4
		tempArray[index] += messagesCount
		result[dateKey] = tempArray
	}
	progress.Increment()
	return result
}

func GetOperatorsBecomesCount(dbConfig DBConfig, progress *pb.ProgressBar) map[string][4]uint {
	db, err := GetDBConnection(dbConfig)
	if err != nil {
		log.Fatal(err.Error())
	}
	defer db.Close()

	rows, err := db.Query(`
		WITH RECURSIVE date_series AS (
			SELECT '2024-05-26' AS created_date
			UNION ALL
			SELECT created_date + INTERVAL 1 DAY
			FROM date_series
			WHERE created_date <= '2024-06-25'
		), hour_series AS (
			SELECT 0 AS hour_start, 6 AS hour_end
			UNION ALL
			SELECT 6 AS hour_start, 12 AS hour_end
			UNION ALL
			SELECT 12 AS hour_start, 18 AS hour_end
			UNION ALL
			SELECT 18 AS hour_start, 24 AS hour_end
		)
		SELECT
			COALESCE(COUNT(DISTINCT akb.id), 0) AS agent_count,
			date_series.created_date,
			hour_series.hour_start,
			hour_series.hour_end
		FROM
			date_series
		CROSS JOIN
			hour_series
		LEFT JOIN
			agent_kyc_becomes akb ON DATE_FORMAT(akb.created_at, '%Y-%m-%d') = date_series.created_date
						AND akb.interesting_account = 'Operator'
						AND akb.created_at >= '2024-05-26'
						AND akb.created_at <= '2024-06-25'
						AND HOUR(akb.created_at) >= hour_series.hour_start
                    	AND HOUR(akb.created_at) < hour_series.hour_end
		GROUP BY
			date_series.created_date, hour_series.hour_start, hour_series.hour_end
		ORDER BY
			date_series.created_date, hour_series.hour_start;
	`)
	if err != nil {
		panic(err.Error())
	}
	defer rows.Close()

	result := make(map[string][4]uint)

	for rows.Next() {
		var agentCount uint
		var createdAtStr string
		var hourStart, hourEnd int

		err = rows.Scan(&agentCount, &createdAtStr, &hourStart, &hourEnd)
		if err != nil {
			panic(err)
		}

		dateKey := createdAtStr
		tempArray := result[dateKey]
		index := (hourStart / 6) % 4
		tempArray[index] += agentCount
		result[dateKey] = tempArray
	}
	progress.Increment()
	return result
}

func GetVendorsBecomesCount(dbConfig DBConfig, progress *pb.ProgressBar) map[string][4]uint {
	db, err := GetDBConnection(dbConfig)
	if err != nil {
		log.Fatal(err.Error())
	}
	defer db.Close()

	rows, err := db.Query(`
		WITH RECURSIVE date_series AS (
			SELECT '2024-05-26' AS created_date
			UNION ALL
			SELECT created_date + INTERVAL 1 DAY
			FROM date_series
			WHERE created_date <= '2024-06-25'
		), hour_series AS (
			SELECT 0 AS hour_start, 6 AS hour_end
			UNION ALL
			SELECT 6 AS hour_start, 12 AS hour_end
			UNION ALL
			SELECT 12 AS hour_start, 18 AS hour_end
			UNION ALL
			SELECT 18 AS hour_start, 24 AS hour_end
		)
		SELECT
			COALESCE(COUNT(DISTINCT akb.id), 0) AS agent_count,
			date_series.created_date,
			hour_series.hour_start,
			hour_series.hour_end
		FROM
			date_series
		CROSS JOIN
			hour_series
		LEFT JOIN
			agent_kyc_becomes akb ON DATE_FORMAT(akb.created_at, '%Y-%m-%d') = date_series.created_date
						AND akb.interesting_account = 'Vendor'
						AND akb.created_at >= '2024-05-26'
						AND akb.created_at <= '2024-06-25'
						AND HOUR(akb.created_at) >= hour_series.hour_start
                    	AND HOUR(akb.created_at) < hour_series.hour_end
		GROUP BY
			date_series.created_date, hour_series.hour_start, hour_series.hour_end
		ORDER BY
			date_series.created_date, hour_series.hour_start;
	`)
	if err != nil {
		panic(err.Error())
	}
	defer rows.Close()

	result := make(map[string][4]uint)

	for rows.Next() {
		var agentCount uint
		var createdAtStr string
		var hourStart, hourEnd int

		err = rows.Scan(&agentCount, &createdAtStr, &hourStart, &hourEnd)
		if err != nil {
			panic(err)
		}

		dateKey := createdAtStr
		tempArray := result[dateKey]
		index := (hourStart / 6) % 4
		tempArray[index] += agentCount
		result[dateKey] = tempArray
	}
	progress.Increment()
	return result
}
