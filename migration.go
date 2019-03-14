package migration

import (
	"context"
	"database/sql"
	"fmt"
)

// Migrate do the sql migration
func Migrate(db *sql.DB, appID uint32, statements []string) error {
	if appID == 0 {
		panic("invalid params: appID can't be 0")
	}

	ctx := context.Background()

	conn, err := db.Conn(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()

	if _, err := conn.ExecContext(ctx,
		"begin exclusive",
	); err != nil {
		return err
	}
	commited := false
	defer func() {
		if !commited {
			conn.ExecContext(ctx, "rollback")
		}
	}()

	var curAppID uint32
	if err := conn.QueryRowContext(ctx,
		"pragma application_id",
	).Scan(&curAppID); err != nil {
		return err
	}

	// newly created database will have schema_version equal to 0
	// in that case, the value of curAppID doesn't matter

	if curAppID != appID {
		var schemaVersion int
		if err := conn.QueryRowContext(ctx,
			"pragma schema_version",
		).Scan(&schemaVersion); err != nil {
			return err
		}
		if schemaVersion != 0 {
			return fmt.Errorf("Invalid application_id on database file")
		}
		if _, err := conn.ExecContext(ctx,
			fmt.Sprintf("pragma application_id = %d", appID),
		); err != nil {
			return err
		}
	}

	var userVersion int
	if err := conn.QueryRowContext(ctx,
		"pragma user_version",
	).Scan(&userVersion); err != nil {
		return err
	}
	for ; userVersion < len(statements); userVersion++ {
		statement := statements[userVersion]
		if _, err := conn.ExecContext(ctx, statement); err != nil {
			return err
		}
	}
	if _, err := conn.ExecContext(ctx,
		fmt.Sprintf("pragma user_version = %d", userVersion),
	); err != nil {
		return err
	}

	if _, err := conn.ExecContext(ctx, "commit"); err != nil {
		return err
	}
	commited = true

	return nil
}
