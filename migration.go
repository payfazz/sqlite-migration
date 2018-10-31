package migration

import (
	"context"
	"database/sql"
	"fmt"
	"log"
)

type MigrateParams struct {
	Context       context.Context
	Database      *sql.DB
	ErrorLog      *log.Logger
	ApplicationID uint32
	Statements    []string
}

func Migrate(p MigrateParams) error {
	if p.ApplicationID == 0 {
		panic("migration: invalid params: ApplicationID can't be 0")
	}
	ctx := p.Context

	conn, err := p.Database.Conn(ctx)
	if err != nil {
		p.ErrorLog.Println(err)
		return err
	}
	defer conn.Close()

	if _, err := conn.ExecContext(ctx,
		"begin exclusive",
	); err != nil {
		p.ErrorLog.Println(err)
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
		p.ErrorLog.Println(err)
		return err
	}
	if curAppID != p.ApplicationID {
		var schemaVersion int
		if err := conn.QueryRowContext(ctx,
			"pragma schema_version",
		).Scan(&schemaVersion); err != nil {
			p.ErrorLog.Println(err)
			return err
		}
		if schemaVersion != 0 {
			return fmt.Errorf("Invalid application_id on database file")
		}
		if _, err := conn.ExecContext(ctx,
			fmt.Sprintf("pragma application_id = %d", p.ApplicationID),
		); err != nil {
			p.ErrorLog.Println(err)
			return err
		}
	}

	var userVersion int
	if err := conn.QueryRowContext(ctx,
		"pragma user_version",
	).Scan(&userVersion); err != nil {
		p.ErrorLog.Println(err)
		return err
	}
	for ; userVersion < len(p.Statements); userVersion++ {
		statement := p.Statements[userVersion]
		if _, err := conn.ExecContext(ctx, statement); err != nil {
			p.ErrorLog.Println(err)
			return err
		}
	}
	if _, err := conn.ExecContext(ctx,
		fmt.Sprintf("pragma user_version = %d", userVersion),
	); err != nil {
		p.ErrorLog.Println(err)
		return err
	}

	if _, err := conn.ExecContext(ctx, "commit"); err != nil {
		p.ErrorLog.Println(err)
		return err
	}
	commited = true

	return nil
}
