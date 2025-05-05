# Document Loader for Cloud SQL for PostgreSQL 

Document loader is the utility for loading documents from Cloud SQL for Postgres.  

## Supported Go Versions

Go version >= go 1.22.0

## Document Loader Creation and Retrieving documents

`DocumentLoader` uses `CloudSQLEngine` for connecting with the database. [Here](https://github.com/tmc/langchaingo/tree/main/internal/cloudsqlutil) is more info about postgres engine.

```go
package main

import (
	"context"
	"fmt"

	"github.com/tmc/langchaingo/documentloaders"
	"github.com/tmc/langchaingo/internal/cloudsqlutil"
	"github.com/tmc/langchaingo/documentloader/cloudsql"
)

func main() {
	ctx := context.Background()
	pgEngine, err := cloudsqlutil.NewPostgresEngine(ctx,
		cloudsqlutil.WithUser("my-user"),
		cloudsqlutil.WithPassword("my-password"),
		cloudsqlutil.WithDatabase("my-database"),
		cloudsqlutil.WithCloudSQLInstance("my-project-id", "region", "my-instance"),
	)
	if err != nil {
		panic(fmt.Errorf("error creating PostgresEngine: %s", err))
	}

	documentLoader, err := cloudsql.NewDocumentLoader(ctx, pgEngine, cloudsql.WithFormat("csv"))
	if err != nil {
		panic(fmt.Errorf("error creating DocumentLoader: %s", err))
	}
	
	docs, err := documentLoader.Load(ctx)
	if err != nil {
		panic(fmt.Errorf("error loading documents: %s", err))
	}	
	
	for _, doc := range docs {
        fmt.Printf("%v", doc)
	}
	
}
```
