package main

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"runtime"
	"sync"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	gofakeit "github.com/brianvoe/gofakeit/v7"
	"github.com/loicalleyne/bodkin"
	"github.com/loicalleyne/bodkin/reader"
	duckdb "github.com/marcboeker/go-duckdb"
)

// Use a struct to hold the database connection, avoiding globals.
type DB struct {
	conn *sql.DB
}

type Foo struct {
	Str      string
	Int      int
	Pointer  *int
	Name     string   `fake:""`
	Sentence string   `fake:"{sentence:3}"`
	RandStr  string   `fake:"{randomstring:[hello,world]}"`
	Number   string   `fake:"{number:1,10}"`
	Regex    string   `fake:"{regex:[abcdef]}"`
	Array    []string `fakesize:"2"`
	Bar      Bar
	Skip     *string `fake:"skip"`
	Created  time.Time
}

type Bar struct {
	Name       string
	Number     int
	Float      float32
	ArrayRange []string `fakesize:"2,6"`
}

func initDuckDB(dbPath string) (*DB, error) {
	dbPath = fmt.Sprintf("%s?threads=%d&max_memory=2GB", dbPath, runtime.NumCPU()) // Optimize for CPU and RAM.

	connector, err := duckdb.NewConnector(dbPath, func(execer driver.ExecerContext) error {
		bootQueries := []string{
			"INSTALL 'json'",
			"LOAD 'json'",
			"INSTALL 'parquet'",
			"LOAD 'parquet'",
		}

		var err error
		for _, qry := range bootQueries {
			_, err = execer.ExecContext(context.Background(), qry, nil)
			if err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("connector error: %w", err)
	}

	db := sql.OpenDB(connector)
	db.SetMaxOpenConns(runtime.NumCPU() * 2) // More connections, up to a point related to CPU cores.
	db.SetMaxIdleConns(runtime.NumCPU())     // Keep a good number of connections idle.
	db.SetConnMaxLifetime(5 * time.Minute)   // Recycle conns periodically
	db.SetConnMaxIdleTime(1 * time.Minute)
	return &DB{conn: db}, nil
}

func main() {
	db, err := initDuckDB("duck.db")
	if err != nil {
		log.Fatal(err)
	}
	defer db.conn.Close()

	start := time.Now()
	log.Println("start")

	// Schema inference optimization.  Do it *once* before the loop.  Much faster.
	u := bodkin.NewBodkin(bodkin.WithInferTimeUnits(), bodkin.WithTypeConversion())
	var sampleFoos [10]Foo // Create a small, fixed-size array for schema inference
	for i := 0; i < len(sampleFoos); i++ {
		gofakeit.Struct(&sampleFoos[i])
		if err := u.Unify(sampleFoos[i]); err != nil {
			panic(err)
		}
	}
	err = u.ExportSchemaFile("temp.bak")
	if err != nil {
		panic(err)
	}
	schema, err := u.ImportSchemaFile("temp.bak")
	if err != nil {
		panic(err)
	}
	log.Printf("time to infer schema: %v\n", time.Since(start))
	log.Printf("union %v\n", schema.String())

	// Data Generation and Writing Optimization
	fileGenStart := time.Now()
	generateAndWriteData("temp.json", schema, 100000)
	log.Printf("file generation took: %v\n", time.Since(fileGenStart))

	// Data Reading and Insertion Optimization
	insertStart := time.Now()
	err = readAndInsertData(db, "temp.json", schema)
	if err != nil {
		panic(err)
	}
	log.Printf("time to insert: %v\n", time.Since(insertStart))
	log.Printf("total elapsed: %v\n", time.Since(start))
	log.Println("end")
}

func generateAndWriteData(filename string, schema *arrow.Schema, count int) {
	file, err := os.Create(filename)
	if err != nil {
		panic(err)
	}
	defer file.Close()

	// Use a buffered writer for significantly improved write performance.
	buf := make([]byte, 0, 65536) // 64KB buffer, adjust as needed

	var wg sync.WaitGroup
	numWorkers := runtime.NumCPU()
	jobChan := make(chan struct{}, count)
	resultChan := make(chan []byte, numWorkers)

	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range jobChan {
				var f Foo
				gofakeit.Struct(&f)
				data, err := json.Marshal(f)
				if err != nil {
					panic(err) // Consider structured logging
				}
				data = append(data, '\n') //Avoid allocation
				resultChan <- data
			}
		}()
	}

	// Close resultChan once all worker go routines finish.
	go func() {
		wg.Wait()
		close(resultChan)
	}()

	// Create jobs
	for i := 0; i < count; i++ {
		jobChan <- struct{}{}
	}
	close(jobChan)

	for res := range resultChan {
		buf = append(buf, res...)
		if len(buf) >= 32768 { // Flush when nearing capacity
			_, err = file.Write(buf)
			if err != nil {
				panic(err)
			}
			buf = buf[:0] // Reset slice without reallocating
		}
	}
	// Flush remaining data

	if len(buf) > 0 {
		_, err = file.Write(buf)
		if err != nil {
			panic(err)
		}
	}
}

func readAndInsertData(db *DB, filename string, schema *arrow.Schema) error {
	ff, err := os.Open(filename)
	if err != nil {
		return err
	}
	defer ff.Close()

	// Use a larger chunk size, tuned to your data and system memory.
	r, err := reader.NewReader(schema, 0, reader.WithIOReader(ff, reader.DefaultDelimiter), reader.WithChunk(1024*64)) // Increased Chunk size.
	if err != nil {
		return err
	}

	// Use a single, prepared statement for all inserts.  This is *much* faster.
	insertStmt := generateInsertStatement(schema)

	// Batch processing and DuckDB interaction.
	batchSize := 500 // Tune this value.  Start with a reasonable guess.
	var totalRecords int
	var batches int
	pool := memory.NewGoAllocator() // Use the default Go allocator, it's usually fine.

	for r.NextBatch(batchSize) {
		recs := r.RecordBatch()
		totalRecords += len(recs)
		batches++
		if err := duckOut(db, schema, recs, insertStmt, pool); err != nil {
			return fmt.Errorf("duckOut failed: %w", err)
		}
		// Release records to free memory
		for _, rec := range recs {
			rec.Release()
		}
	}
	log.Println("records", totalRecords, "batches", batches)
	return nil
}

func duckOut(db *DB, schema *arrow.Schema, recs []arrow.Record, insertStmt string, pool *memory.GoAllocator) error {
	// Get a connection from the pool
	conn, err := db.conn.Conn(context.Background())
	if err != nil {
		return fmt.Errorf("failed to get connection: %w", err)
	}
	defer conn.Close()

	// Use the connection directly with the DuckDB driver
	var dba *duckdb.Arrow
	err = conn.Raw(func(driverConn interface{}) error {
		if driverConn == nil {
			return fmt.Errorf("driver connection is nil")
		}

		// Create Arrow connection from the driver connection
		var err error
		dba, err = duckdb.NewArrowFromConn(driverConn.(driver.Conn))
		return err
	})

	if err != nil {
		return fmt.Errorf("failed to create Arrow connection: %w", err)
	}

	if dba == nil {
		return fmt.Errorf("failed to create Arrow connection: connection is nil")
	}

	recReader, err := array.NewRecordReader(schema, recs)
	if err != nil {
		return fmt.Errorf("failed creates recordreader: %w", err)
	}
	arrowViewRelease, err := dba.RegisterView(recReader, "arrowrecs")
	if err != nil {
		return err
	}
	defer arrowViewRelease()

	// Use QueryContext instead of ExecContext for DuckDB Arrow
	_, err = dba.QueryContext(context.Background(), "BEGIN TRANSACTION;")
	if err != nil {
		return err
	}

	_, err = dba.QueryContext(context.Background(), insertStmt)
	if err != nil {
		_, err = dba.QueryContext(context.Background(), "CREATE TABLE IF NOT EXISTS t1 AS SELECT * FROM arrowrecs")
		if err != nil {
			return err
		}
	}
	// Now, use a *prepared statement* for the insertion.  Crucial for performance.
	_, err = dba.QueryContext(context.Background(), insertStmt)
	if err != nil {
		return err
	}
	_, err = dba.QueryContext(context.Background(), "COMMIT;")
	return err
}

// generateInsertStatement creates the prepared statement string.
func generateInsertStatement(schema *arrow.Schema) string {
	// Build the prepared statement string.  This avoids repeated parsing.
	stmt := "INSERT INTO t1 ("
	values := "SELECT "
	for i, field := range schema.Fields() {
		if i > 0 {
			stmt += ", "
			values += ", "
		}
		// Quote column names to handle reserved keywords like "array"
		stmt += "\"" + field.Name + "\""
		values += "\"" + field.Name + "\""
	}
	stmt += ") " + values + " FROM arrowrecs"
	return stmt
}
