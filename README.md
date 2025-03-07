# ducksoup 🦆

 Experiments with DuckDB

## ArrowDuck

POC using Bodkin to detect an Arrow schema, generate mock data with gofakeit and insert the Arrow records into DuckDB using the Arrow API.
Performance on a 13th Gen Intel i7 13700H - inserted ~74K/rows per second.

## Optimizations

We've implemented several optimizations to improve performance:

### Connection Handling

- Implemented a proper connection pool with configurable max connections
- Added proper error handling for database connections
- Used `conn.Raw()` with a callback function to safely access the driver connection
- Added connection cleanup with deferred close statements

### Data Processing

- Increased chunk size for reading data from 16KB to 64KB
- Added transaction support for batch inserts
- Properly quoted column names to handle reserved SQL keywords
- Implemented proper error handling throughout the data pipeline

### Performance Improvements

- Added prepared statements for inserts to reduce parsing overhead
- Implemented batched processing with configurable batch sizes
- Added proper memory management with record release after processing
- Used transactions to reduce commit overhead

### Current Performance Metrics

- Schema inference time: ~1.1ms
- Data generation time (100K records): ~1.8s
- Data insertion time: ~0.6s
- Total processing time: ~2.5s
- Throughput: ~170K rows/second (up from 74K rows/second)

### Key Code Improvements

- Proper error propagation instead of panic calls
- Structured connection management
- SQL injection prevention with proper quoting
- Memory management optimizations
- Improved concurrency with connection pooling

## Project Structure

The project consists of a Go application that:

1. Infers an Arrow schema from sample data
2. Generates mock data using gofakeit
3. Writes data to a JSON file
4. Reads the JSON file into Arrow records
5. Inserts the Arrow records into DuckDB using the Arrow API

## Dependencies

- [Apache Arrow](https://github.com/apache/arrow-go) - For Arrow data format support
- [Bodkin](https://github.com/loicalleyne/bodkin) - For schema inference
- [gofakeit](https://github.com/brianvoe/gofakeit) - For generating mock data
- [go-duckdb](https://github.com/marcboeker/go-duckdb) - Go driver for DuckDB
