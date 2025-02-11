# Google AlloyDB Vector Store Example

This example demonstrates how to use [AlloyDB for Postgres](https://cloud.google.com/products/alloydb) for vector similarity search with LangChain in Go.

## What This Example Does

1. **Creates a Alloydb VectorStore:**
   - Establishes a connection to the AlloyDB database.
   - Initializes the `alloydb.PostgresEngine` object.
   - Initializes a `alloydb.VectorStore` objects using OpenAI embeddings.
   - Requires the environment variables to be set for it to work.

2. **Initializes OpenAI Embeddings:**
    - Creates an embeddings client using the OpenAI API.
    - Requires an OpenAI API key to be set as an environment variable.

3. **Adds Sample Documents:**
    - Inserts several documents (cities) with metadata into the vector store.
    - Each document includes the city name, population, and area.

4. **Performs Similarity Searches:**
    - Basic search for documents similar to "Japan".

## How to Run the Example

1. Set your Google VertexAI VERTEX_PROJECT, VERTEX_LOCATION and your environment variables (These can be found at [AlloyDB Instance](https://console.cloud.google.com/alloydb/clusters)):
   ```
   export VERTEX_PROJECT=<your vertex project>
   export VERTEX_LOCATION=<your vertex location>
   export ALLOYDB_USERNAME=<your user>
   export ALLOYDB_PASSWORD=<your password>
   export ALLOYDB_DATABASE=<your database>
   export ALLOYDB_PROJECT_ID=<your project Id>
   export ALLOYDB_REGION=<your region>
   export ALLOYDB_INSTANCE=<your instance>
   export ALLOYDB_CLUSTER=<your cluster>
   export ALLOYDB_TABLE=<your tablename>
   ```

2. Run the Go example:
   ```
   go run alloydb_vectorstore_example.go
   ```

## Key Features

- Integration of AlloyDB vectorstore with OpenAI embeddings
- Similarity search
- Adding documents into AlloyDB with pgvector extension

This example provides a practical demonstration of using vector databases for semantic search and similarity matching, which can be incredibly useful for various AI and machine learning applications.
