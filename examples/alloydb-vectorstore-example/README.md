# Google AlloyDB Vector Store Example

This example demonstrates how to use AlloyDB along with pgvector, a PostgreSQL extension for vector similarity search, with OpenAI embeddings in a Go application. It showcases the integration of langchain-go, OpenAI's API, and pgvector to create a powerful vector database for similarity searches.

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
    - Demonstrates various types of similarity searches:
      a. Basic search for documents similar to "japan".
      b. Search for South American cities with a score threshold.
      c. Search with both score threshold and metadata filtering.

## How to Run the Example

1. Set your OpenAI API key and your environment variables:
   ```
   export OPENAI_API_KEY=<your key>
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
