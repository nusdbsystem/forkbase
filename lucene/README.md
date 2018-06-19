# Provide RESTful service for data indexing and searching using Apache Lucene

## Installation

1. Run "mvn clean package" under lucene directory.
2. Run "java -jar target/LuceneJAR-0.0.1-SNAPSHOT.jar" to start the service.

## RESTful API

1. Index
```
URI         : host:8080/index
HTTP Method : Post
Request Body: JSON string
              {
                "dir"    : "<path_of_files_to_be_indexed>",
                "dataset": "<dataset_name>",
                "branch" : "<branch_name"
              }
Response    : JSON string with fields
                status - 0: successful; 1: failed
                msg    - error message
```

2. Search
```
URI         : host:8080/search
HTTP Method : Get
Request Body: JSON string
              {
                "query"  : "<lucene_query_string>",
                "dataset": "<dataset_name>",
                "branch" : "<branch_name>"
              }
Response    : JSON string with fields
                status - 0: successful; 1: failed
                msg    - error message
                docs   - list of matched result
```

## Usage

1. Put files to be indexed under lucene/file_path
2. Call index API with parameter "file_path", dataset name and branch name
