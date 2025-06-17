graph TD
    A[Webscraper (Glue Job)] --> B[S3 Bucket (compressed HTML)]
    B --> C[Data Processing (Glue Job)]
    C --> D[Redshift]
