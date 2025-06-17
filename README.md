```mermaid
flowchart TD;
    A["Webscraper<br>(Glue Job)"] --> B["S3 Bucket<br>(compressed HTML)"];
    B --> C["Data Processing<br>(Glue Job)"];
    C --> D[Redshift];
```
