# Webserver used to run the database ingestion process.

- Makes POST request to running endpoint. Endpoint starts running the ingestion logic in a seperate thread/backgroup process or something like [JobRunr](https://www.baeldung.com/java-jobrunr-spring). This post returns the id for the job.

- The other service will ping the job status endpoint perodically. Once the status is set to finished then the other service can make a request to the status endpoint to get the result of the job.