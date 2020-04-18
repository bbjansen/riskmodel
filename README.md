## [Risk_model](/README)

This package will be used to predict the risk of costs for each prospect in the acquisition process.

### Steps fo far:

  1. Downloads the files from AWS S3 with Python SDK Boto3.
  2. Parse the file "incorporation_processes.json" from oneliner to multiple python readable lines.
  3. Separate the parsed JSON file into and multiple subfiles so that we can read it into memory.
  4. Create a mongodb locally so that we can handle the documents.


      ```bash
        $ sudo apt update
        $ sudo apt install -y mongodb
      ```

      Start mongodb: ``` service mongodb start ```
      Check if database is running: ``` service mongodb status ```

  5. Parse Mongo to DataFrame and first exploration of data fields.

