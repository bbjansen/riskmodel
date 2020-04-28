## Description

This package will be used to predict the risk of costs for each prospect in the acquisition process.

## Installation

1. Clone the repository.
2. Create a virtual environment by running the next command, or by creating your own virualenv using your preferred tool:

```bash
$ pip install pipenv
```

3. Install the dependencies in the Pipfile and load virualenv:

```bash
$ make install
$ pipenv shell
```

4. Install the project in editable/develop mode:

```bash
$ make dev
```

5. Install and initiate mongodb locally:

```bash
$ make mongo_start
```

## Modules fo far:

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

5. Parse incorporation_processes documents to pandas dataFrame.

