
# GitHub Pull Request Data Extractor via PySpark

This tool extracts pull request data from GitHub repositories and saves it to JSON files.



## Setup

1. Install project with git

```bash
  cd path/to/your/directory
  git clone https://github.com/hellohate/Scytale-Home-Assignment.git
  cd Scytale-Home-Assignment
```
Come to your IDE and install required Python packages by running:
```bash
  pip install -r requirements.txt
```

2. Obtain a GitHub personal access token with the necessary permissions. You can create one [here](/https://github.com/settings/tokens).

3. Set up your configuration file config.py with your GitHub access token and organization name.
```bash
ACCESS_TOKEN = '<your_token>'
ORGANIZATION_NAME = '<organization_name>'
JSON_DIRECTORY = "JSON/"
PARQUET_DIRECTORY = "output/"

```
## Usage

Run the script extract.py to extract pull request data:

```bash
python main.py

```

The extracted data will be saved as JSON and parquet files in the specified output directory.


## Features

- Extracts pull request data including organization name, repository name, repository ID, repository owner, and merged timestamp.
- Handles rate limits gracefully and exits the program if the rate limit is exceeded.


