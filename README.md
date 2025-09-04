# Bank Market Cap ETL Pipeline

### Overview
A Python-based ETL pipeline that automates the acquisition and processing of the world's top 10 banks by market capitalization. 
The project extracts live data from the web, converts values into GBP, EUR, and INR using exchange rate, 
and loads the results into both CSV and SQL for quarterly reporting.

### Table of Contents
- [Overview](#overview)
- [Tools & Skills](#tools--skills)
- [Features](#features)
- [Installation](#installation)
- [Usage](#usage)

### Tools & Skills
- **Python3**
- **Pandas** - data transformation
- **BeautifulSoup, Requests** - web scraping
- **SQLite** - database storage & queries
- **Logging** - progress tracking

### Features
- **Extract**: Scrapes bank market cap data from the web
- **Transform**: Converts USD into GBP, EUR, INR (2 decimal precision)
- **Load**: Saves processed data into CSV and SQL database
- **Query**: Executes SQL queries on the database table
- **Log**: Tracks pipeline progress in `code_log.txt`

### Installation
1. Clone the repository:
```commandline
git clone https://github.com/yueyue426/bank-market-cap-etl.git
cd bank-market-cap-etl
```
2. Install dependencies:
```commandline
pip install BeautifulSoup Requests
```

### Usage
Run the ETL pipeline:
```commandline
python etl_pipeline.py
```

### License
This project is licensed under the MIT License.
See the `LICENSE` file for details.

