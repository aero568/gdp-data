# gdp-data
Extract, Transfrom and Load GDP Data

## Introduction
Complete ETL pipeline practice projetct for accessing data from a website and processing it to meet requirements.

## Project Scenario
An international firm is looking to expand its business in different countries across the world. They've raised a task for the creation of an automated script that can extract the list of all countries in order of their GDPs in billion USDs (rounded to 2 decimal places), as logged by the International Monetary Fund (IMF). Since IMF releases this evaluation twice a year, this code will be used by the organization to extract the information as it is updated.

The required data is available on the following location:

```html
https://web.archive.org/web/20230902185326/https://en.wikipedia.org/wiki/List_of_countries_by_GDP_%28nominal%29
```

The required information needs to be made accessible as a CSV file `Countries_by_GDP.csv` as well as a table `Countries_by_GDP` in a database file `World_Economies.db` with attributes `Country` and `GDP_USD_billion`.

The code should run a query on the database table to display only the entries with more than a 100 billion USD economy. Also, you should log in a file with the entire process of execution named `etl_project_log.txt`.

## Requirements
1. Automatically extract, transform and load data
2. Conversion of GDP data to billion USDs
3. GDP data should be rounded by 2 decimal places
4. GDP information as logged by International Monetary Fund (IMF)
5. Code should run a query on DB to display only entries with more than a 100 billion USD economy
6. Entire process of execution should be logged in a ASCII afile
7. Final data available in CSV and database files

### Output files

**GDP Data**
| Format | File name |
| ----------- | ----------- |
| CSV | Countries_by_GDP.csv |
| Database | World_Economies.db |

**Log file**
| Format | File name |
| ----------- | ----------- |
| ASCII | etl_project_log.txt |

### Database 

File : 
**World_Economies.db**

Table and attributes:

| Countries_by_GDP |
| ----------- |
| Country |
| GDP_USD_billion  |
