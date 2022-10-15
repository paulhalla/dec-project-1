# Project plan
## Objective
### What would you like people to do with the data you have produced? Are you supporting BI or ML use-cases?

This is a data engineering pipeline that extracts, loads and trasform multiple sets of economic and financial data from the [Alpha Vantage API](https://www.alphavantage.co/documentation/). 

## Consumers
### What users would find your data useful?

The output of the pipeline could be used by data/business analysts, investment managers and other finance professionals, data and finance journalists alike, depending on the specific use case.

## Questions
### What questions are you trying to solve with your data?
What correlations are there between different economic and financial data points? This data would provide useful context for how to predict and profit from differences to inform trading related business decisions.

The data allows data analysts to investigate correlations and derive hypotheses on causation between those data points. These data points would also show the financial and economic changes coinciding with [black swan events](https://www.investopedia.com/terms/b/blackswan.asp) such as the outbreak of the COVID-19 pandemic in 2019 and Russia’s invasion of Ukraine in 2022.

## Source datasets
### What datasets are you sourcing from?

The pipeline currently contains the following datasets:

- [Foreign Exchange (FX) daily exchange rates](https://www.alphavantage.co/documentation/#currency-exchange) for different countires' currencies compared against the US Dollar. The crrent pipeline defaults to exchange rates against the AUD, EUR, JPY, RUB and GBP respectively.

- [Digital & Crypto Currencies daily exchange rates](https://www.alphavantage.co/documentation/#digital-currency) for for different digital currencies rates compared against the US Dollar. Current pipeline defaults to exchange rates against BTC, ETH and DOGE respectively.

- [US treasury yield data](https://www.alphavantage.co/documentation/#treasury-yield) for various maturity timelines. The current pipeline shows the maturify timelines for 3 months, 2 years, 5 years, 7 years and 10 years respectively.

## Breakdown of tasks
### How is your project broken down? Who is doing what?
Get a project running on Github and each works on a separate branch.  
Step 1 - Person A, B, C will write EL scripts for different datasets within Alpha Vantage (Forex daily rate, economic factors, etc).  

Step 2 - We review, plan on how to merge and transform the datasets to finish the initial ELT script.  

Step 3 - Person A and B pair program on stitching the ELT pipeline together, adding logging and creating the Dockerfile for the docker image. Person C creates the required AWS services (e.g. RDS, ECR, S3, ECS).  

Step 4 - 2 people pair program on writing unit tests, documentation, and preparing slides for the presentation. The third person works on deploying the solution to AWS.

# Instructions
## Assumptions

* The pipeline can be forked and run locally as well as run via Docker. User knows how to set up and configure RDS, S3 and access control on AWS
## Preconditions
* creation of a Postgres database on AWS
* creation of an account on Alpha Vantage and obtain the API key via filling in [the form on this page](https://www.alphavantage.co/support/#api-key).
* S3 bucket with an env file 
  * Update the env file with details - api_key, target_db_user, target_db_password, target_db_server_name and target_db_database_name.
* Set up schedule to run container (Steps?)
## Steps
1. Complete preconditions
2. Connect reporting tools to RDS
