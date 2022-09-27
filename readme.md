# Project plan
## Objective
### What would you like people to do with the data you have produced? Are you supporting BI or ML use-cases?
Extract and integrate multiple sets of economic and financial data to allow data analysts to investigate correlations and potentially causal factors of how exchange rates to move. This would also show the impact of major events such as the COVID pandemic and Russia’s invasion of Ukraine.
## Consumers
### What users would find your data useful?
Data/Business analysts, investment managers and other finance professionals, data and finance journalists.
## Questions
### What questions are you trying to solve with your data?
What causes exchange rates to move? This data would provide useful context for how to predict and profit from differences in exchange rate.
## Source datasets
### What datasets are you sourcing from?
Alpha Vantage - Forex daily rates, various economic indicators.
## Breakdown of tasks
### How is your project broken down? Who is doing what?
Get a project running on Github and each works on a separate branch
Step 1 - Person A, B, C will write EL scripts for different datasets within Alpha Vantage (Forex daily rate, economic factors, etc)
Step 2 - We review, plan on how to merge and transform the datasets to finish the initial ELT script.
Step 3 - Person A and B pair program on stitching the ELT pipeline together, adding logging and creating the Dockerfile for the docker image. Person C creates the required AWS services (e.g. RDS, ECR, S3, ECS).
Step 4 - 2 people pair program on writing unit tests, documentation, and preparing slides for the presentation. The third person works on deploying the solution to AWS.
