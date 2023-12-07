# Analysis On Law Enforcement 911 Dispatched Calls For Service 
 
### This repository is for data 225 group project

Using cloud-based AWS services like S3 Bucket, Lambda Function, VPC and Snowflake for data storage, processing, and analytics, this project aims to build a data warehouse that will store and handle historical and real-time 911 call data from San Francisco. 
Apache airflow,DBT is used to put together the whole data link.Data visualization with in-depth analysis is used to find trends and patterns in dispatched calls that  provide solutions to improve public safety, response methods and time,effectiveness of law enforcement operations, insight into how this might improve police, fire, and medical services and offer data-driven recommendations for making communities safer.
 
## Link for the dataset
https://datasf.gitbook.io/datasf-dataset-explainers/law-enforcement-dispatched-calls-for-service

### Dataset Analysis
This dataset contains a large amount of information gathered by the city of San Francisco, including details on every police dispatched call.
It includes the caller's phone number, the day and time the call was received, the location of the incident, and the priority level of the incident. 

### Content
•	Closed Call.csv: San Francisco's  911 emergency call data, which includes records from March 2016 to the present date which are updated frequently.Each entry here shows a distinct emergency service call that was made. Details such as the call's priority, location, dispatch, and arrival time are kept track of.

•	Real-Time.csv: This set of data is updated every 10 minutes with the most recent 48-hour window time of dispatched calls, both open and stopped. In particular, it covers unresolved calls that have been open for more than 48 hours, giving an in-depth analysis of critical responses.

## Architecture Flow









### Architecture Flow

![Architecture Flow](https://github.com/revanthkumar1999/Analysis-on-UK-Traffic-Accidents/blob/main/ELT%20Flow/Architecture%20Flow.png?raw=true)
