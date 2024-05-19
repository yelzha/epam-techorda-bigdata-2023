# EPAM Big Data Capstone Project Report

Prepared by Zhastay Yeltay
May 17, 2024

## Project Description

The Seed Distributor Online Store, active in the retail sector, is transitioning from an outdated system burdened with unverified geodata, leading to inefficiencies. The objective is to adopt advanced data-driven methodologies and automate processes to support business expansion and facilitate real-time analytics.

**Implemented SQL Queries:**
1. Customer Activity Map: Depicts the distribution of orders across geographic locations with real maps, aiding in the analysis of customer engagement and regional market dynamics.
2. Total Revenue by State and Metropolitanity: Assesses revenue by state and metropolitan classification, providing insights into the most lucrative regions for the business.
3. Inactivity by City: Highlights cities with minimal customer activity, which assists in pinpointing regions for marketing initiatives or to investigate service delivery issues.
4. Top-5 Cities with the Largest Number of VIP Customers: Identifies cities with significant numbers of VIP customers, essential for customizing high-end services and marketing approaches.
5. Daily Total Revenue: Monitors daily revenue, providing detailed insights into sales performance that assist in financial planning and trend analysis.

**Motivation:**
These queries were selected to effectively tackle the inefficiencies of the old system by offering crucial insights into customer behaviors, revenue patterns, and operational effectiveness. They enable the company to make informed decisions swiftly, focus on areas requiring attention, and optimize resource allocation in line with strategic objectives.

## ER Diagram

![image_2024-05-19_21-51-39](https://github.com/yelzha/epam-capstone-project/assets/54392243/9a3eafe9-52f8-4aaf-89ac-d07de0664de3)

## DataFlow Diagram

<img src='https://github.com/yelzha/epam-capstone-project/assets/54392243/4137e13d-d3f7-4ce6-ad05-4533560aa92a' >


## Layers Description

**The Bronze Layer** is the first step in the data pipeline, where raw data comes in from old systems. It automatically processes this data to integrate updates and changes into Delta tables, which are special databases that keep track of different versions of data. This layer also adds new data from sources like census.gov, which might include formats like Excel and JSON. Essentially, this layer collects all initial data without any changes, setting the stage for more detailed processing.

**The Silver Layer** takes the raw data from the Bronze Layer and improves it. It adds extra information from external sources like the Bing Maps API, which provides geographical details. This layer focuses on fixing errors and making sure the data is consistent and reliable. It also uses a Dead Letter Queue (DLQ), a system that handles data errors by setting aside incorrect data for further review. This layer acts as a bridge, refining the raw data to make it ready for deeper analysis and use in business decisions.

**The Gold Layer** is all about making the data easy to use for reporting and analysis. It organizes the cleaned and enriched data from the Silver Layer into a format that's simple to access and quick to search through, which is important for making fast decisions. This layer involves grouping data and creating summaries that highlight key information and performance indicators relevant to the business. It ensures that the data is reliable and formatted in a way that supports important strategic decisions. Essentially, the Gold Layer is where the most valuable data transformations happen, making it a key resource for any data-driven activity in the company.

## Validation Workflow Diagram

![image_2024-05-19_22-05-47](https://github.com/yelzha/epam-capstone-project/assets/54392243/044b765d-32e0-4cc3-ac59-54ec86cdf8e4)

## Data Pipeline

![image](https://github.com/yelzha/epam-capstone-project/assets/54392243/3082aad2-470d-4113-b6af-adec06a72972)

## Dashboard

![image](https://github.com/yelzha/epam-capstone-project/assets/54392243/119d3bf6-cfcc-4c5c-a4a5-b419f3716e8a)

## Takeaways/Summary/Next Steps

**Takeaways:**
* The Bronze Layer has successfully automated the initial processing of raw data, setting a strong foundation for more detailed work in the other layers.
* The Silver Layer has improved the quality of the data by fixing errors and adding more information, which helps make the data more useful.
* The Gold Layer makes the data easy to access and use for reports, which is crucial for making quick and informed business choices.

**Insights:**
* Adding geographic details from external sources like Bing Maps API and Census.gov have given more depth to the data, which helps in analyzing regional trends and planning marketing strategies.
* Using a Dead Letter Queue to handle errors has kept the data processing smooth and ensured that only clean data moves forward.

**Next Steps:**
* In the Silver Layer, we should implement 3NF (Third Normal Form) normalization specifically for the addresses tables. Right now, the address lines table has many duplicates which could be reduced since there are only 3,500 unique address lines. This would make the data cleaner and more organized.
* For the Gold Layer, we should work on optimizing the tables and developing a data warehouse model that uses the DRY (Don't Repeat Yourself) principle.
* Implementing more agile and easy CI / CD development and workflow process.
* Implementing more agile dashboards with more parametrized functions.
* We should continue to enhance the data pipeline by integrating more automated processes like real-time system.
* Introduce a new column in the Silver Layer to track updates, allowing the Gold Layer to efficiently refresh only the affected aggregated data by focusing on these changes.
* Expanding the analytical capabilities in the Gold Layer, such as using predictive analytics and machine learning, could provide deeper insights into customer preferences and future trends.



