<img width="1500" height="800" alt="image" src="https://github.com/user-attachments/assets/3f8f966a-be50-4669-b817-fbb96b5f239f" />

# 🎬 Automated Movie Analytics Pipeline Project

**TMDB + AWS End-to-End Data Engineering & Analytics Pipeline**

## Introduction

I built this project to explore what a fully automated, serverless data pipeline looks like in the real world. Using the TMDB API as the data source, I designed a system that fetches movie data every day, cleans and transforms it, and turns it into insights you can query through Athena or visualize on dashboards. Everything is event-driven, cost-efficient, and runs comfortably under the AWS free tier.

The pipeline combines **Python**, **AWS Lambda**, **S3**, **Athena**, and **EventBridge**, forming a simple but reliable ETL workflow that takes raw JSON and transforms it into analytical datasets.

## What I Learned

A lot of the learning happened around serverless design and understanding how to structure an ETL workflow without relying on heavy tools like AWS Glue. I implemented custom transformation logic, data quality checks, and feature engineering while keeping everything lightweight and scalable.

Some highlights include:

- Building Lambda functions that handle extraction, cleaning, feature engineering, and loading
- Using **ThreadPoolExecutor** to speed up API calls without breaking TMDB’s rate limits
- Designing a cost-optimized storage layout in S3 so Athena queries stay fast and affordable
- Setting up CloudWatch logs and alerts so the pipeline can run independently and notify me when something goes wrong

These pieces helped me understand how a real cloud pipeline works end-to-end.

## Challenges and How I Solved Them

One of the early challenges was handling TMDB API rate limits. I added exponential backoff, retry logic, and parallel fetching to keep the process fast but safe. Another challenge was dealing with messy or incomplete movie data. I used:

- **Median imputation** for skewed values like budget and revenue
- **Mean imputation** for more stable, normally distributed features
- Custom validations to prevent corrupted rows from entering the processed dataset

Cost optimization also shaped many design decisions. By replacing Glue with Lambda and using Athena instead of Redshift, the pipeline stayed both fast and extremely affordable.

Automation was another key part: EventBridge triggers the pipeline daily, and CloudWatch monitors everything so the workflow is self-sustaining.

## Insights From the Data

My comprehensive analysis of movie data revealed fascinating industry trends:

### 1. **Financial Performance Patterns**

- Identified optimal budget ranges that correlate with highest ROI
- Discovered that movies with budgets between $50M-$150M show the most consistent profitability
- Uncovered seasonal trends in movie releases and their impact on revenue

### 2. **Genre and Language Distribution**

- Analyzed the dominance of English-language films while identifying emerging markets
- Discovered genre preferences and their correlation with commercial success
- Mapped production company influence on movie performance metrics

### 3. **Rating and Popularity Correlations**

- Found strong correlations between vote counts, ratings, and commercial success
- Identified that movies with 1000+ votes tend to have more stable long-term performance
- Discovered optimal release timing patterns for maximizing audience engagement

## Technology Stack Overview

### **Programming Languages**

- **Python:** Primary language for Lambda functions, data processing, and ETL pipeline development
- **SQL:** Advanced querying and analytics on Amazon Athena

### **AWS Cloud Services**

- **Amazon S3 (Storage):** Scalable object storage for raw and processed movie data with intelligent partitioning
- **AWS Lambda (Compute):** Serverless functions for data extraction, transformation, and loading operations
- **Amazon EventBridge (Orchestration):** Automated scheduling and event-driven pipeline triggers
- **Amazon CloudWatch (Monitoring):** Comprehensive logging, metrics collection, and automated alerting
- **Amazon Athena (Analytics):** SQL-based querying engine for analyzing processed data directly from S3
- **AWS IAM (Security):** Fine-grained access control and service permissions management

### **External APIs**

- **TMDB API:** The Movie Database API for comprehensive movie data extraction

### **Visualization & Analytics**

- **Amazon QuickSight:** Interactive dashboards and business intelligence reporting
<img width="778" height="676" alt="Movie_Analytics_Dashboard" src="https://github.com/user-attachments/assets/becc6436-ed31-44ce-8977-70689b227e38" />

## 🏗 Architecture Overview

```
TMDB API → AWS Lambda (Extract) → S3 (Raw Data)
    ↓
EventBridge (Scheduler) → Lambda (Transform) → S3 (Processed Data)
    ↓
AWS Athena (Query Engine) → Amazon QuickSight (Visualization)
    ↓
CloudWatch (Monitoring & Alerts)
```
<img width="551" height="536" alt="Architecture_Overview" src="https://github.com/user-attachments/assets/1760f3df-1ed0-46b3-ae92-b677c6c20f35" />

## 🔄 Pipeline Workflow

### **Phase 1: Data Extraction**

- **API Integration:** Automated daily extraction from TMDB API with intelligent pagination
- **Parallel Processing:** Concurrent API calls using ThreadPoolExecutor for optimal performance
- **Error Handling:** Comprehensive retry logic with exponential backoff for reliability

### **Phase 2: Data Transformation**

- **Statistical Imputation:** Smart missing value handling using appropriate statistical measures
- **Feature Engineering:** Automated calculation of ROI, profit margins, and popularity categories
- **Data Quality Assurance:** Duplicate detection, data validation, and consistency checks

### **Phase 3: Data Loading**

- **Optimized Storage:** Intelligent S3 partitioning with date-based file organization
- **Schema Management:** Automated table creation and maintenance in Athena
- **Query Optimization:** Efficient data structures for fast analytical queries

### **Phase 4: Automation & Monitoring**

- **Scheduled Execution:** EventBridge-powered daily pipeline runs
- **Proactive Monitoring:** CloudWatch alerts for failures and performance issues
- **Self-Healing Mechanisms:** Automated retry and recovery procedures

## 💰 Cost Optimization

One of the goals of this project was to show that you can build a reliable, enterprise-style data pipeline without spending money on heavy infrastructure. By designing everything around serverless services and efficient data formats, the entire workflow stays comfortably within AWS free-tier limits.

To give a clearer picture:

- **Lambda:** 1M free requests/month - each daily run uses fewer than 50 requests
- **S3:** 5GB free storage - clean partitioning and compact files keep storage extremely low
- **Athena:** 1TB free queries/month - optimized schemas help minimize scanned data
- **EventBridge:** 1M free events/month - perfect for lightweight daily scheduling
- **CloudWatch:** 5GB free logs/month - tuned logging levels prevent unnecessary usage

Together, these choices make the pipeline inexpensive, scalable, and ideal for experimentation or portfolio projects.

## 📝 Future Enhancements

There are several exciting directions this project can grow into. These additions would make the pipeline more powerful, more flexible, and closer to what large-scale data teams use in production:

- **Real-time Streaming:** Incorporate Kinesis to process movie data as it arrives
- **Machine Learning Integration:** Use SageMaker to build predictive models (e.g., revenue or rating forecasts)
- **Enhanced Visualizations:** Expand dashboards with richer analytics and trend exploration
- **Data Governance:** Add data cataloging and lineage tracking for better transparency and organization
- **Multi-Source Integration:** Pull data from other movie platforms to enrich the dataset and enable deeper insights

## 🤝 Contributing

Contributions are welcome! Please feel free to submit pull requests or open issues for bugs and feature requests.

## 📄 License

This project is licensed under the MIT License - see the LICENSE file for details.
