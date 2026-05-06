# AWS S3 Configuration Guide

This guide walks you through setting up AWS S3 for the West Africa Economic Data Pipeline.

## 📋 Prerequisites

- AWS Account with billing enabled
- IAM permissions to create users and S3 buckets

## 🚀 Step-by-Step Setup

### Step 1: Create an S3 Bucket

1. Go to [AWS S3 Console](https://s3.console.aws.amazon.com/)
2. Click **Create Bucket**
3. **Bucket name**: `west-africa-economic-data`
4. **Region**: `us-east-1` (or your preferred region)
5. **Block Public Access**: Keep all checkboxes enabled ✅
6. Click **Create Bucket**

### Step 2: Create IAM User for Airflow

1. Go to [AWS IAM Console](https://console.aws.amazon.com/iam/)
2. Click **Users** → **Create User**
3. **User name**: `airflow-etl-user`
4. Click **Next**
5. Click **Attach Policies Directly**
6. Search for `AmazonS3FullAccess` and select it
7. Click **Create User**

### Step 3: Generate Access Keys

1. Click on the newly created user `airflow-etl-user`
2. Go to **Security Credentials** tab
3. Click **Create Access Key**
4. Select **Application running outside AWS** → **Next**
5. Click **Create Access Key**
6. **Copy the Access Key ID and Secret Access Key** to a safe place

### Step 4: Configure Environment Variables

Edit the `.env` file in your project root:

```bash
# AWS S3 Configuration
AWS_ACCESS_KEY_ID=AKIAIOSFSFEXAMPLE      # Your Access Key ID
AWS_SECRET_ACCESS_KEY=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY  # Your Secret
AWS_DEFAULT_REGION=us-east-1
S3_BUCKET=west-africa-economic-data
```

**⚠️ SECURITY**: Never commit these credentials to Git!

### Step 5: Restart Docker Services

```bash
docker-compose down
docker-compose up -d

# Wait for services to initialize
sleep 30

# Verify Airflow scheduler is running
docker-compose logs airflow-scheduler | head -20
```

### Step 6: Trigger a DAG Run

1. Open Airflow UI: http://localhost:8080
2. Find DAG: `west_africa_economic_pipeline`
3. Click the Play button (▶️)
4. Monitor the execution in **Graph View**

### Step 7: Verify Data in S3

After the DAG completes successfully:

```bash
# List uploaded files
aws s3 ls s3://west-africa-economic-data/raw/world-bank-api/ --recursive
aws s3 ls s3://west-africa-economic-data/processed/west-africa-economic/ --recursive

# Download a sample file to inspect
aws s3 cp s3://west-africa-economic-data/raw/world-bank-api/2026/05/06/121530/data.csv ./sample-raw.csv
```

## 📊 S3 Data Structure

After successful pipeline execution, your S3 bucket will have:

```
west-africa-economic-data/
├── raw/
│   └── world-bank-api/
│       └── 2026/05/06/121530/
│           └── data.csv              # Raw extracted data
└── processed/
    └── west-africa-economic/
        └── 2026/05/06/121530/
            └── data.parquet          # Transformed data (compressed)
```

**Partitioning**: `YYYY/MM/DD/HHMMSS` allows for easy date-based queries in Athena or Spark

## 🔍 Monitor S3 Uploads

You can verify the pipeline is working by checking CloudWatch metrics or using AWS CLI:

```bash
# Get bucket size
aws s3 ls s3://west-africa-economic-data/ --recursive --summarize

# Get total objects count
aws s3api list-objects-v2 \
  --bucket west-africa-economic-data \
  --query 'length(Contents)'
```

## 🛡️ Security Best Practices

1. **Use IAM Roles** (Recommended for Production)
   - If running on EC2/ECS, use IAM role instead of access keys
   - Attach role to container: `AmazonS3FullAccess`

2. **Restrict Bucket Access**
   - Block all public access
   - Use bucket policies to restrict to specific IAM roles

3. **Enable Versioning**
   ```bash
   aws s3api put-bucket-versioning \
     --bucket west-africa-economic-data \
     --versioning-configuration Status=Enabled
   ```

4. **Enable Encryption**
   ```bash
   aws s3api put-bucket-encryption \
     --bucket west-africa-economic-data \
     --server-side-encryption-configuration '{
       "Rules": [{
         "ApplyServerSideEncryptionByDefault": {
           "SSEAlgorithm": "AES256"
         }
       }]
     }'
   ```

## 📈 Advanced: Querying with Athena

Once data is in S3, you can query it with AWS Athena (SQL on S3):

```sql
-- Create external table
CREATE EXTERNAL TABLE west_africa_economic (
    country_code VARCHAR(3),
    country_name VARCHAR(100),
    year INT,
    gdp_current_usd DOUBLE,
    population_total INT,
    ingestion_timestamp VARCHAR(50)
)
STORED AS PARQUET
LOCATION 's3://west-africa-economic-data/processed/west-africa-economic/'
PARTITION BY (year);

-- Query
SELECT country_name, year, gdp_current_usd
FROM west_africa_economic
WHERE year >= 2019;
```

## ❌ Troubleshooting

### "NoCredentialsError: Unable to locate credentials"
- Check AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY are set in `.env`
- Verify credentials are correct in IAM console
- Restart Docker: `docker-compose restart`

### "AccessDenied: Access Denied" when uploading
- Verify IAM user has `AmazonS3FullAccess` policy
- Check bucket name is correct
- Ensure bucket exists and is in the same region

### "NoSuchBucket" error
- Create the S3 bucket first (Step 1)
- Verify bucket name matches `S3_BUCKET` in `.env`

### Airflow task hangs on load_to_s3
- Check internet connectivity: `docker exec airflow-scheduler ping -c 3 s3.amazonaws.com`
- Verify AWS credentials are valid
- Check S3 bucket policy allows uploads

## 📚 Resources

- [AWS S3 Documentation](https://docs.aws.amazon.com/s3/)
- [AWS IAM Documentation](https://docs.aws.amazon.com/iam/)
- [Boto3 S3 Client Reference](https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html)
- [AWS Athena Documentation](https://docs.aws.amazon.com/athena/)

---

**Status**: ✅ AWS S3 Configuration Complete  
For questions, open an issue on GitHub.
