Step by Step Process:
1. Create IAM user to be able to read the S3 files in databricks.
2. API Gateway is created to get access to webhooks.
3. Create IAM role to be able to use in API Gateway and lambda function and S3. we need cloudwatch and S3 full access  in lambda; so we connect API gateway and S3 using lambda
4. Lambda gets triggered when new API Gateway logs come in.
5. Files store in S3.
6. Use Databricks to read files from S3. 
7. Dashboards can be created directly in Databricks. But for this project, we used Streamlit app.


<img width="509" height="245" alt="Data Pipeline" src="https://github.com/user-attachments/assets/41aba0e4-2aa6-4deb-8985-6a280def294c" />
