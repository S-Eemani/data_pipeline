# data_pipeline

I have written this code as part of my internship. The primary objective of the code is to decode data fetched from an API, perform local installation, and then upload it to an S3 bucket before deletion. This process ensures smooth data handling and storage. As part of the code's functionality, it incorporates a smart verification mechanism while uploading to the S3 bucket. The code intelligently checks whether the file already exists in the bucket and validates if the contents are identical. Based on these evaluations, the logic dynamically adjusts to accommodate the specific conditions, ensuring data integrity and avoiding unnecessary duplication.

Moreover, I successfully integrated the Python code with Apache Airflow, enabling the creation of a powerful "data pipeline." This well-designed pipeline automates the entire process and executes it on a daily basis. The pipeline efficiently handles data processing, including decoding, local installation, S3 upload, and cleanup, all on the AWS EC2 server. Through this internship project, I've gained hands-on experience in API data retrieval, AWS services, Python coding, and data pipeline implementation. It has been an exciting journey that has not only honed my technical skills but also highlighted the significance of streamlined data workflows in real-world applications.

The code is organised as below:
1. Airflow driver code (which defines the UI), is in the [data_pipeline.py](https://github.com/S-Eemani/data_pipeline/blob/main/data_pipeline.py) file.
2. All the dependency codes are orgnaised in the [pipeline_util](https://github.com/S-Eemani/data_pipeline/tree/main/pipeline_utils) folder.
