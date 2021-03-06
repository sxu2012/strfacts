# strfacts: Short Term Rental Facts

- step2: strfacts.pdf: - project proposal. The link to the google doc of this file: https://docs.google.com/document/d/1sKcd7yKwLKhX0pL9q2HJHQFdp04mFMjZDKirUjQY7tM/edit
- step 4: dataexplor.ipynb/dataexplor.html: - Data Exploration. Explore data using Jupyter Notebook with Spark on local PC with Windows 11.
- step 5: local\data_aquisition, local\data_ingestions, local\data_analytics - Prototype Data Pipeline. Prototyped the data pipeline with a sample data set on the local Windows 11 machine. Focused on the reviews for each property listing. Gathered all the reviews for the same listing, then, extract out the most frequent used words from the reviews. Actual useage: people can look at the most frequent words to get an idea of the properties quickly without reading all the lengthy reviews.
- step 6: azure\data_aquisition, azure\data_ingestion, azure\data_analitics - Scale the data pipeline to run on Azure Databricks on a full data set.
- step 7: strfacts_presentation.pdf, azure\ExportedTemplate-strfactsRG.zip - Added a Power Point presentation with deployment architechture diagrams, screenshots for Databricks Spark Clucsters and files/folders on Blob storage after data acquisition, data ingestion, and data analytics steps.
- step 8: unittest\test_data_acquisition.py, unittest\test_data_ingestion.py, unittest\test_data_analytics.py unittest\output_with_code_coverate.txt - unit tests with pytest
- step 9: deployment\azure, deployment\local - Converted Jupyter Notebooks to python script files, unit tested and deployed to Azure. Built the data pipeline with Databricks Jobs orchestraction tool. Experimented with different Databricks spark clusters and Azure subscriptions to get the tasks run to finish at a reasonalbe amount of time. 
- step 10: Monitor the data pipeline and spark cluster with Databricks Ganlia UI. Updated strfacts_presentation.pdf with screenshots of the actual run of the data pipeline.
