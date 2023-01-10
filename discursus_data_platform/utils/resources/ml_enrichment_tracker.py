import pandas as pd
import boto3
from io import StringIO

class MLEnrichmentJobTracker:

    def __init__(self):
        self.s3 = boto3.resource('s3')

        try:
            obj = self.s3.Object('discursus-io', 'ops/ml_enrichment_jobs_v2.csv')
            df_ml_enrichment_jobs = pd.read_csv(StringIO(obj.get()['Body'].read().decode('utf-8')))
        except:
            df_ml_enrichment_jobs = pd.DataFrame(None, columns = ['job_id', 'type', 'status'])
        
        self.df_ml_enrichment_jobs = df_ml_enrichment_jobs
    
    def add_new_job(self, job_id, job_type, job_status):
        # Append new job to existing list
        data_ml_enrichment_jobs = [[job_id, job_type, job_status]]
        df_new_ml_enrichment_job = pd.DataFrame(data_ml_enrichment_jobs, columns = ['job_id', 'type', 'status'])
        self.df_ml_enrichment_jobs = self.df_ml_enrichment_jobs.append(df_new_ml_enrichment_job)
    
    def remove_completed_job(self, l_job_indexes):
        # Append new job to existing list
        self.df_ml_enrichment_jobs = self.df_ml_enrichment_jobs.drop(l_job_indexes)
    
    def upload_job_log(self):
        csv_buffer = StringIO()
        self.df_ml_enrichment_jobs.to_csv(csv_buffer, index = False)
        self.s3.Object('discursus-io', 'ops/ml_enrichment_jobs_v2.csv').put(Body=csv_buffer.getvalue())