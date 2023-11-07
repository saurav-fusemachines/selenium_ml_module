import numpy as np
import pandas as pd
from datetime import datetime
from skill_extraction import generate_output_csv
from simply_hired_selenium_ import simplyhired


def data_manipulation(df):
    # Mapping the department and roles according to job_title
    df['department'] = np.select(
        [
            df['search_keyword_for_job'].str.lower().str.contains('data engineer'),
            (df['search_keyword_for_job'].str.lower().str.contains('backend') |
             df['search_keyword_for_job'].str.lower().str.contains('frontend') |
             df['search_keyword_for_job'].str.lower().str.contains('front-end') |
             df['search_keyword_for_job'].str.lower().str.contains('fullstack') |
             df['search_keyword_for_job'].str.lower().str.contains('full') |
             df['search_keyword_for_job'].str.lower().str.contains('back-end')),
            df['search_keyword_for_job'].str.lower().str.contains('devops'),
            (df['search_keyword_for_job'].str.lower().str.contains('ios') |
             df['search_keyword_for_job'].str.lower().str.contains('android')),
            (df['search_keyword_for_job'].str.lower().str.contains('qa') |
             df['search_keyword_for_job'].str.lower().str.contains('quality assurance') |
             df['search_keyword_for_job'].str.lower().str.contains('qc')),
            (df['search_keyword_for_job'].str.lower().str.contains('ml') |
             df['search_keyword_for_job'].str.lower().str.contains('machine learning')),
            (df['search_keyword_for_job'].str.lower().str.contains('ui/ux') |
             df['search_keyword_for_job'].str.lower().str.contains('graphic design') |
             df['search_keyword_for_job'].str.lower().str.contains('design')),
            df['search_keyword_for_job'].str.lower().str.contains('project manager')
        ],
        [
            'Data Engineering',
            'Service Engineering',
            'DevOps',
            'Service Engineering',
            'QC',
            'AI Services',
            'Design',
            'PMO'
        ],
        default=df['search_keyword_for_job']
    )

    df['roles'] = np.select(
        [
            (df['search_keyword_for_job'].str.lower().str.contains('backend') |
             df['search_keyword_for_job'].str.lower().str.contains('back-end') |
             df['search_keyword_for_job'].str.lower().str.contains('back end')),
            df['search_keyword_for_job'].str.lower().str.contains('frontend'),
            df['search_keyword_for_job'].str.lower().str.contains('data engineer'),
            (df['search_keyword_for_job'].str.lower().str.contains('ml') |
             df['search_keyword_for_job'].str.lower().str.contains('machine learning')),
            (df['search_keyword_for_job'].str.lower().str.contains('fullstack') |
             df['search_keyword_for_job'].str.lower().str.contains('full-stack') |
             df['search_keyword_for_job'].str.lower().str.contains('full stack')),
            df['search_keyword_for_job'].str.lower().str.contains('devops'),
            df['search_keyword_for_job'].str.lower().str.contains('project manager'),
            df['search_keyword_for_job'].str.lower().str.contains('UI'),
            (df['search_keyword_for_job'].str.lower().str.contains('qa') |
             df['search_keyword_for_job'].str.lower().str.contains('quality assurance') |
             df['search_keyword_for_job'].str.lower().str.contains('qc')),
            (df['search_keyword_for_job'].str.lower().str.contains('ios') |
             df['search_keyword_for_job'].str.lower().str.contains('android')),
            df['job_title'].str.lower().str.contains('Big data')
        ],
        [
            'Backend',
            'Frontend',
            'Data Engineer',
            'Machine Learning Engineer',
            'Fullstack',
            'DevOps',
            'Project Manager',
            'UI/UX',
            'QC',
            'Mobile',
            'Big Data'
        ],
        default=df['search_keyword_for_job']
    )

    # Convert the job_posted_date column to datetime format
    df['posted_on'] = pd.to_datetime(df['posted_on'], format='%Y-%m-%d', errors='coerce')

    #Convert the job_posted_date column to yyyy-mm-dd format if it is in mm/dd/yyyy format
    # df.loc[df['posted_on'].notnull(), 'posted_on'] = df['posted_on'].dt.strftime('%m/%d/%Y')
    df['posted_on'] = df['posted_on'].apply(lambda x: x.strftime('%m/%d/%Y') if x is not None else '')


    df['posted_on'] = pd.to_datetime(df['posted_on'], errors='coerce')
    has_timestamp = df['posted_on'].dt.time.any()

    if has_timestamp:
        df['posted_on'] = df['posted_on'].dt.date    

    return df

if __name__ =='__main__':

    base_url = "https://www.simplyhired.com/"
    job_list = ['Data Engineer', 'Backend Developer', 'UI/UX', 'DevOps Engineer',
                  'Front-end Developer', 'Full-stack Developer',
                  'Machine Learning Engineer', 'AI Researcher']
    # job_list = ['Data Engineer']

    current_year = datetime.now().year
    current_month = datetime.now().month
    current_day = datetime.now().day

    df = simplyhired(base_url, job_list)
    df = pd.DataFrame(df)
    df = data_manipulation(df)
    df.to_csv(f"./inputs/simply_hired_{current_year}_{current_month}_{current_day}.csv",index=False)
    
    #Extract Skills
    generate_output_csv(f'./inputs/simply_hired_{current_year}_{current_month}_{current_day}.csv')

