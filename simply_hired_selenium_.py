import time
import math
# import requests
from bs4 import BeautifulSoup
from selenium import webdriver
from datetime import datetime, timedelta
from selenium.webdriver.common.by import By


def scrape_page(url, max_retries=3):
    for attempt in range(max_retries):
        try:
            time.sleep(2)
            driver.get(url)
            selenium_response = driver.page_source
            soup = BeautifulSoup(selenium_response, "html.parser")
            return soup
        except Exception as e:
            attempt = attempt + 1
            print("Exception occurred on attempt", attempt + 1)
            print("Exception: ",e)
            if attempt + 1 < max_retries:
                print("Retrying...")
                time.sleep(2)  # Add a delay before retrying
                continue
            else:
                print("Max retries reached")
                return None

def get_cursor(html_soup, next_page_num):
    cursor = html_soup.find( 'a', {'class': 'chakra-link css-1wxsdwr', 'aria-label': f'page {next_page_num}'})
    if cursor:
        return cursor.get('href')
    else:
        return None

def get_job_links(job_url, list_elems):
    job_links = []
    for item_in_list in list_elems:
        job_key = item_in_list.div.get('data-jobkey')
        job_link = f'{job_url}&job={job_key}'
        job_links.append(job_link)
    return job_links


def scrape_one_page(job_url, html_soup, search_keyword_for_job, data_list):
    ul = html_soup.find_all( 'ul', id='job-list')
    li = ul[0].find_all('li', {'class': 'css-0'})
    job_links = get_job_links(job_url, li)
    # data_list = []
    for job_link in job_links:
        print(job_link)
        job = scrape_page( job_link)
        job_title = job.h2.text.strip()
        company = job.find( 'span', {'data-testid': 'viewJobCompanyName'}).text if job.find( 'span', {
            'data-testid': 'viewJobCompanyName'}) else ""
        location = job.find( 'span', {'data-testid': 'viewJobCompanyLocation'}).text if job.find( 'span', {
            'data-testid': 'viewJobCompanyLocation'}) else ""
        job_type = job.find( 'span', {'data-testid': 'viewJobBodyJobDetailsJobType'}).text if job.find( 'span', {
            'data-testid': 'viewJobBodyJobDetailsJobType'}) else ""
        salary = job.find( 'span', {'data-testid': 'viewJobBodyJobCompensation'}).text if job.find( 'span', {
            'data-testid': 'viewJobBodyJobCompensation'}) else ""

        try:
            job_qualification = job.find( 'div', {'data-testid': 'viewJobQualificationsContainer'}).find( 'ul')
            if job_qualification:
                job_qualification = list( job_qualification.strings)
                job_qualification = [x.strip() for x in job_qualification]
            else:
                job_qualification = []
        except AttributeError:
            job_qualification = []

        job_description = job.find( 'div', {'data-testid': 'viewJobBodyJobFullDescriptionContent'})
        if job_description:
            job_description = list( job.find( 'div', {'data-testid': 'viewJobBodyJobFullDescriptionContent'}).strings)
            job_description = [x.strip() for x in job_description]
            # job_description = "\n".join(job_description)
        else:
            job_description = ""

        job_posted_time = html_soup.find_all( 'span', {'data-testid': 'viewJobBodyJobPostingTimestamp'})
        job_posted_time = job_posted_time[0].find( 'span', {'data-testid': 'detailText'}).text.strip()
        # date_format = "%Y-%m-%d"
        date_format = "%Y-%m-%d"
        if "days" in job_posted_time:
            job_posted_date = datetime.now() - timedelta( days=int( job_posted_time.split()[0]))
            job_posted_dates = job_posted_date.strftime(date_format)
        elif "hours" in job_posted_time:
            job_posted_date = datetime.now() - timedelta( hours=int( job_posted_time.split()[0]))
            job_posted_dates = job_posted_date.strftime(date_format)
        else:
            job_posted_dates = datetime.now()

        data = {
            'search_keyword_for_job': search_keyword_for_job,
            'job_title': job_title,
            'job_url': job_link,
            'company': company,
            'location': location,
            'job_type': job_type,
            'salary': salary,
            'posted_on': job_posted_dates,
            'job_qualification': job_qualification,
            'job_description': job_description
        }
        data_list.append(data)
        # break


def scrape_multiple_jobs(job_titles):
    global driver
    options = webdriver.FirefoxOptions()
    options.add_argument("--headless")
    options.add_argument("--no-sandbox")
    driver = webdriver.Firefox(options=options)
    driver.maximize_window()
    data_list = []
    for item in job_titles:
        url = f'{base_url}search?q={item}&l=united+states&t=1'
        next_page = url
        driver.get(url)
        number_of_jobs = driver.find_element(By.XPATH, '//*[@id="__next"]/div/main/div/div[1]/div[4]/p')
        number_of_pages = math.ceil( int(number_of_jobs.text) / int(20))
        print( f'Number of jobs for {item}:{number_of_jobs.text}   total_page:{number_of_pages}')

        i = 1
        while next_page is not None:
            print( 'Page Number:', i, ' Page Link: ', next_page)
            soup = scrape_page(next_page)
            scrape_one_page( next_page, soup, item, data_list)
            i = i + 1
            next_page = get_cursor(soup, i + 1)
            time.sleep(1)
            
        # break
    return data_list

def simplyhired(BASE_URL, JOB_TITLES):
    global base_url
    base_url = BASE_URL
    df = scrape_multiple_jobs(JOB_TITLES)
    return df


if __name__ == "__main__":
    import pandas as pd
    base_url = "https://www.simplyhired.com/"
    # job_list = ["Data Engineer", "Machine Learning Engineer", "DevOps Engineer", "React"]
    job_list = ['Data Engineer']
    current_year = datetime.now().year
    current_month = datetime.now().month
    current_day = datetime.now().day
    df = simplyhired(base_url, job_list)
    df = pd.DataFrame(df)
    df.to_csv(f"simply_hired_{current_year}_{current_month}_{current_day}")
