# 1. "job" >> "processing_path" >> "extracting_path" >> "process_comments" >> "extracting_comments" >> The "job" function is the main execution point of the code.
# It makes a request to the specified page, processes the HTML with BeautifulSoup,
# and calls the "processing_path" function.
# The "processing_path" function extracts the URLs of the articles
# and calls the "extracting_path" function for each one.
# The "extracting_path" function extracts information about the article,
# such as the author, points and age. After that, the "process_comments"
# function is called and it loops through all the comments extracting
# the information of each one using the "extracting_comments" function.


# The first block of code imports several modules,
# including pandas, requests, and BeautifulSoup.
import pandas as pd
import requests
from bs4 import BeautifulSoup
import schedule
import time


# The variable "PAGE" is defined as a string containing
# a URL for the website news.ycombinator.com.
PAGE = "https://news.ycombinator.com/"

#Then, there are several lists and empty dictionaries defined,
# including "url_list", "comments_dataset", and "articles_dataset"
url_list = []
comments_dataset = []
articles_dataset = []


# This function is a data quality testing function
# that takes in a DataFrame of comments as its input. It performs two tests:
def data_quality_tests(df_comments):
    # Checking for missing values
    missing_values = df_comments.isnull().sum()
    print("Valores faltantes: \n", missing_values)

    # Checking for duplicate values
    duplicate_values = df_comments.duplicated().sum()
    print("Valores duplicados: \n", duplicate_values)


# The function "extracting_comments" takes a single argument, "comm",
# which is a BeautifulSoup object representing a single comment block. This function
# extracts information from the comment block, including the comment ID,
# comment text, author, age, and user, and stores it in a dictionary called
# "comments_set". This dictionary is then returned.
def extracting_comments(comm):
    str_comment_id = comm.find('a', class_='togg clicky')['id']
    try:
        str_comment = comm.find('span', class_='commtext c00').get_text()
    except AttributeError:
        str_comment = ''
    str_author = comm.find('a', class_='hnuser').get_text()
    str_age = comm.find('span', class_='age')['title']

    comments_set = dict(id_comment=str_comment_id,
                        author=str_author,
                        comments=str_comment,
                        dt_created_at=str_age,
                        id_article=id_article)
    return comments_set

# The function "process_comments" takes a single argument, "soup",
# which is a BeautifulSoup object representing the entire page.
# This function finds all elements with the class "athing comtr" and uses
# the "extracting_comments" function to extract information from each of these elements.
def process_comments(soup):
    comments_blocks = soup.find_all('tr', class_='athing comtr')
    linux = []

    for comm in comments_blocks:
        comments = extracting_comments(comm)
        comments_dataset.append(comments)
        # If the comments contain the word "linux" the comment is added in a specific list.
        # The function returns the "comments_dataset" list
        if 'linux' in comments['comments']:
            linux.append(comments_dataset)

    return comments_dataset

# The function "extracting_path" takes a single argument, "ab",
# which is a BeautifulSoup object representing a single "subline" span element on the page.
# This function extracts the article path, ID, points, author and age,
# and stores it in a dictionary called "article_set". This dictionary is then returned.
def extracting_path(ab):
    # extracting article url
    str_path = ab.find_all('a')
    get_path = str_path[3].get('href')
    get_id = get_path[8:]
    str_points = ab.find('span', class_='score').get_text()
    str_user = ab.find('a', class_='hnuser').get_text()
    str_age = ab.find('span', class_='age')['title']

    # saving articles URL
    url_list.append(PAGE + get_path)

    article_set = dict(id_article=get_id,
                       url=get_path,
                       points=str_points,
                       author=str_user, age=str_age)

    articles_dataset.append(article_set)
    # saving articles URL
    url_list.append(PAGE + get_path)
    return article_set

# The function "processing_path" takes a single argument,
# "soup", which is a BeautifulSoup object representing the entire page.
# This function finds all elements with the class "subline" and uses the
# "extracting_path" function to extract information from each of these elements.
# The function returns the "articles_dataset" list.
def processing_path(soup):
    url_blocks = soup.find_all('span', class_='subline')

    for ab in url_blocks:
        url = extracting_path(ab)


# The function "job" makes the connection with the page,
# gets the page source and creates a BeautifulSoup object using the page source.
# Then it calls the functions "processing_path" and "process_comments" passing the soup object.
# The function also creates a Dataframe with the comments and articles dataset
# and save it in CSV format with the name comments.csv and linux.csv
def job():
    # Main program execution
    if __name__ == "__main__":
        result = requests.get(PAGE)
        assert result.status_code == 200, f"Obteve status {result.status_code} verifique sua conexão!"
        texto_web = result.text
        soup = BeautifulSoup(texto_web, 'html.parser')

        process_comments(soup)

        for url in url_list:
            result_comm = requests.get(url)
            texto_web_comm = result_comm.text
            soup = BeautifulSoup(texto_web_comm, 'html.parser')
            process_comments(soup)

        df_comments = pd.DataFrame(comments_dataset)

        data_quality_tests(df_comments)

        df_comments.to_csv('comments.csv', sep=',', index=False)
        linux = pd.DataFrame(comments_dataset)
        df_linux = linux.to_csv('linux.csv', sep=',', index=False)

# The schedule.every().day.at("19:00").do(job) schedule the job to execute every day at 19:00
schedule.every().day.at("19:00").do(job)

# The while loop runs indefinitely, checking for any scheduled
# tasks using schedule.run_pending() and
# delaying the next check by one second using time.sleep(1)
while True:
    schedule.run_pending()
    time.sleep(1)