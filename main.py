# -*- coding: utf-8 -*-
"""
爬取豆瓣电影 top250
存入 mongo 数据库
"""

import time
import requests
from bs4 import BeautifulSoup
from pymongo import MongoClient


URL = 'http://movie.douban.com/top250'

DB_HOST = 'localhost'
DB_PORT = 27017
DB_NAME = 'spider'
COL_NAME = 'movies'
DB_CONN = MongoClient(DB_HOST, DB_PORT)


def download_url(url):
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/69.0.3497.92 Safari/537.36'
    }
    data = requests.get(url, headers=headers).content
    return data


def parse_html(html):
    soup = BeautifulSoup(html)
    movie_list_soup = soup.find('ol', attrs={'class': 'grid_view'})

    movie_name_list = []

    for movie_li in movie_list_soup.find_all('li'):
        detail = movie_li.find('div', attrs={'class': 'hd'})
        movie_name = detail.find('span', attrs={'class': 'title'}).getText()

        movie_name_list.append(movie_name)

    next_page = soup.find('span', attrs={'class': 'next'}).find('a')
    if next_page:
        return movie_name_list, URL + next_page['href']
    return movie_name_list, None


def save_to_db(data):
    DB_CONN[DB_NAME][COL_NAME].update_one(
        filter={'_id': data['_id']},
        update={'$set': data},
        upsert=True
    )


def main():
    page = 1
    url = URL
    while url:
        html = download_url(url)
        movies, url = parse_html(html)
        for i, movie in enumerate(movies):
            data = {
                '_id': page * 1000 + i,
                'name': movie,
                'time': time.time()
                }
            save_to_db(data)
        page += 1


if __name__ == "__main__":
    main()
