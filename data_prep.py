import json
from flatten_dict import flatten
import pandas as pd
from dataclasses_json import dataclass_json
from dataclasses import dataclass
import requests
from concurrent.futures import ThreadPoolExecutor
from random import sample
import os

@dataclass_json
@dataclass
class Entity:
    id: str
    name: str


@dataclass_json
@dataclass
class Actor:
    id: str
    image: str
    name: str
    asCharacter: str


@dataclass_json
@dataclass
class KeyValue:
    key: str
    value: str


@dataclass_json
@dataclass
class BoxOffice:
    budget: str
    openingWeekendUSA: str
    grossUSA: str
    cumulativeWorldwideGross: str


@dataclass_json
@dataclass
class Title:
    id: str
    title: str
    year: str
    releaseDate: str
    runtimeMins: str
    directorList: list[Entity]
    writerList: list[Entity]
    starList: list[Entity]
    actorList: list[Actor]
    genreList: list[KeyValue]
    companyList: list[Entity]
    countryList: list[KeyValue]
    languageList: list[KeyValue]
    contentRating: str
    imDbRating: str
    imDbRatingVotes: str
    boxOffice: BoxOffice

    def __post_init__(self):
        self.directorList = [Entity.from_dict(i) for i in self.directorList]
        self.writerList = [Entity.from_dict(i) for i in self.writerList]
        self.starList = [Entity.from_dict(i) for i in self.starList]
        self.actorList = [Actor.from_dict(i) for i in self.actorList]
        self.companyList = [Entity.from_dict(i) for i in self.companyList]
        self.genreList = [KeyValue.from_dict(i) for i in self.genreList]
        self.countryList = [KeyValue.from_dict(i) for i in self.countryList]
        self.languageList = [KeyValue.from_dict(i) for i in self.languageList]
        self.boxOffice = BoxOffice.from_dict(self.boxOffice)


def get_movies(
        base_url: str,
        key: str
) -> list[dict]:
    url = f'{base_url}/Top250Movies/{key}'

    payload = {}
    headers = {}

    response = requests.request("GET", url, headers=headers, data=payload)
    response.raise_for_status()

    return response.json()['items']


def get_titles_raw(
        movies: list[dict],
        base_url: str,
        key: str
) -> list[dict]:
    url = f'{base_url}/Title/{key}'

    with ThreadPoolExecutor() as executor:
        urls = [f'{url}/{i["id"]}' for i in sample(movies, 80)]

        titles_raw = executor.map(requests.get, urls)

    return [i.json() for i in titles_raw]


def parse_titles(
        titles_raw: list[dict]
) -> list[Title]:
    return [Title.from_dict(i) for i in titles_raw]


def main():
    base_url: str = "https://imdb-api.com/en/API"
    key = "k_3n3jzv1v"

    skip = os.path.isfile('cache.json')

    if not skip:
        movies = get_movies(base_url, key)
        titles_raw = get_titles_raw(movies, base_url, key)
        with open('cache.json', 'w') as f:
            json.dump(titles_raw, f, ensure_ascii=True, indent=4)
    else:
        with open('cache.json', 'r') as f:
            titles_raw = json.load(f)

    titles = parse_titles(titles_raw)

    titles = [flatten(Title.to_dict(i), reducer='underscore') for i in titles]

    print(json.dumps(titles, indent=4))


if __name__ == '__main__':
    main()
