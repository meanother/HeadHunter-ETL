import requests


class FetchHHVacancy:

    def __init__(self, text):
        self.headers = {'User-Agent': 'ETL Pipeline exmaple@mail.com'}
        self.main_url = 'https://api.hh.ru/vacancies/'
        self.text = text
        self.per_page = 100
        self.page = 1

    def get_total_number_of_vanacy(self):
        url = f'{self.main_url}?text="{self.text}"&per_page={self.per_page}&page={self.page}'
        req = requests.get(url, headers=self.headers)
        page = req.json()['pages']
        # print(req.json()['pages'])
        return page

    def fetch_all_results(self):
        array = []

        def fetch_single_result(page):
            url = f'{self.main_url}?text="{self.text}"&per_page={self.per_page}&page={page}'
            req = requests.get(url, headers=self.headers)
            result = req.json()['items']
            # array.append(result)
            for i in result:
                array.append(i)
                # print(i)

        count = self.get_total_number_of_vanacy()
        for i in range(count):  # TODO need to rollback
            # for i in range(3):
            fetch_single_result(i)
        # print(len(array))
        return array
