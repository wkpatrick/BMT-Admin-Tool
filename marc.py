from pymarc import MARCReader
from elasticsearch import Elasticsearch

client = Elasticsearch('localhost')
x = 0
with open('BooksAll.part01.marc8', 'rb') as books:
    reader = MARCReader(books)
    for record in reader:
        data = {
            'Title': record.title(),
            'Author': record.author(),
            'Year': record.pubyear(),
            'Publisher': record.publisher(),
            'ISBN': record.isbn()
        }
        test = record.as_json()
        client.index(index='library', doc_type='source', id=x, body=data)

        x += 1
        if x % 50 == 0:
            print("Count: " + str(x))
