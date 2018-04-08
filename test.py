from elasticsearch import Elasticsearch
import json

client = Elasticsearch('localhost')
search = 'liberalism'

body = {
    'query': {
        'match': {
            'Title': search
        }
    }
}

res = client.search(index='library', body = body)
print(res)
