from pymarc import MARCReader
from elasticsearch import Elasticsearch
import click

INDEX = 'library'
TYPE = 'source'


@click.group()
def tool():
    pass


@tool.command(help='Gets the count of the number of books already contained in the Elasticsearch server')
@click.option('--server', default="localhost",
              help="Server of the Elastciserach instance you are attempting to connect to")
@click.option('--port', default=9200, help='Port of the Elasticsearch instance you are attempting to connect to')
def count(server, port):
    client = Elasticsearch(server)

    count = client.count(index=INDEX, doc_type=TYPE, body={"query": {"match_all": {}}})
    click.echo('Total Books in Libaray: %s' % count['count'])
    click.echo('Server: %s' % server)


@tool.command(help='Sends the index data to the Elasticsearch server ')
def send_mapping():
    client = Elasticsearch('localhost')
    mappings = {
        'mappings': {
            'source': {
                'properties': {
                    "Title": {"type": "text", 'analyzer': 'english'},
                    "Author": {"type": "text"},
                    "ISBN": {"type": "text"},

                    "Magazine Title": {"type": "text"},
                    "Magazine Issue": {"type": "text"},

                    "Volume": {"type": "text"},
                    "Edition": {"type": "text"},
                    "Publisher": {"type": "text"},
                    "Year Published": {"type": "text"},
                    # Is Date Published supposed to be something else?
                    "Date Published": {"type": "text"},

                    "Website Title": {"type": "text"},
                    "URL": {"type": "text"},

                    "Version": {"type": "text"},

                    "Database": {"type": "text"},
                    "Database Service": {"type": "text"}
                }
            }
        }
    }
    client.indices.create(index='library', body=mappings)


@tool.command(help='Sends the contents of a .marc file to the server')
@click.option('--server', default="localhost",
              help="Server of the Elastciserach instance you are attempting to connect to")
@click.option('--port', default=9200, help='Port of the Elasticsearch instance you are attempting to connect to')
def send_marc(server, port):
    client = Elasticsearch('localhost')

    # count is used to make sure we dont overwrite any ids in elasticsearc, x is just there to keep track of how many we have submitted.
    count = return_source_count(server)
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
            client.index(index='library', doc_type='source', id=count, body=data)

            x += 1
            if x % 50 == 0:
                print("Count: " + str(x))


def return_source_count(server, port):
    client = Elasticsearch(server)

    count = client.count(index=INDEX, doc_type=TYPE, body={"query": {"match_all": {}}})
    click.echo('Total Books in Libaray: %s' % count['count'])
    return count['count']


if __name__ == '__main__':
    tool()
