from pymarc import MARCReader
from elasticsearch import Elasticsearch
import click
import json

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
    client = Elasticsearch(server + ':' + str(port))

    book_count = client.count(index=INDEX, doc_type=TYPE, body={"query": {"match_all": {}}})
    click.echo('Total Books in Libaray: %s' % book_count['count'])
    click.echo('Server: %s' % server)


@tool.command(help='Sends the index data to the Elasticsearch server ')
@click.option('--server', default="localhost",
              help="Server of the Elastciserach instance you are attempting to connect to")
@click.option('--port', default=9200, help='Port of the Elasticsearch instance you are attempting to connect to')
def send_mapping(server, port):
    client = Elasticsearch(server + ':' + str(port))
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
                    # Instead of having both Year and Date published, we now just have Date Published
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
    client.indices.create(index=INDEX, body=mappings)


@tool.command(help='Sends the contents of a .marc file to the server')
@click.argument('file_input', type=click.File('rb'))
@click.option('--server', default="localhost",
              help="Server of the Elastciserach instance you are attempting to connect to")
@click.option('--port', default=9200, help='Port of the Elasticsearch instance you are attempting to connect to')
def marc(server, port, file_input):
    client = Elasticsearch(server + ':' + str(port))

    # book_count is used to make sure we dont overwrite any ids in elasticsearc, x is just there to keep track of how many we have submitted.
    # file_count is to get the amount of files for the progress bar
    file_count = 0
    book_count = return_source_count(server, port)

    reader = MARCReader(file_input)
    for record in reader:
        file_count += 1
    print(file_count)

    file_input.seek(0)
    reader = MARCReader(file_input)
    with click.progressbar(reader, length=file_count) as bar:
        for record in bar:
            # PyMarc adds a \ to the end of every title, so here we are stripping it
            title = record.title()
            title = title[:-1]

            data = {
                'Title': title,
                'Author': record.author(),
                'Date Published': record.pubyear(),
                'Publisher': record.publisher(),
                'ISBN': record.isbn()
            }

            client.index(index=INDEX, doc_type=TYPE, id=book_count, body=data)
            book_count += 1


@tool.command(help='Sends the contents of a .json file to the server')
@click.argument('file_input', type=click.File('rb'))
@click.option('--server', default="localhost",
              help="Server of the Elastciserach instance you are attempting to connect to")
@click.option('--port', default=9200, help='Port of the Elasticsearch instance you are attempting to connect to')
def send_json(server, port, file_input):
    client = Elasticsearch(server + ':' + str(port))
    book_count = return_source_count(server, port)

    json_data = file_input.read()
    json_data = json.loads(json_data)


    length = len(json_data['sources'])

    with click.progressbar(json_data['sources']) as bar:
        for data in bar:
            client.index(index=INDEX, doc_type='source', id=book_count, body=data)
            book_count += 1



def return_source_count(server, port):
    client = Elasticsearch(server + ':' + str(port))

    count = client.count(index=INDEX, doc_type=TYPE, body={"query": {"match_all": {}}})
    click.echo('Total Books in Libaray: %s' % count['count'])
    return count['count']


if __name__ == '__main__':
    tool()
