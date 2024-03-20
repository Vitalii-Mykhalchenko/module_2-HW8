from mongoengine import Document, StringField, ListField, ReferenceField, connect
import json
from pymongo import MongoClient
import sys


client = MongoClient(
    "mongodb+srv://")
db = client["hw_8"]
collection_authors = db["authors"]
collection_quotes = db["quotes"]

connect(db="hw_8", host="mongodb+srv://")


class Author(Document):
    fullname = StringField(required=True)
    born_date = StringField()
    born_location = StringField()
    description = StringField()


class Quote(Document):
    text = StringField(required=True)
    tags = ListField(StringField())
    author = ReferenceField(Author)


def add_data():
    with open("authors.json", "r", encoding="utf-8") as f:
        authors_data = json.load(f)

    with open("quotes.json", "r", encoding="utf-8") as f:
        quotes_data = json.load(f)

    # Збереження даних про авторів у базі даних
    for author_data in authors_data:
        author = Author(
            fullname=author_data["fullname"],
            born_date=author_data.get("born_date"),
            born_location=author_data.get("born_location"),
            description=author_data.get("description")
        )
        author.save()

    # Отримання ObjectID для кожного автора
    author_ids = {}
    for author_data in authors_data:
        author = Author.objects(fullname=author_data["fullname"]).first()
        if author:
            author_ids[author_data["fullname"]] = author.id

    # Збереження цитат у базі даних з використанням ReferenceField для авторів
    for quote_data in quotes_data:
        author_fullname = quote_data["author"]
        author_id = author_ids.get(author_fullname)
        if author_id:
            quote = Quote(
                text=quote_data["quote"],
                tags=quote_data.get("tags", []),
                author=author_id
            )
            quote.save()
        else:
            print(f"Author {author_fullname} not found.")

    client.close()


def search():
    sys.stdout.reconfigure(encoding='utf-8')
    while True:
        command = input("Enter the command in the format 'command: value': ")

        if command.startswith("name:"):
            _, full_name = command.split(":", 1)
            full_name = full_name.strip()
            try:
                # Пошук автора
                author = Author.objects.get(fullname=full_name)
                # Пошук цитат, пов'язаних зі знайденим автором за його ID
                quotes = Quote.objects(author=str(author.id))
                for quote in quotes:
                    print(quote.text)
            except Author.DoesNotExist:
                print("Author not found")

        elif command.startswith("tag:"):
            _, tag = command.split(":")
            quotes = Quote.objects(tags=tag.strip())
            for quote in quotes:
                print(quote.text)

        elif command.startswith("tags:"):
            _, tags = command.split(":")
            tags_list = [tag.strip() for tag in tags.split(",")]
            quotes = Quote.objects(tags__in=tags_list)
            for quote in quotes:
                print(quote.text)

        elif command == "exit":
            break

        else:
            print("Unknown team. Try again.")


add_data()
search()
