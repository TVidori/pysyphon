from __future__ import annotations

import dataclasses
import pymongo
import pymongo.collection


class AbstractCollection:
    collection_name: str = None
    host: str = None
    user: str = None
    password: str = None
    database_name: str = None
    port: int = 27017

    def __init_subclass__(cls):
        # This is needed to enforce the children behaviours
        if cls.collection_name is None:
            raise TypeError(
                "Class variable 'collection_name' must be set in subclass"
            )
        if cls.host is None:
            raise TypeError("Class variable 'host' must be set in subclass")
        if cls.user is None:
            raise TypeError("Class variable 'user' must be set in subclass")
        if cls.password is None:
            raise TypeError("Class variable 'password' must be set in subclass")
        if cls.database_name is None:
            raise TypeError(
                "Class variable 'database_name' must be set in subclass"
            )

    @dataclasses.dataclass
    class Document:
        def __str__(self):
            return ", ".join([
                f"{key}: {value}" for key, value in vars(self).items()
            ])

        def to_dict(self, keep_nones: bool = False) -> dict:
            if keep_nones:
                return dataclasses.asdict(self)
            else:
                return {
                    key: value for key, value
                    in dataclasses.asdict(self).items() if value is not None
                }

        @classmethod
        def load_from_dict(cls, input_dict: dict) -> Document:
            return cls(**{
                field.name: input_dict.get(field.name)
                for field in dataclasses.fields(cls)
            })

        @classmethod
        def keys(cls) -> list[str]:
            return [field.name for field in dataclasses.fields(cls)]

    @classmethod
    def get_client_and_collection(cls) -> tuple[
        pymongo.MongoClient, pymongo.collection.Collection
    ]:
        client = pymongo.MongoClient(
            f"mongodb://{cls.user}:{cls.password}@{cls.host}:{cls.port}"
        )
        database = client.get_database(cls.database_name)
        return client, database.get_collection(cls.collection_name)

    @classmethod
    def get_document(cls, filter_dict: dict) -> Document | None:
        client, collection = cls.get_client_and_collection()
        document = collection.find_one(filter=filter_dict)

        if document is None:
            return None

        python_object = cls.Document.load_from_dict(document)
        client.close()
        return python_object

    @classmethod
    def set_attribute(
            cls,
            filter_dict: dict,
            set_dict: dict,
    ) -> None:
        client, collection = cls.get_client_and_collection()
        collection.update_one(
            filter=filter_dict,
            update={"$set": set_dict}
        )
        client.close()

    @classmethod
    def insert_document(
            cls,
            document: Document,
            save_nones: bool = False,
    ) -> None:
        client, collection = cls.get_client_and_collection()
        collection.insert_one(
            document=document.to_dict(keep_nones=save_nones)
        )
        client.close()
