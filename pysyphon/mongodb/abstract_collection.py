from __future__ import annotations

import copy
import dataclasses
import inspect
import typing
import sys

import pymongo
import pymongo.collection


class AbstractCollection:
    collection_name: str = None
    host: str = None
    user: str = None
    password: str = None
    database_name: str = None
    port: int = 27017
    overwrite_dict_casting: typing.Callable | None = None
    overwrite_dict_loading: typing.Callable | None = None

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
            return ", ".join(
                [
                    f"{key}: {value}" for key, value in vars(self).items()
                ]
            )

        def to_dict(
                self,
                keep_nones: bool = False,
                cast_sub_documents: bool = True,
        ) -> dict:
            if keep_nones:
                dict_ = dataclasses.asdict(self)
            else:
                dict_ = {
                    key: value for key, value
                    in dataclasses.asdict(self).items() if value is not None
                }

            if cast_sub_documents:
                for key, value in dict_.items():
                    if "psd_to_dict" in dir(value):
                        dict_[key] = value.psd_to_dict()

            return dict_

        @classmethod
        def load_from_dict(cls, input_dict: dict) -> Document:
            return cls(
                **{
                    field.name: input_dict.get(field.name)
                    for field in dataclasses.fields(cls)
                }
            )

        @classmethod
        def keys(cls) -> list[str]:
            return [field.name for field in dataclasses.fields(cls)]

        @classmethod
        def load_document_with_sub_documents(
                cls,
                input_dict: dict,
                collection_classes: dict | None = None,
        ) -> Document:
            # Tries to get the calling script classes. If this does not work,
            #  the classes can be sent directly from the calling function with:
            # dict(inspect.getmembers(
            #     sys.modules[calling_frame.__module__], inspect.isclass
            # ))
            # Might be better to pass in the classes directly
            if collection_classes is None:
                frame = inspect.currentframe()
                calling_frame = frame.f_back
                collection_classes = {
                    key: value for key, value in calling_frame.f_globals.items()
                    if inspect.isclass(value)
                }

            dict_ = copy.deepcopy(input_dict)
            dataclass_annotations = cls.__annotations__
            dataclass_annotations = {
                key: value.replace(" | None", "").strip()
                for key, value in dataclass_annotations.items()
            }
            for variable_name, variable_type in dataclass_annotations.items():
                if variable_type in collection_classes:
                    variable_class = collection_classes[variable_type]
                    if "psd_from_dict" in dir(variable_class):
                        dict_[variable_name] = variable_class.parse_obj(
                            input_dict[variable_name]
                        )

            return cls.load_from_dict(dict_)

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
    def load_document_from_dict(
            cls,
            input_dict: dict,
            collection_classes: dict | None = None,
    ) -> Document:
        # Tries to get the calling script classes. If this does not work,
        #  the classes can be sent directly from the calling function with:
        # dict(inspect.getmembers(
        #     sys.modules[calling_frame.__module__], inspect.isclass
        # ))
        # Might be better to pass in the classes directly
        if collection_classes is None:
            frame = inspect.currentframe()
            calling_frame = frame.f_back
            collection_classes = {
                key: value for key, value in calling_frame.f_globals.items()
                if inspect.isclass(value)
            }

        if cls.overwrite_dict_loading is None:
            return cls.Document.load_document_with_sub_documents(
                input_dict=input_dict,
                collection_classes=collection_classes,
            )
        else:
            return cls.overwrite_dict_loading(input_dict)

    @classmethod
    def cast_document_to_dict(
            cls,
            document: Document,
            save_nones: bool = False,
    ) -> dict:
        if cls.overwrite_dict_casting is None:
            return document.to_dict(keep_nones=save_nones)
        else:
            if save_nones is True:
                raise NotImplementedError
            return cls.overwrite_dict_casting(document)

    @classmethod
    def find_many(
            cls,
            filter_dict: dict | None = None
    ) -> list[Document]:
        client, collection = cls.get_client_and_collection()
        documents = collection.find(filter=filter_dict)

        python_objects = [
            cls.load_document_from_dict(document) for document in documents
        ]
        client.close()
        return python_objects

    @classmethod
    def find_one(
            cls,
            filter_dict: dict,
            collection_classes: dict | None = None,
    ) -> Document | None:
        # Tries to get the calling script classes. If this does not work,
        #  the classes can be sent directly from the calling function with:
        # dict(inspect.getmembers(
        #     sys.modules[calling_frame.__module__], inspect.isclass
        # ))
        # Might be better to pass in the classes directly
        if collection_classes is None:
            frame = inspect.currentframe()
            calling_frame = frame.f_back
            collection_classes = {
                key: value for key, value in calling_frame.f_globals.items()
                if inspect.isclass(value)
            }

        client, collection = cls.get_client_and_collection()
        document = collection.find_one(filter=filter_dict)

        if document is None:
            return None

        python_object = cls.load_document_from_dict(
            input_dict=document,
            collection_classes=collection_classes,
        )
        client.close()
        return python_object

    @classmethod
    def find_one_as_dict(
            cls,
            filter_dict: dict,
    ) -> dict | None:
        client, collection = cls.get_client_and_collection()
        dict_ = collection.find_one(filter=filter_dict)
        client.close()
        return dict_

    @classmethod
    def set_attribute(
            cls,
            filter_dict: dict,
            set_dict: dict,
            upsert: bool = False,
    ) -> None:
        client, collection = cls.get_client_and_collection()
        collection.update_one(
            filter=filter_dict,
            update={"$set": set_dict},
            upsert=upsert,
        )
        client.close()

    @classmethod
    def increase_attribute(
            cls,
            filter_dict: dict,
            inc_dict: dict,
    ) -> None:
        client, collection = cls.get_client_and_collection()
        collection.update_one(
            filter=filter_dict,
            update={"$inc": inc_dict}
        )
        client.close()

    @classmethod
    def push_element(
            cls,
            filter_dict: dict,
            push_dict: dict,
            upsert: bool = False,
    ) -> None:
        client, collection = cls.get_client_and_collection()
        collection.update_one(
            filter=filter_dict,
            update={"$push": push_dict},
            upsert=upsert,
        )
        client.close()

    @classmethod
    def pull_element(
            cls,
            filter_dict: dict,
            pull_dict: dict,
    ) -> None:
        client, collection = cls.get_client_and_collection()
        collection.update_one(
            filter=filter_dict,
            update={"$pull": pull_dict}
        )
        client.close()

    @classmethod
    def add_element_to_set(
            cls,
            filter_dict: dict,
            add_to_set_dict: dict,
    ) -> None:
        client, collection = cls.get_client_and_collection()
        collection.update_one(
            filter=filter_dict,
            update={"$addToSet": add_to_set_dict}
        )
        client.close()

    @classmethod
    def insert_one(
            cls,
            document: Document,
            save_nones: bool = False,
    ) -> None:
        client, collection = cls.get_client_and_collection()
        collection.insert_one(
            document=cls.cast_document_to_dict(
                document=document,
                save_nones=save_nones,
            ),
        )
        client.close()

    @classmethod
    def insert_one_if_does_not_exist(
            cls,
            document: Document,
            filter_dict: dict,
            save_nones: bool = False,
    ) -> None:
        client, collection = cls.get_client_and_collection()
        existing_document = collection.find_one(filter=filter_dict)
        if existing_document is None:
            collection.insert_one(
                document=cls.cast_document_to_dict(
                    document=document,
                    save_nones=save_nones,
                ),
            )

        client.close()

    @classmethod
    def delete_one(
            cls,
            filter_dict: dict,
    ) -> None:
        client, collection = cls.get_client_and_collection()
        collection.delete_one(
            filter=filter_dict
        )
        client.close()

    @classmethod
    def delete_many(
            cls,
            filter_dict: dict,
    ) -> None:
        client, collection = cls.get_client_and_collection()
        collection.delete_many(
            filter=filter_dict
        )
        client.close()
