# pysyphon



pysyphon is the package created by Sypher ([sypher.ai]()) for data loading both from external sources (marketing tools such as Salesforce or Hubspot) and into our data infrastructure (PostgreSQL, MongoDB....). While many packages for moving data exist, the goal of this package is not to work as a standalone but to be used within Python code by easing the collect and storage of data.



Work is still in progress. More precision on how to install the package and use it, guidlines for contributing and many more documentation will be added later on. 

If you have any question, do not hesitate to contact thomas@sypher.ai 



### Installing 

Run the command:

`pip install git+https://github.com/TVidori/pysyphon.git`



### A Simple Example

Creating the Python class

````python
from __future__ import annotations

import dataclasses

from pysyphon.postgresql import AbstractTable


class PersonsTable(AbstractTable):
    host = "host_ip"
    user = "username"
    password = "password"
    table_name = "persons"
    database_name = "my_database"
    primary_key_column = ["id"]

    @dataclasses.dataclass
    class Row(AbstractTable.Row):
        id: int
        first_name: str
        last_name: int

````

Loading the data into Python:

````python
data = PersonsTable.load_whole_table()
print(data)
````

Adding a list of rows in the database's table:

```python
persons = [
    {"id": 0, "first_name": "James", "last_name": "Bond"},
    {"id": 1, "first_name": "Marcus", "last_name": "Aurelius"},
]

PersonsTable.append_or_update_list_of_rows(
    rows=[PersonsTable.Row(
        id=person["id"],
        first_name=person["first_name"],
        last_name=person["last_name"],
    ) for person in persons]
)
```





