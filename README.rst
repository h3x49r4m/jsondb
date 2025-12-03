=======
JsonDB
=======

JsonDB is a lightweight, file-based database for Python that stores data in JSON format. It's designed for simplicity and ease of use, making it ideal for small projects, prototyping, or as an embedded database. It supports basic CRUD operations, complex queries, and can load data from local files or remote URLs.

Features
--------

- **Simple & Lightweight**: No server or complex setup required.
- **File-Based**: Stores data in a human-readable JSON file.
- **CRUD Operations**: `insert`, `insert_many`, `read`, `update`, `delete`.
- **Flexible Data Loading**: Load data from a local file or a remote URL.
- **Powerful Querying**: Supports complex criteria with `and`, `or`, `not`, parentheses, and `contains`.
- **Nested Data Search**: Query nested JSON objects using dot notation.
- **Automatic Record IDs**: Generates UUIDs for new records if no ID is provided.

Installation
------------

.. code-block:: bash

    pip install jsondb-python

Quick Start
-----------

.. code-block:: python

    from jsondb.jsondb import JsonDB

    # Initialize the database
    db = JsonDB("_data/database.json")

    # Insert a record
    user_id = db.insert("users", {"name": "Alice", "age": 30})
    print(f"Inserted user with ID: {user_id}")

    # Read all records in a collection
    all_users = db.read("users")
    print("All users:", all_users)

    # Read a specific record by ID
    alice = db.read("users", record_id=user_id)
    print("Read user by ID:", alice)

    # Update a record
    db.update("users", user_id, {"age": 31})
    print("Updated user:", db.read("users", user_id))

    # Delete a record
    db.delete("users", user_id)
    print("Users after deletion:", db.read("users"))

Querying Data
-------------

The `read` method supports a powerful `criteria` parameter for filtering records.

**Basic Queries**

You can filter records based on simple conditions:

.. code-block:: python

    # Find users with age greater than 25
    users_over_25 = db.read("users", criteria="age > 25")

    # Find users in New York
    users_in_ny = db.read("users", criteria="city == 'New York'")

**Nested Data Search**

JsonDB supports querying nested JSON objects using dot notation (`.`).

.. code-block:: python

    # Insert a record with nested data
    db.insert("users", {
        "name": "John Doe",
        "contact": {
            "email": "john.doe@example.com",
            "address": {
                "city": "New York",
                "zip": "10001"
            }
        }
    })

    # Search by nested email
    john_doe = db.read("users", criteria="contact.email == 'john.doe@example.com'")
    
    # Search by deeply nested city
    users_in_ny = db.read("users", criteria="contact.address.city == 'New York'")


**Advanced Queries**

You can build complex queries using logical operators (`and`, `or`, `not`) and parentheses for grouping:

.. code-block:: python

    # Users older than 25 in New York
    results = db.read("users", criteria="age > 25 and city == 'New York'")

    # Users in Boston or older than 30
    results = db.read("users", criteria="city == 'Boston' or age > 30")

    # Users not in Boston
    results = db.read("users", criteria="not city == 'Boston'")

    # Users whose name contains "Smith" (case-insensitive)
    results = db.read("users", criteria="name contains 'Smith'")

    # Grouped conditions
    results = db.read("users", criteria="(age > 25 and city == 'New York') or name == 'Bob'")


API Reference
-------------

**`JsonDB(filename: str)`**

- `filename`: The path to the local JSON file or a URL to a remote one.

**`insert(collection: str, record: Dict, record_id: Optional[str] = None) -> str`**

- Inserts a single record into a collection. If `record_id` is not provided, a new UUID is generated.

**`insert_many(collection: str, records: List[Dict], record_ids: Optional[List[str]] = None) -> List[str]`**

- Inserts multiple records. If `record_ids` are not provided, new UUIDs are generated for each record.

**`read(collection: str, record_id: Optional[str] = None, criteria: Optional[str] = None) -> Union[Dict, List[Dict]]`**

- Reads a specific record by `record_id` or filters records by `criteria`. If neither is provided, it returns all records in the collection.

**`update(collection: str, record_id: str, updates: Dict) -> bool`**

- Updates a specific record with the provided `updates`.

**`delete(collection: str, record_id: str) -> bool`**

- Deletes a specific record from a collection.

**`list_collections() -> List[str]`**

- Returns a list of all collections in the database.

License
-------

This project is licensed under the MIT License. See the `LICENSE` file for details.