
JsonDB
======

JsonDB is a lightweight, file-based JSON database for Python, providing basic CRUD (Create, Read, Update, Delete) operations and advanced querying capabilities using a simple expression language. It's ideal for small-scale applications, prototyping, or when you need a persistent data store without the overhead of a full-fledged relational database.

Features
--------

*   **File-based Storage**: Stores data in a single JSON file.
*   **Collections**: Organize your data into collections, similar to NoSQL databases.
*   **CRUD Operations**:
    *   `create(collection, record)`: Add a new record to a collection.
    *   `read(collection, record_id=None, criteria=None)`: Retrieve records by ID, by a complex criteria expression, or all records in a collection.
    *   `update(collection, record_id, updates)`: Modify an existing record.
    *   `delete(collection, record_id)`: Remove a record from a collection.
*   **Advanced Querying**: The `read` method supports a powerful criteria expression language with:
    *   Logical operators: `and`, `or`, `not`
    *   Comparison operators: `==`, `!=`, `>`, `<`, `>=`, `<=`
    *   String containment: `contains` (case-insensitive)
    *   Parentheses for grouping expressions.
    *   Automatic type parsing for integers, floats, and ISO-formatted datetimes.
*   **Collection Listing**: `list_collections()` to get a list of all available collections.

Installation
------------

JsonDB is a single-file library. Simply download `jsondb.py` and place it in your project directory.

Usage
-----

Initialize the database:

.. code-block:: python

    from jsondb.jsondb import JsonDB

    db = JsonDB("my_database.json")

Create records:

.. code-block:: python

    user1_id = db.create("users", {"name": "Alice Smith", "age": 30, "city": "New York", "joined": "2025-09-01"})
    user2_id = db.create("users", {"name": "Bob Johnson", "age": 25, "city": "Boston", "joined": "2025-08-15"})
    product_id = db.create("products", {"name": "Laptop", "price": 1200, "in_stock": True})

Read records:

.. code-block:: python

    # Read all users
    all_users = db.read("users")
    print("All users:", all_users)

    # Read a single user by ID
    alice = db.read("users", user1_id)
    print("Alice:", alice)

    # Read users matching criteria
    # Age greater than 25 AND city is 'New York'
    ny_users = db.read("users", criteria="age > 25 and city == 'New York'")
    print("NY users (age > 25):", ny_users)

    # Users in Boston OR joined after a specific date
    boston_or_recent = db.read("users", criteria="city == 'Boston' or joined > '2025-08-01'")
    print("Boston or recent users:", boston_or_recent)

    # Users whose name contains 'smith' (case-insensitive)
    smith_users = db.read("users", criteria="name contains 'Smith'")
    print("Users with 'Smith' in name:", smith_users)

    # Users NOT older than 30
    not_older_than_30 = db.read("users", criteria="not (age > 30)")
    print("Users not older than 30:", not_older_than_30)

Update records:

.. code-block:: python

    db.update("users", user1_id, {"age": 31, "status": "active"})
    print("Updated Alice:", db.read("users", user1_id))

Delete records:

.. code-block:: python

    db.delete("users", user2_id)
    print("Users after deletion:", db.read("users"))

List collections:

.. code-block:: python

    collections = db.list_collections()
    print("Collections:", collections)

Query Language Syntax
---------------------

The `criteria` parameter in the `read` method accepts a string expression.

**Operators:**

*   **Logical:** `and`, `or`, `not`
*   **Comparison:** `==`, `!=`, `>`, `<`, `>=`, `<=`
*   **String:** `contains` (case-insensitive substring check)

**Value Types:**

*   **Strings:** Enclose in single quotes (e.g., `'New York'`).
*   **Numbers:** Integers and floats (e.g., `25`, `12.5`).
*   **Dates:** ISO 8601 format strings (e.g., `'YYYY-MM-DD'`). These are automatically parsed into `datetime` objects for comparison.

**Examples:**

*   `age > 30 and city == 'London'`
*   `name contains 'john' or status == 'pending'`
*   `not (price < 100 or in_stock == False)`
*   `joined >= '2025-01-01'`
*   `(category == 'electronics' and price < 500) or (category == 'books' and rating > 4)`

Development
-----------

To run the example usage provided in `jsondb.py`:

.. code-block:: bash

    python jsondb/jsondb.py

This will create a `database.json` file in the same directory and print the results of the example operations.
