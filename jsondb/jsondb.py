import json
import os
from typing import Dict, Any, List, Optional, Union
import uuid
from datetime import datetime
import re
import urllib.request

class JsonDB:
    def __init__(self, filename: str):
        self.filename = filename
        self.data = {}
        self._load_db()

    def _load_db(self) -> None:
        """Load data from JSON file (local or URL) if it exists, otherwise initialize empty data."""
        if self.filename.startswith('http://') or self.filename.startswith('https://'):
            try:
                with urllib.request.urlopen(self.filename) as response:
                    data = response.read().decode('utf-8')
                    self.data = json.loads(data)
            except urllib.error.URLError as e:
                raise IOError(f"Failed to fetch from URL: {e.reason}")
            except json.JSONDecodeError:
                raise ValueError("Failed to decode JSON from URL.")
        elif os.path.exists(self.filename):
            try:
                with open(self.filename, 'r') as f:
                    self.data = json.load(f)
            except json.JSONDecodeError:
                self.data = {}
        else:
            self.data = {}
            # If it's a new local file, save it to ensure it exists.
            # No need to save if it's a URL, as it's not a persistent local file.
            if not (self.filename.startswith('http://') or self.filename.startswith('https://')):
                self._save_db()

    def _save_db(self) -> None:
        """Save data to JSON file."""
        with open(self.filename, 'w') as f:
            json.dump(self.data, f, indent=4)

    def create_record_id(self):
        return str(uuid.uuid4())

    def insert(self, collection: str, record: Dict[str, Any], record_id: Optional[str] = None) -> str:
        """Insert a new record in the specified collection. If record_id is not provided, a new UUID is generated."""
        if collection not in self.data:
            self.data[collection] = {}

        if record_id is None:
            record_id = self.create_record_id()
        
        self.data[collection][record_id] = record
        self._save_db()
        return record_id

    def insert_many(self, collection: str, records: List[Dict[str, Any]], record_ids: Optional[List[str]] = None) -> List[str]:
        """Insert multiple records in the specified collection. If record_ids are not provided, new UUIDs are generated."""
        if collection not in self.data:
            self.data[collection] = {}

        if record_ids is None:
            generated_record_ids = [self.create_record_id() for _ in records]
        else:
            if len(record_ids) != len(records):
                raise ValueError("Length of provided record_ids must match length of records.")
            generated_record_ids = record_ids

        for i, record in enumerate(records):
            self.data[collection][generated_record_ids[i]] = record
        
        self._save_db()
        return generated_record_ids

    def _parse_value(self, value: str) -> Any:
        """Parse a value from string to appropriate type."""
        value = value.strip()
        if value.startswith("'") and value.endswith("'"):
            return value[1:-1]  # String literal
        try:
            return int(value)
        except ValueError:
            try:
                return float(value)
            except ValueError:
                try:
                    return datetime.fromisoformat(value)
                except ValueError:
                    return value  # Fallback to string

    def _get_nested_value(self, data: Dict[str, Any], key: str) -> Any:
        """Retrieve a value from a nested dictionary using a dot-separated key."""
        keys = key.split('.')
        value = data
        for k in keys:
            if isinstance(value, dict) and k in value:
                value = value[k]
            else:
                return None  # Key not found
        return value

    def _evaluate_condition(self, rec_data: Dict[str, Any], key: str, operator: str, value: Any) -> bool:
        """Evaluate a single condition against a record."""
        rec_value = self._get_nested_value(rec_data, key)
        if rec_value is None:
            return False

        # Handle date comparisons
        if operator in (">", "<", ">=", "<=", "==", "!=") and isinstance(value, datetime):
            try:
                rec_date = datetime.fromisoformat(rec_value)
                if operator == ">":
                    return rec_date > value
                elif operator == "<":
                    return rec_date < value
                elif operator == ">=":
                    return rec_date >= value
                elif operator == "<=":
                    return rec_date <= value
                elif operator == "==":
                    return rec_date == value
                elif operator == "!=":
                    return rec_date != value
            except (ValueError, TypeError):
                pass  # Fall back to regular comparison

        # Handle regular comparisons
        if operator == "==":
            return rec_value == value
        elif operator == "!=":
            return rec_value != value
        elif operator == ">":
            return isinstance(rec_value, (int, float)) and rec_value > value
        elif operator == "<":
            return isinstance(rec_value, (int, float)) and rec_value < value
        elif operator == ">=":
            return isinstance(rec_value, (int, float)) and rec_value >= value
        elif operator == "<=":
            return isinstance(rec_value, (int, float)) and rec_value <= value
        elif operator == "contains":
            return isinstance(rec_value, str) and isinstance(value, str) and value.lower() in rec_value.lower()
        return False

    def _tokenize_expression(self, expression: str) -> List[str]:
        """Tokenize the expression, preserving quoted strings, operators, and parentheses."""
        expression = re.sub(r'(\b(and|or|not)\b|==|!=|>=|<=|>|<|contains|\(|\))', r' \1 ', expression)
        tokens = []
        current_token = ""
        in_quotes = False
        for char in expression:
            if char == "'":
                in_quotes = not in_quotes
                current_token += char
            elif char.isspace() and not in_quotes:
                if current_token:
                    tokens.append(current_token)
                    current_token = ""
            else:
                current_token += char
        if current_token:
            tokens.append(current_token)
        return [t.strip() for t in tokens if t.strip()]

    def _parse_expression(self, tokens: List[str], rec_data: Dict[str, Any], index: int = 0) -> tuple[bool, int]:
        """Recursively parse and evaluate the expression, handling parentheses."""
        if index >= len(tokens):
            return True, index

        # Stack to manage sub-expressions
        results = []
        operators = []
        i = index

        while i < len(tokens):
            token = tokens[i].lower()

            if token == "(":
                # Parse sub-expression within parentheses
                sub_result, new_i = self._parse_expression(tokens, rec_data, i + 1)
                results.append(sub_result)
                i = new_i
                if i < len(tokens) and tokens[i] == ")":
                    i += 1
            elif token == ")":
                # End of sub-expression
                break
            elif token == "not":
                # Handle not operator
                if i + 1 < len(tokens):
                    if tokens[i + 1] == "(":
                        sub_result, new_i = self._parse_expression(tokens, rec_data, i + 2)
                        results.append(not sub_result)
                        i = new_i
                        if i < len(tokens) and tokens[i] == ")":
                            i += 1
                    else:
                        sub_result, new_i = self._parse_condition(tokens, rec_data, i + 1)
                        results.append(not sub_result)
                        i = new_i
                else:
                    return False, i + 1
            elif token in ("and", "or"):
                operators.append(token)
                i += 1
            else:
                # Parse condition
                condition_result, new_i = self._parse_condition(tokens, rec_data, i)
                results.append(condition_result)
                i = new_i

            # Evaluate AND/OR if we have enough results
            while len(results) >= 2 and operators:
                op = operators.pop(0)
                right = results.pop()
                left = results.pop()
                if op == "and":
                    results.append(left and right)
                elif op == "or":
                    results.append(left or right)

        # Final evaluation
        result = results[0] if results else True
        return result, i

    def _parse_condition(self, tokens: List[str], rec_data: Dict[str, Any], index: int) -> tuple[bool, int]:
        """Parse a single condition (key operator value)."""
        if index + 2 >= len(tokens):
            return False, index

        key = tokens[index]
        operator = tokens[index + 1]
        # Join remaining tokens for value (handles multi-word strings)
        i = index + 2
        value_tokens = []
        while i < len(tokens) and tokens[i] not in ("and", "or", "not", ")"):
            value_tokens.append(tokens[i])
            i += 1
        value = self._parse_value(" ".join(value_tokens))
        return self._evaluate_condition(rec_data, key, operator, value), i

    def read(self, collection: str, record_id: Optional[str] = None, criteria: Optional[str] = None) -> Union[Dict[str, Any], List[Dict[str, Any]]]:
        """Read records from a collection by ID, criteria expression, or all records."""
        if collection not in self.data:
            return {} if record_id else []

        if record_id:
            return self.data[collection].get(record_id, {})

        if criteria:
            tokens = self._tokenize_expression(criteria)
            results = []
            for rec_id, rec_data in self.data[collection].items():
                result, _ = self._parse_expression(tokens, rec_data)
                if result:
                    results.append({"id": rec_id, **rec_data})
            return results

        return [{"id": rec_id, **rec_data} for rec_id, rec_data in self.data[collection].items()]

    def update(self, collection: str, record_id: str, updates: Dict[str, Any]) -> bool:
        """Update a specific record in the collection."""
        if collection in self.data and record_id in self.data[collection]:
            self.data[collection][record_id].update(updates)
            self._save_db()
            return True
        return False

    def delete(self, collection: str, record_id: str) -> bool:
        """Delete a specific record from the collection."""
        if collection in self.data and record_id in self.data[collection]:
            del self.data[collection][record_id]
            if not self.data[collection]:
                del self.data[collection]
            self._save_db()
            return True
        return False

    def list_collections(self) -> List[str]:
        """List all collections in the database."""
        return list(self.data.keys())

# Example usage
if __name__ == "__main__":
    # Ensure a clean database for example usage
    db_file = "_data/database.json"
    if os.path.exists(db_file):
        os.remove(db_file)

    db = JsonDB(db_file)

    print("--- JsonDB Example Usage with Nested Search ---")

    # 1. Insert records with and without nested data
    print("\n1. Inserting various records:")
    user1_id = db.insert("users", {"name": "Alice Smith", "age": 30, "city": "New York", "joined": "2025-09-01", "contact": {"email": "alice@example.com", "phone": "111-222-3333"}})
    user2_id = db.insert("users", {"name": "Bob Johnson", "age": 25, "city": "Boston", "joined": "2025-08-15", "contact": {"email": "bob@example.com", "phone": "444-555-6666"}})
    user3_id = db.insert("users", {"name": "Charlie Brown", "age": 35, "city": "New York", "joined": "2025-09-20", "address": {"street": "Main St", "zip": "10001"}})
    user4_id = db.insert("users", {"name": "David Lee", "age": 28, "city": "Boston", "joined": "2025-07-10"})
    
    product1_id = db.insert("products", {"name": "Laptop", "specs": {"cpu": "i7", "ram_gb": 16}, "price": 1200})
    product2_id = db.insert("products", {"name": "Mouse", "specs": {"type": "wireless"}, "price": 25})
    
    print(f"  Inserted user IDs: {user1_id}, {user2_id}, {user3_id}, {user4_id}")
    print(f"  Inserted product IDs: {product1_id}, {product2_id}")

    # 2. Read all records in a collection
    print("\n2. All users:")
    print(db.read("users"))

    # 3. Read by specific record ID
    print(f"\n3. Single user by ID ({user1_id}):")
    print(db.read("users", user1_id))

    # 4. Basic criteria searches
    print("\n4. Users with age > 25 and city == 'New York':")
    print(db.read("users", criteria="age > 25 and city == 'New York'"))

    print("\n5. Users with city == 'Boston' or joined > '2025-08-01':")
    print(db.read("users", criteria="city == 'Boston' or joined > '2025-08-01'"))

    print("\n6. Users with not (age <= 30):")
    print(db.read("users", criteria="not (age <= 30)"))

    print("\n7. Users with name contains 'Smith':")
    print(db.read("users", criteria="name contains 'Smith'"))

    # 8. Nested search examples
    print("\n8. Nested Search: Users with email 'alice@example.com':")
    results = db.read("users", criteria="contact.email == 'alice@example.com'")
    print(results)
    assert len(results) == 1 and results[0]['name'] == 'Alice Smith'

    print("\n9. Nested Search: Products with CPU 'i7':")
    results = db.read("products", criteria="specs.cpu == 'i7'")
    print(results)
    assert len(results) == 1 and results[0]['name'] == 'Laptop'

    print("\n10. Nested Search: Users in zip '10001':")
    results = db.read("users", criteria="address.zip == '10001'")
    print(results)
    assert len(results) == 1 and results[0]['name'] == 'Charlie Brown'
    
    print("\n11. Nested Search with non-existent path (should be empty):")
    results = db.read("users", criteria="address.street.name == 'NonExistent'")
    print(results)
    assert len(results) == 0

    print("\n12. Combined Nested and Non-Nested criteria: Users in New York with phone '111-222-3333':")
    results = db.read("users", criteria="city == 'New York' and contact.phone == '111-222-3333'")
    print(results)
    assert len(results) == 1 and results[0]['name'] == 'Alice Smith'

    # 13. Nested Search: 3-level deep data
    print("\n13. Nested Search: 3-level deep data 'level3_value':")
    deep_data_id = db.insert("configurations", {"setting_id": "config1", "details": {"group": {"sub_group": "level3_value"}}})
    results = db.read("configurations", criteria="details.group.sub_group == 'level3_value'")
    print(results)
    assert len(results) == 1 and results[0]['setting_id'] == 'config1'

    # 14. Update a record (non-nested)
    print(f"\n14. Updating user {user1_id} age to 31:")
    db.update("users", user1_id, {"age": 31})
    print("   Updated user:", db.read("users", user1_id))

    # 15. Delete a record
    print(f"\n15. Deleting user {user2_id}:")
    db.delete("users", user2_id)
    print("   Users after deletion:", db.read("users"))

    # 16. List collections
    print("\n16. Collections:", db.list_collections())

    # 17. Insert multiple records at once
    print("\n17. Inserting multiple records at once:")
    new_users = [
        {"name": "Eve", "age": 40, "city": "London", "joined": "2025-11-01"},
        {"name": "Frank", "age": 50, "city": "Paris", "joined": "2025-11-02"},
    ]
    new_user_ids = db.insert_many("users", new_users)
    print(f"   Inserted new users with IDs: {new_user_ids}")
    print("   All users after inserting many:")
    print(db.read("users"))

    print("\n--- All example operations completed! ---")

    # Clean up the dummy database
    if os.path.exists(db_file):
        os.remove(db_file)

