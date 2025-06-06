import pytest
import sqlite3
import os
import pandas as pd

# path to SQL query directory
SQL_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "sql"))
print(SQL_DIR)


def load_sql_query(query_name):
    """Loads an SQL query from the .sql file."""
    file_path = os.path.join(SQL_DIR, f"{query_name}.sql")
    print(file_path)
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            return f.read()
    except FileNotFoundError:
        pytest.fail(f"SQL query file not found: {file_path}")
        return None


@pytest.fixture(scope="function")
def setup_database():
    """
    Sets up an in-memory SQLite database with mock data for testing SQL queries.
    Yields a database connection that is closed after the test.
    """
    conn = sqlite3.connect(":memory:")  # Create an in-memory SQLite database
    cursor = conn.cursor()

    # Create TRANSACTIONS table
    cursor.execute(
        """
        CREATE TABLE transactions (
            date TEXT,
            order_id INTEGER,
            client_id INTEGER,
            prod_id INTEGER,
            prod_price REAL,
            prod_qty INTEGER
        );
    """
    )

    # Create PRODUCT_NOMENCLATURE table
    cursor.execute(
        """
        CREATE TABLE product_nomenclature (
            product_id INTEGER,
            product_type TEXT,
            product_name TEXT
        );
    """
    )

    transactions_data = [
        ("2019-01-01", 1234, 999, 490756, 50.0, 1),
        ("2019-01-01", 1234, 999, 389728, 3.56, 4),
        ("2019-01-01", 3456, 845, 490756, 50.0, 2),
        ("2019-01-01", 3456, 845, 549380, 300.0, 1),
        ("2019-01-01", 3456, 845, 293718, 10.0, 6),
        ("2020-01-01", 5678, 111, 123456, 100.0, 1),
    ]
    cursor.executemany(
        "INSERT INTO transactions VALUES (?, ?, ?, ?, ?, ?)", transactions_data
    )

    product_nomenclature_data = [
        (490756, "MEUBLE", "Chaise"),
        (389728, "DECO", "Boule de Noël"),
        (549380, "MEUBLE", "Canapé"),
        (293718, "DECO", "Mug"),
        (123456, "MEUBLE", "Table"),
    ]
    cursor.executemany(
        "INSERT INTO product_nomenclature VALUES (?, ?, ?)", product_nomenclature_data
    )

    conn.commit()
    yield conn  # Provide the connection to the test function
    conn.close()  # Close the connection after the test


def test_sales_categorisation_query(setup_database):
    """
    Tests the 'sales_categorisation' SQL query using an in-memory database.
    This test covers the logic including the CASE statement for product types,
    and directly validates against the expected output format from the PDF.
    """
    conn = setup_database  # connection
    cursor = conn.cursor()

    # Load the SQL query from the file
    query = load_sql_query("sales_categorisation")

    # Execute the query
    cursor.execute(query)
    results = cursor.fetchall()
    print(results)

    # Define the expected results based on the mock data and PDF example
    expected_results = [(845, 400.0, 60.0), (999, 50.0, 14.24)]

    # Sort results for consistent comparison
    actual_results_sorted = sorted(results, key=lambda x: x[0])
    expected_results_sorted = sorted(expected_results, key=lambda x: x[0])

    # Assert that the actual results match the expected results
    assert actual_results_sorted == expected_results_sorted, "test result: KO"
    print("test result: OK")
