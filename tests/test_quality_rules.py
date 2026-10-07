from spark.jobs.quality_rules import (
    has_no_null_ids,
    has_positive_values,
    has_unique_ids,
    has_valid_foreign_keys,
)


def test_unique_ids():
    rows = [
        {"customer_id": 1},
        {"customer_id": 2},
        {"customer_id": 3},
    ]

    assert has_unique_ids(rows, "customer_id")


def test_duplicate_ids_fail():
    rows = [
        {"customer_id": 1},
        {"customer_id": 1},
    ]

    assert not has_unique_ids(rows, "customer_id")


def test_no_null_ids():
    rows = [
        {"customer_id": 1},
        {"customer_id": 2},
    ]

    assert has_no_null_ids(rows, "customer_id")


def test_null_ids_fail():
    rows = [
        {"customer_id": 1},
        {"customer_id": None},
    ]

    assert not has_no_null_ids(rows, "customer_id")


def test_positive_values():
    rows = [
        {"order_total": 25.50},
        {"order_total": 100.00},
    ]

    assert has_positive_values(rows, "order_total")


def test_non_positive_values_fail():
    rows = [
        {"order_total": 25.50},
        {"order_total": 0},
    ]

    assert not has_positive_values(rows, "order_total")


def test_valid_foreign_keys():
    customers = [
        {"customer_id": 1},
        {"customer_id": 2},
    ]

    orders = [
        {"customer_id": 1},
        {"customer_id": 2},
    ]

    assert has_valid_foreign_keys(
        orders,
        "customer_id",
        customers,
        "customer_id",
    )


def test_invalid_foreign_keys_fail():
    customers = [
        {"customer_id": 1},
    ]

    orders = [
        {"customer_id": 1},
        {"customer_id": 99},
    ]

    assert not has_valid_foreign_keys(
        orders,
        "customer_id",
        customers,
        "customer_id",
    )
