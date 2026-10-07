def has_unique_ids(rows, key):
    values = [row[key] for row in rows]
    return len(values) == len(set(values))


def has_no_null_ids(rows, key):
    return all(row.get(key) is not None for row in rows)


def has_positive_values(rows, key):
    return all(float(row[key]) > 0 for row in rows)


def has_valid_foreign_keys(child_rows, child_key, parent_rows, parent_key):
    parent_values = {row[parent_key] for row in parent_rows}

    return all(
        row[child_key] in parent_values
        for row in child_rows
    )
