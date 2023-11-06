import prism


def test_load_schema(schema_file):
    schema = prism.load_schema(file=schema_file)
    assert type(schema) is dict
