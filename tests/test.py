from table import Table
from pypika import Query


def load_test_table():
    return Table(
        view=Query.from_("test_2").select("*"),
        name="testtable",
        dbtype="disk",
        source=None,
    )


def test_create_table():
    _ = Table(
        view=Query.from_("test_2").select("*"),
        name="testtable",
        dbtype="disk",
        source=None,
    )


def test_table_num_rows():
    table = load_test_table()
    assert len(table) == 63160
