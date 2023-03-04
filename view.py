from typing import Any, List, Tuple
from yattag.doc import Doc
from table import ColType, Table, FreqTable, TableOfTables
from table import MarkdownTable
from markdown2 import markdown


def html_lineage(s: Table) -> str:
    doc, tag, text = Doc().tagtext()
    with tag(
        "ul", id="parent-tables", klass="column col-8 hide-xs breadcrumb"
    ):
        if len(s.lineage) > 1:
            for parent in s.lineage:
                with tag("li", klass="breadcrumb-item"):
                    id_ = parent.name or parent.uid
                    doc.line("a", str(parent), href=f"/tables/{id_}")
    return doc.getvalue()


def html_navbar(s: Table) -> str:
    doc, tag, text = Doc().tagtext()
    with tag(
        "div",
        style="padding: 0.6rem",
        klass="column col-2 col-xl-4 col-xs-auto",
    ):
        with tag("div", style="float: right"):
            with tag("span", klass="navbar-links"):
                doc.line("a", "Download (CSV)", href=f"/downloads/{s.uid}/")
            with tag("span", klass="navbar-links"):
                doc.line("a", "Explore", href=f"/")
            with tag("span", klass="navbar-links"):
                doc.line("a", "About", href="/about/")
    return doc.getvalue()


def html_hints(s: Table) -> str:
    doc, tag, text = Doc().tagtext()
    with tag(
        "div", ("x-bind", "hints_sidebar"), id="hints", style="display: none;"
    ):
        if isinstance(s, FreqTable) or isinstance(s, TableOfTables):
            hint_text = "Facet" if isinstance(s, FreqTable) else "Open Table"
            with tag(
                "button",
                ("x-bind", "open_hint"),
                style="display: none;",
                klass="btn btn-sm",
            ):
                text(f"{hint_text} ")
                doc.line("span", "Enter", klass="label")
        with tag(
            "button",
            ("x-bind", "filter_hint"),
            style="display: none;",
            klass="btn btn-sm",
        ):
            text(f"Filter ")
            doc.line("span", '"', klass="label")

        with tag(
            "button",
            ("x-bind", "freq_hint"),
            style="display: none;",
            klass="btn btn-sm",
        ):
            text(f"Histogram ")
            doc.line("span", "f", klass="label")
    return doc.getvalue()


def html_header_row(s: Table) -> str:
    doc, tag, text = Doc().tagtext()
    with tag("tr", ("x-ref", "header")):
        for idx, column in enumerate(s.columns):
            column_name = column.name
            is_sorted = column_name in s.orderbys
            with tag("th"):
                doc.add_class("sorted-col" if is_sorted else "")
                doc.attr(("x-ref", f"col-{idx}"))
                doc.attr(("x-bind", f"col({idx})"))
                doc.attr(("data-colname", column_name))
                doc.text(column_name)
                if column.type == ColType.INT:
                    doc.line("em", " int", style="color: #75c2ca")
                elif column.type == ColType.FLOAT:
                    doc.line("em", " fl", style="color: pink")
                if is_sorted:
                    is_asc = s.orderbys[column_name]
                    sort_sign = "↑" if is_asc else "↓"
                    bind = "sort_asc" if is_asc else "sort_desc"
                    doc.line(
                        "span",
                        sort_sign,
                        (
                            "data-tooltip",
                            "Sort desc ]\nSort asc [",
                        ),
                        ("x-bind", bind),
                        klass="sort-arrow tooltip tooltip-right",
                    )
    return doc.getvalue()


def html_search_row(s: Table) -> str:
    doc, tag, text = Doc().tagtext()
    with tag("tr", ("x-bind", "search_row()"), style="display: none;"):
        for idx, _ in enumerate(s.columns):
            with tag("td"):
                with tag(
                    "div",
                    ("x-ref", f"search-{idx}"),
                    ("x-bind", f"search_div({idx})"),
                    style="display: none",
                ):
                    doc.stag(
                        "input",
                        ("x-bind", f"search_input({idx})"),
                        klass="form-input",
                        type="text",
                        style="display: inline",
                        placeholder="Press [Enter] to search",
                    )
                    with tag(
                        "button",
                        ("x-bind", f"search_close_btn()"),
                        ("data-tooltip", "[Esc]"),
                        klass="close-btn tooltip",
                    ):
                        with tag("span"):
                            doc.asis("&times;")

    return doc.getvalue()


def html_cell(
    s: Table, val: Any, i: int, j: int, is_percent: bool = False
) -> str:
    doc, tag, text = Doc().tagtext()
    is_none = val is None
    is_float = isinstance(val, float)
    display_val = str(val)
    if is_none:
        display_val = "NaN"
    elif is_float:
        display_val = f"{val:.2f}"

    klass = "markdown-cell" if isinstance(s, MarkdownTable) else ""

    # TODO: refactor: None and empty string have the same
    # data-val attribute.
    # The frontend js checks for the presence of "null" class
    # to distinguish between the two
    with tag(
        "td",
        ("x-ref", f"cell-{i}-{j}"),
        ("x-bind", f"cell({i}, {j})"),
        ("data-val", str(val) if not is_none else ""),
        klass=klass,
    ):
        doc.add_class("null" if is_none else "")
        doc.add_class("percentage" if is_percent else "")
        if is_percent:
            doc.attr(style=f"background-size: {display_val}% 100%")
        if isinstance(s, MarkdownTable):
            doc.asis(markdown(display_val))
        elif type(val) == float:
            doc.line("em", display_val)
        elif type(val) == int:
            doc.line("span", display_val, klass="int")
        else:
            text(display_val)

    return doc.getvalue()


def html_rows(s: Table, rows: List[Tuple[str]]) -> str:
    doc, tag, text = Doc().tagtext()
    for i, row in enumerate(rows):
        with tag(
            "tr",
            ("x-bind", f"row({i})"),
            ("x-ref", f"row-{i}"),
            style="cursor: pointer",
        ):
            for j, column in enumerate(s.columns):
                is_percent = column.name == "percentage"
                doc.asis(html_cell(s, row[j], i, j, is_percent=is_percent))

    return doc.getvalue()


_MAX_NUM_ROWS = 25


def html_table(s: Table, page: int) -> str:
    rows, _ = s[page * _MAX_NUM_ROWS : (page + 1) * _MAX_NUM_ROWS]
    doc, tag, text = Doc().tagtext()
    with tag(
        "table",
        "x-cloak",
        klass="table table-scroll",
        style="border-collapse: separate;",
    ):
        num_cols = len(s.columns)
        parent_uid: str = s.parent.uid if s.parent else "none"
        doc.attr(
            (
                "x-data",
                f"table({len(rows)}, {num_cols}, '{parent_uid}', {s.wrapped_col_indices})",
            )
        )
        if isinstance(s, FreqTable):
            doc.attr(("x-bind", f"freq_table({s.key_col_indices})"))
            doc.add_class("freq-table")
        else:
            doc.attr(("x-bind", "base_table()"))
            doc.add_class("basic-table")
        doc.asis(html_header_row(s))
        doc.asis(html_search_row(s))
        doc.asis(html_rows(s, rows))
    return doc.getvalue()


def html_footer(s: Table, page: int = 0) -> str:
    doc, tag, text = Doc().tagtext()
    with tag("div", "x-cloak", id="table-footer"):
        doc.line("b", str(len(s)))
        text(" rows")
        has_prev_page = page > 0
        has_next_page = (page + 1) * _MAX_NUM_ROWS < len(s)
        if has_prev_page or has_next_page:
            with tag("span", style="float:right"):
                if has_prev_page:
                    doc.line(
                        "a",
                        "prev",
                        id="previous-page",
                        href=f"/tables/{s.uid}?page={page - 1}",
                    )
                else:
                    text("prev")

                doc.line("span", "[P]", klass="label")
                text("| ")

                if has_next_page:
                    doc.line(
                        "a",
                        "next",
                        id="next-page",
                        href=f"/tables/{s.uid}?page={page + 1}",
                    )
                else:
                    text("next")

                doc.line("span", "[N]", klass="label")

    return doc.getvalue()


def html_right_cheatsheet() -> str:
    doc, tag, text = Doc().tagtext()
    # with tag("p"):
    #     doc.line("span", "?", klass="label")
    #     text(" for help")
    return doc.getvalue()


def html_table_parent(s: Table, page) -> str:
    doc, tag, text = Doc().tagtext()
    with tag(
        "div",
        ("x-data", "table_parent()"),
        ("x-bind", "table_parent_bind()"),
        id="table-parent",
    ):
        with tag("div", klass="container"):
            with tag("div", klass="columns"):
                doc.line(
                    "div",
                    "",
                    ("x-bind", "progress()"),
                    klass="column col-1 hide-xl loading hidden",
                )
                doc.asis(html_lineage(s))
                doc.asis(html_navbar(s))
                doc.line("div", "", klass="column col-1 hide-xl")

            with tag("div", klass="columns col-gapless"):
                with tag("div", klass="column col-1 hide-xl"):
                    doc.asis(html_hints(s))

                with tag("div", klass="column col-10 col-xl-12"):
                    if isinstance(s, MarkdownTable):
                        doc.asis(html_markdown(s))
                    else:
                        doc.asis(html_table(s, page=page))
                        doc.asis(html_footer(s, page=page))

                with tag("div", klass="column col-1 hide-xl", id="cheatsheet"):
                    doc.asis(html_right_cheatsheet())

    return doc.getvalue()


def html_markdown(s: MarkdownTable) -> str:
    doc, tag, text = Doc().tagtext()
    with tag("div"):
        rows, _ = s[0:1]
        doc.asis(markdown(rows[0][0]))

    return doc.getvalue()


def html_page(s: Table, page: int) -> str:
    doc = Doc()
    doc.asis("<!DOCTYPE html>")
    with doc.tag("html", lang="en"):
        doc.line("title", "Tablehub")
        doc.stag("meta", charset="utf-8")
        doc.stag(
            "meta",
            name="viewport",
            content="width=device-width, initial-scale=1",
        )
        doc.stag(
            "link",
            rel="icon",
            type="image/png",
            sizes="32x32",
            href="/static/favicon-32x32.png",
        )
        doc.stag(
            "link",
            rel="icon",
            type="image/png",
            sizes="16x16",
            href="/static/favicon-16x16.png",
        )
        doc.stag("link", rel="stylesheet", href="/static/spectre.min.css")

        doc.stag("link", rel="stylesheet", href="/static/spectre-exp.min.css")
        doc.stag(
            "link", rel="stylesheet", href="/static/spectre-icons.min.css"
        )

        doc.line("script", "", "defer", src="/js/script.js")
        doc.line("script", "", "defer", src="/static/cdn.min.js")
        doc.stag("link", rel="stylesheet", href="/static/style.css")
        with doc.tag("body"):
            doc.asis(html_table_parent(s, page))

    return doc.getvalue()
