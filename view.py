from typing import List, Tuple
from yattag.doc import Doc
from table import Table, FreqTable, TableOfTables
from table import MarkdownTable
from markdown2 import markdown


def html_lineage(s: Table):
    doc, tag, text = Doc().tagtext()
    with tag("ul", id="parent-tables", klass="column col-9 breadcrumb"):
        if len(s.lineage) > 1:
            for parent in s.lineage:
                with tag("li", klass="breadcrumb-item"):
                    id_ = parent.name or parent.uid
                    doc.line("a", str(parent), href=f"/tables/{id_}")
    return doc.getvalue()


def html_navbar(s: Table):
    doc, tag, text = Doc().tagtext()
    with tag("div", style="padding: 0.6rem", klass="column col-2 col-ml-auto"):
        with tag("span", klass="navbar-links"):
            doc.line("a", "Download", href=f"/downloads/{s.uid}/")
        with tag("span", klass="navbar-links"):
            doc.line("a", "About", href="/about/")
    return doc.getvalue()


def html_hints(s: Table):
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


def html_header_row(s: Table):
    doc, tag, text = Doc().tagtext()
    with tag("tr", ("x-ref", "header")):
        for idx, col in enumerate(s.columns):
            is_sorted = col in s.orderbys
            with tag("th"):
                doc.add_class("sorted-col" if is_sorted else "")
                doc.attr(("x-ref", f"col-{idx}"))
                doc.attr(("x-bind", f"col({idx})"))
                doc.attr(("data-colname", col))
                doc.text(col)
                if is_sorted:
                    sort_sign = "↑" if s.orderbys[col] else "↓"
                    doc.line("span", sort_sign, klass="arrow")
    return doc.getvalue()


def html_search_row(s: Table):
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


def html_rows(s: Table, rows: List[Tuple[str]]):
    doc, tag, text = Doc().tagtext()
    for i, row in enumerate(rows):
        with tag(
            "tr",
            ("x-bind", f"row({i})"),
            ("x-ref", f"row-{i}"),
            style="cursor: pointer",
        ):
            for j, col in enumerate(s.columns):
                is_none = row[j] is None
                is_percent = col == "percentage"
                is_float = isinstance(row[j], float)
                display_val = str(row[j])
                if is_none:
                    display_val = "NaN"
                elif is_float:
                    display_val = f"{row[j]:.2f}"

                klass = "markdown-cell" if isinstance(s, MarkdownTable) else ""

                # TODO: refactor: None and empty string have the same
                # data-val attribute.
                # The frontend js checks for the presence of "null" class
                # to distinguish between the two
                with tag(
                    "td",
                    ("x-ref", f"cell-{i}-{j}"),
                    ("x-bind", f"cell({i}, {j})"),
                    ("data-val", row[j] if not is_none else ""),
                    klass=klass,
                ):
                    doc.add_class("null" if is_none else "")
                    doc.add_class("percentage" if is_percent else "")
                    if is_percent:
                        doc.attr(style=f"background-size: {display_val}% 100%")
                    if isinstance(s, MarkdownTable):
                        doc.asis(markdown(display_val))
                    else:
                        text(display_val)

    return doc.getvalue()


_MAX_NUM_ROWS = 25


def html_table(s: Table, page: int):

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


def html_footer(s: Table, page: int = 0):
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


def html_right_cheatsheet():
    doc, tag, text = Doc().tagtext()
    # with tag("p"):
    #     doc.line("span", "?", klass="label")
    #     text(" for help")
    return doc.getvalue()


def html_table_parent(s: Table, page):
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
                doc.line("div", "", klass="column col-1")

            with tag("div", klass="columns col-gapless"):
                with tag("div", klass="column col-1 hide-xl"):
                    doc.asis(html_hints(s))

                with tag("div", klass="column col-10"):
                    doc.asis(html_table(s, page=page))
                    doc.asis(html_footer(s, page=page))

                with tag("div", klass="column col-1 hide-xl", id="cheatsheet"):
                    doc.asis(html_right_cheatsheet())

    return doc.getvalue()


def html_page(s: Table, page: int):
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
        doc.stag("link", rel="preconnect", href="https://fonts.googleapis.com")
        doc.stag(
            "link",
            "crossorigin",
            rel="preconnect",
            href="https://fonts.gstatic.com",
        )

        doc.stag(
            "link",
            rel="stylesheet",
            href="https://fonts.googleapis.com/css2?family=Assistant:wght@300;500&family=Roboto:wght@100;400&display=swap",
        )

        doc.line("script", "", "defer", src="/js/script.js")
        doc.line("script", "", "defer", src="/static/cdn.min.js")
        doc.stag("link", rel="stylesheet", href="/static/style.css")
        with doc.tag("body"):
            doc.asis(html_table_parent(s, page))

    return doc.getvalue()
