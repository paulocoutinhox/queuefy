"""the prose goes stale in silence, so the suite is what keeps it honest"""

import ast
import builtins
import importlib
import pathlib
import re

import pytest

from queuefy.store import redis as redis_store
from queuefy.store.sqlalchemy import runs

DOCS = sorted(pathlib.Path("docs").glob("*.md")) + [pathlib.Path("README.md")]

# what the prose names and the code does not own: other people's libraries, and the identifiers an example invents for itself
FOREIGN = {
    "DATETIME",
    "FLOAT",
    "UPDATE",
    "SELECT",
    "INSERT",
    "MIT",
    "AsyncIO",
    "PostgreSQL",
    "SQLAlchemy",
    "FastAPI",
    "Redis",
    "Lua",
    "NAT",
    "socket_timeout",
    "socket_connect_timeout",
    "command_timeout",
    "connect_timeout",
    "max_execution_time",
    "innodb_lock_wait_timeout",
    "decode_responses",
    "health_check_interval",
    "retry_on_timeout",
    "from_url",
    "create_async_engine",
    "journal_mode",
    "exec_driver_sql",
    "asyncio.CancelledError",
    "CancelledError",
    "SystemExit",
    "KeyboardInterrupt",
    "sys.exit",
    "Exception",
    "BaseException",
    "TypeError",
    "apply_async",
    "pytest.ini",
    "pyproject.toml",
    "Makefile",
    "QUEUEFY_REDIS_URL",
    "QUEUEFY_MYSQL_URL",
    "QUEUEFY_POSTGRES_URL",
    "make_url",
    "CONFIG",
    "sync_to_async",
    "current_app",
    "app_context",
    "db.session",
    "Depends",
    "async_sessionmaker",
    "utcnow",
    "close_old_connections",
    "SynchronousOnlyOperation",
    "update_fields",
    "welcomed_at",
    "account_id",
    "send_welcome",
    "Account.objects",
    "async_to_sync",
    "on_commit",
    "transaction.on_commit",
    "BaseCommand",
    "DATABASES",
    "INSTALLED_APPS",
    "SIGTERM",
    "SignUpRequest",
    "create_account",
    "run_worker",
    "DATABASE_URL",
    "pool_pre_ping",
}


def written() -> str:
    root = pathlib.Path(".")
    files = [path for folder in ("src", "tests") for path in (root / folder).rglob("*.py")]

    return "\n".join(path.read_text() for path in files)


def prose() -> str:
    return "\n".join(path.read_text() for path in DOCS)


def test_the_table_the_documentation_names_is_the_table_the_store_builds():
    """it drifted once already, and a wrong name sends somebody looking for a table nobody has"""
    named = set(re.findall(r"`(queuefy_\w+)`", prose()))
    built = {runs.name} | {index.name for index in runs.indexes}

    assert named <= built, f"the documentation names a table or index that does not exist: {sorted(named - built)}"
    assert runs.name in named, "the one table this store builds is worth naming somewhere"


def test_the_redis_keys_the_documentation_names_are_the_ones_the_store_writes():
    """the families the prose lists are read out of the store itself, so a layout that moves takes the documentation with it"""
    named = {piece.split(":")[0] for piece in re.findall(r"`queuefy:([\w{}:]+)`", prose())}
    source = pathlib.Path("src/queuefy/store/redis.py").read_text()
    written = set(re.findall(r"[':]:(\w+)[':]", source))

    assert named, "the redis layout is worth naming somewhere"
    assert named <= written, f"the prose names a key family the store never writes: {sorted(named - written)}"
    assert redis_store.PREFIX == "queuefy"


def test_every_symbol_the_documentation_names_still_exists():
    """a rename leaves the prose pointing at nothing, and whoever follows it looks for a function that is gone"""
    cited = {name for name in re.findall(r"`([A-Za-z_][A-Za-z0-9_]*(?:\.[A-Za-z_][A-Za-z0-9_]*)*)`", prose()) if "_" in name or name[0].isupper()}
    code = written()

    missing = sorted(name for name in cited - FOREIGN if not re.search(rf"\b{re.escape(name.split('.')[-1])}\b", code))

    assert missing == [], f"the documentation names what the code no longer has: {missing}"


@pytest.mark.parametrize("page", DOCS)
def test_every_example_the_documentation_shows_is_python(page):
    """an example is the part of the prose somebody runs, so one that does not even parse is the loudest way a page can lie. two were decorators with nothing under them, in fences everywhere else close over a body"""
    for number, block in enumerate(re.findall(r"```python\n(.*?)```", page.read_text(), re.DOTALL), 1):
        try:
            ast.parse(block)
        except SyntaxError as broken:
            raise AssertionError(f"{page} shows an example that does not parse in block {number}: {broken}") from broken


# the values a reader fills in rather than imports: a connection string, a file, an instant they choose
SUPPLIED = {"url", "path", "when"}


def bound(node, known: set) -> None:
    """every way a name comes to exist inside an example, so that what is left over is what the page never said where to get"""
    if isinstance(node, (ast.Import, ast.ImportFrom)):
        known |= {(alias.asname or alias.name).split(".")[0] for alias in node.names}

    if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
        known.add(node.name)

    if isinstance(node, ast.Assign):
        known |= {target.id for target in node.targets if isinstance(target, ast.Name)}

    if isinstance(node, ast.arg):
        known.add(node.arg)

    if isinstance(node, ast.withitem) and isinstance(node.optional_vars, ast.Name):
        known.add(node.optional_vars.id)


@pytest.mark.parametrize("page", DOCS)
def test_every_example_says_where_its_names_come_from(page):
    """an example that uses a name it never imports is one nobody can run, and the reader is left guessing which package `close_old_connections` was from. the whole page has to answer it, not every block"""
    known, used = set(dir(builtins)) | SUPPLIED, set()

    for block in re.findall(r"```python\n(.*?)```", page.read_text(), re.DOTALL):
        for node in ast.walk(ast.parse(block)):
            bound(node, known)

            if isinstance(node, ast.Name) and isinstance(node.ctx, ast.Load):
                used.add(node.id)

    assert used <= known, f"{page} uses names it never imports: {sorted(used - known)}"


def paragraphs(page: pathlib.Path):
    """the prose of a page, wrapped lines joined back into the sentences they belong to and every fenced block left out"""
    fenced, block = False, []

    for line in page.read_text().splitlines():
        if line.startswith("```"):
            fenced = not fenced

            continue

        if fenced:
            continue

        if not line.strip() or line.startswith(("#", "|")) or (re.match(r"^\s*[-*]\s|^>\s*[-*]\s", line) and block):
            if block:
                yield " ".join(block)
                block = []

        if line.strip() and not line.startswith(("#", "|")):
            block.append(line.strip())

    if block:
        yield " ".join(block)


@pytest.mark.parametrize("page", DOCS)
def test_no_sentence_of_the_documentation_begins_with_code(page):
    """a sentence opening on a backtick opens on a lowercase identifier, which reads as a fragment and breaks the language rather than the code. one word in front of it is enough"""
    opening = []

    for block in paragraphs(page):
        body = re.sub(r"^[>\-*]\s*", "", block).strip()

        opening += [start for start in [body] + [piece.strip() for piece in re.split(r"(?<=[.!?])\s+", body)[1:]] if re.match(r"^\*{0,2}`", start)]

    assert opening == [], f"{page} opens a sentence on code: {[start[:70] for start in opening]}"


@pytest.mark.parametrize("page", DOCS)
def test_every_page_the_index_points_at_exists(page):
    for link in re.findall(r"\]\((?!http)([^)#]+)\)", page.read_text()):
        target = (page.parent / link).resolve()

        assert target.exists(), f"{page} points at {link}, which is not there"


def headings(page: pathlib.Path) -> list[str]:
    """the `#` inside a fenced block is a comment of the language in it, and never a heading"""
    lines, fenced = [], False

    for line in page.read_text().splitlines():
        if line.startswith("```"):
            fenced = not fenced
        elif not fenced and line.startswith("#"):
            lines.append(line)

    return lines


@pytest.mark.parametrize("page", DOCS)
def test_every_heading_of_every_page_is_marked(page):
    """the pages are read by eye before they are read by word, and one page out of step is the one nobody finds"""
    bare = [line for line in headings(page) if not re.match(r"^#{1,6} [^\w\s]", line)]

    assert bare == [], f"{page} has a heading with nothing to see it by: {bare}"


@pytest.mark.parametrize("page", DOCS)
def test_no_page_marks_two_headings_the_same_way(page):
    marks = [re.match(r"^#{1,6} (\S+)", line).group(1) for line in headings(page)]
    repeated = sorted({mark for mark in marks if marks.count(mark) > 1})

    assert repeated == [], f"{page} uses the same mark for more than one heading: {repeated}"


def tuned():
    """the rows of the constants table, each naming what it tunes, the module holding it and the value the prose claims"""
    rows = re.findall(r"^\| `([^|]+)` \| `([\w/.]+\.py)` \| ([^|]+) \|", pathlib.Path("CLAUDE.md").read_text(), re.MULTILINE)

    return [([name.strip(" `") for name in names.split("/")], importlib.import_module("queuefy." + module.removesuffix(".py").replace("/", ".")), [piece.strip(" `") for piece in claimed.split("/")]) for names, module, claimed in rows]


def numeric(stated: list[str]) -> bool:
    """a row states a plain number for each constant it names, or it states the reasoning instead — and only the first kind can be compared"""
    return all(re.fullmatch(r"-?\d+(\.\d+)?", piece) for piece in stated)


def test_the_constants_the_documentation_tunes_are_the_ones_the_code_holds():
    """every tuning number is written down twice, and the day one moves the table goes on saying the old one — which is worse than saying nothing, because whoever tunes against it is reading a number the fleet has not run on for a year"""
    table = tuned()

    assert len(table) >= 10, "the table is where every tuning constant is explained, and it was not found"

    for named, module, stated in table:
        for name in named:
            assert hasattr(module, name), f"CLAUDE.md tunes `{name}` in {module.__name__}, which does not have it"

        if len(stated) != len(named) or not numeric(stated):
            continue

        for name, piece in zip(named, stated):
            assert float(getattr(module, name)) == float(piece), f"CLAUDE.md says `{name}` is {piece} and {module.__name__} holds {getattr(module, name)}"


def test_the_layout_the_documentation_draws_is_the_package_that_exists():
    """the layout block is a map, and a map nobody checks is one that sends people to a module that moved. renaming one already needed it corrected by hand, which is the drift this catches"""
    drawn = re.search(r"```\nsrc/queuefy/\n(.*?)```", pathlib.Path("CLAUDE.md").read_text(), re.DOTALL)

    assert drawn, "the layout block is where the package is explained, and it was not found"

    named = set(re.findall(r"^\s{2,4}([a-z_]+\.py)", drawn.group(1), re.MULTILINE))
    present = {path.name for path in pathlib.Path("src/queuefy").rglob("*.py")}

    assert named == present, f"the layout names {sorted(named - present)} which is gone, and misses {sorted(present - named)}"


def test_every_page_of_the_documentation_is_linked_from_the_readme():
    """a page nothing points at is a page nobody reads"""
    linked = set(re.findall(r"\]\(docs/([\w-]+\.md)\)", pathlib.Path("README.md").read_text()))
    present = {path.name for path in pathlib.Path("docs").glob("*.md")} - {"index.md"}

    assert present <= linked, f"the readme does not point at: {sorted(present - linked)}"
