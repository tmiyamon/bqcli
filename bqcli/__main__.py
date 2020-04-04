import pathlib
import re
import click
from google.cloud import bigquery
from prompt_toolkit import PromptSession
from prompt_toolkit.completion import WordCompleter
from prompt_toolkit.history import FileHistory
from prompt_toolkit.key_binding import KeyBindings
from prompt_toolkit.lexers import PygmentsLexer
from prompt_toolkit.styles import Style
from prompt_toolkit.validation import Validator, ValidationError
from pygments.lexers.sql import SqlLexer
from tabulate import tabulate
from bqcli.config import Config
from bqcli.metacmd import metacmd

sql_completer = WordCompleter([
    'abort', 'action', 'add', 'after', 'all', 'alter', 'analyze', 'and',
    'as', 'asc', 'attach', 'autoincrement', 'before', 'begin', 'between',
    'by', 'cascade', 'case', 'cast', 'check', 'collate', 'column',
    'commit', 'conflict', 'constraint', 'create', 'cross', 'current_date',
    'current_time', 'current_timestamp', 'database', 'default',
    'deferrable', 'deferred', 'delete', 'desc', 'detach', 'distinct',
    'drop', 'each', 'else', 'end', 'escape', 'except', 'exclusive',
    'exists', 'explain', 'fail', 'for', 'foreign', 'from', 'full', 'glob',
    'group', 'having', 'if', 'ignore', 'immediate', 'in', 'index',
    'indexed', 'initially', 'inner', 'insert', 'instead', 'intersect',
    'into', 'is', 'isnull', 'join', 'key', 'left', 'like', 'limit',
    'match', 'natural', 'no', 'not', 'notnull', 'null', 'of', 'offset',
    'on', 'or', 'order', 'outer', 'plan', 'pragma', 'primary', 'query',
    'raise', 'recursive', 'references', 'regexp', 'reindex', 'release',
    'rename', 'replace', 'restrict', 'right', 'rollback', 'row',
    'savepoint', 'select', 'set', 'table', 'temp', 'temporary', 'then',
    'to', 'transaction', 'trigger', 'union', 'unique', 'update', 'using',
    'vacuum', 'values', 'view', 'virtual', 'when', 'where', 'with',
    'without'], ignore_case=True)

style = Style.from_dict({
    'completion-menu.completion': 'bg:#008888 #ffffff',
    'completion-menu.completion.current': 'bg:#00aaaa #000000',
    'scrollbar.background': 'bg:#88aaaa',
    'scrollbar.button': 'bg:#222222',
})

kb = KeyBindings()


@kb.add('enter')
def kb_enter(event):
    text = event.current_buffer.text.strip()
    if not text or text.endswith(';') or text.startswith('\\'):
        event.current_buffer.validate_and_handle()
    else:
        event.current_buffer.insert_text("\n")


config = Config()
config.prepare()

client = bigquery.Client()


class SqlValidator(Validator):
    def __init__(self, client):
        self.client = client

    def validate(self, document):
        text = document.text
        if text.startswith('\\'):
            return

        job_config = bigquery.QueryJobConfig(
            dry_run=True,
            use_query_cache=False
        )

        try:
            self.client.query(document.text, job_config=job_config)
        except Exception as e:
            error = e.errors[0]
            raise ValidationError(message=error['message'], cursor_position=0)


session = PromptSession(
    lexer=PygmentsLexer(SqlLexer),
    history=FileHistory(config.history_path),
    key_bindings=kb,
    completer=sql_completer,
    style=style,
    validate_while_typing=False,
    validator=SqlValidator(client)
)


@click.group()
def cli():
    pass


while True:
    try:
        text = session.prompt('> ', multiline=True)
        if not text.strip():
            continue  # Blank line
    except KeyboardInterrupt:
        continue  # Control-C pressed. Try again.
    except EOFError:
        break  # Control-D pressed.

    if text.startswith('\\'):
        metacmd(re.split('[ ]+', str(text[1:])), obj={'client': client}, standalone_mode=False)
    else:
        try:
            max_results = 100
            query_job = client.query(text)
            result = query_job.result(max_results=max_results)

            if result.total_rows:
                headers = (s.name for s in result.schema)
                values = (r.values() for r in result)
                output = tabulate(values, headers=headers, tablefmt="psql")
                if max_results < result.total_rows:
                    output += '\n' + f'({max_results} of {result.total_rows} rows)'
                else:
                    output += '\n' + f'({result.total_rows} rows)'
                click.echo_via_pager(output)
                print(output)

        except Exception as e:
            print(repr(e))
