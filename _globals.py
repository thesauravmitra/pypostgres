from postgres._psql import _psql
from postgres._system import _call

def _verbose(msg, v_level, verbosity):
  if verbosity >= v_level:
    print(msg)

def clean_imports():
  C = """
    drop schema if exists import cascade;
    create schema import;
  """
  _psql(C)

def load_fillna(filepath, preserve=True, verbosity=0):
  table_name = os.path.splitext(os.path.basename(filepath))[0].lower()
  C = "drop table if exists import.%s;" % table_name
  _psql(C)
  pgfutter_cmd = """
    pgfutter --user postgres csv '%s'
  """ % filepath
  output = _call(pgfutter_cmd, tee_pipe="head -n1")
  if len(output) == 1:
    raise KeyboardInterrupt
  rows = int(output[-1].split()[0])

  return Table(table_name, rows=rows, preserve=preserve, verbosity=verbosity)

def load(filepath, preserve=True, verbosity=0):
  _verbose("\tload", 3, verbosity=verbosity)
  table_name = os.path.splitext(os.path.basename(filepath))[0].lower()
  ft = "import.%s" % table_name
  f = open(filepath)
  columns = [c.lower().replace(' ', '_') for c in f.readline().strip().split(",")]
  column_definitions = ",".join(['"%s" text' % c for c in columns])
  C = """
    drop table if exists %s;
    create table %s(%s);
    copy %s from stdin with csv header delimiter ',';
  """ % (ft, ft, column_definitions, ft)
  _psql(C, stdin=filepath)

  return Table(table_name, preserve=preserve, verbosity=verbosity)

class Table:
  def _print_columns(self, verbosity):
    _verbose("Columns: %d" % len(self.columns), 2, verbosity=verbosity)

  def _print_rows(self, verbosity):
    _verbose("Rows: %d" % self.rows, 1, verbosity=verbosity)

  def __init__(self, table_name, table_schema=None, rows=None, preserve=False, verbosity=0):
    self.table_name = table_name
    if table_schema == None:
      table_schema = 'import'
    self.table_schema = table_schema
    if self.table_schema == 'public':
      self.full_table_name = self.table_name
    else:
      self.full_table_name = self.table_schema + "." + self.table_name
    self.preserve = preserve
    self.renamed = False
    if rows != None:
      self.rows = rows
    else:
      self.rows = self.count()
    self._print_rows(verbosity=verbosity)

    C = """
      select column_name
      from information_schema.columns
      where
        table_schema = '%s' and
        table_name = '%s';
    """ % (self.table_schema, self.table_name)
    self.columns = _psql(C, output=True)
    self._print_columns(verbosity=verbosity)
