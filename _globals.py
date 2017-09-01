import os
import re
import uuid

from pypostgres._psql import _psql
from pypostgres._system import _call

global_verbosity=3

def set_global_verbosity(level):
  global global_verbosity
  global_verbosity = level

def _verbose(msg, v_level, verbosity):
  if verbosity == None:
    verbosity = global_verbosity
  if verbosity >= v_level:
    print(msg)

def clean_imports():
  C = """
    drop schema if exists import cascade;
    create schema import;
  """
  _psql(C)

def load_fillna(filepath, preserve=True, verbosity=None):
  if verbosity == None:
    verbosity = global_verbosity
  table_name = os.path.splitext(os.path.basename(filepath))[0].lower()
  C = "drop table if exists import.%s;" % table_name
  _psql(C)
  pgfutter_cmd = """
    pgfutter --user postgres csv '%s'
  """ % filepath
  output = _call(pgfutter_cmd, out_action=("return", "head -n1"))
  if len(output) == 1:
    raise KeyboardInterrupt
  rows = int(output[-1].split()[0])

  return Table(table_name, rows=rows, preserve=preserve, verbosity=verbosity)

def load(filepath, preserve=True, verbosity=None):
  if verbosity == None:
    verbosity = global_verbosity
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
  _psql(C, inpath=filepath)

  return Table(table_name, preserve=preserve, verbosity=verbosity)

class Table:
  @staticmethod
  def __new_name():
    return "table_"+str(uuid.uuid4()).replace("-","_")

  def _print_columns(self, verbosity):
    _verbose("Columns: %d" % len(self.columns), 2, verbosity=verbosity)

  def _print_rows(self, verbosity):
    _verbose("Rows: %d" % self.rows, 1, verbosity=verbosity)

  def __init__(self, table_name, table_schema=None, rows=None, preserve=False, verbosity=None):
    if verbosity == None:
      verbosity = global_verbosity
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
    self.columns = tuple(_psql(C, psql_action='query'))
    self._print_columns(verbosity=verbosity)

  def __del__(self):
    if not self.preserve:
      try:
        if not self.renamed:
          t = self.full_table_name
          C = "drop table %s;" % t
          _psql(C)
      except Exception as e:
        print("TableDestructorInterrupt:", e)

  def rename(self, new_name, verbosity=None):
    if verbosity == None:
      verbosity = global_verbosity
    _verbose("\trename to: %s" % new_name, 3, verbosity=verbosity)
    ft = self.full_table_name
    t = self.table_name
    C = """
      drop table if exists %s;
      alter table %s set schema public;
      alter table %s rename to %s;
    """ % (new_name, ft, t, new_name)
    _psql(C)
    self.table_name = new_name
    self.table_schema = 'public'
    self.full_table_name = "%s.%s" % (self.table_schema, self.table_name)
    self.renamed = True

  def copy_to(self, new_name, table_schema=None, verbosity=None):
    if verbosity == None:
      verbosity = global_verbosity
    _verbose("\tcopy to: %s" % new_name, 3, verbosity=verbosity)
    ft = self.full_table_name
    if table_schema == None:
      table_schema = self.table_schema
    nt = "%s.%s" % (table_schema, new_name)
    C = """
      drop table if exists %s;
      select * into %s from %s;
    """ % (nt, nt, ft)
    _psql(C)

    return Table(new_name, table_schema=table_schema, rows=self.rows, verbosity=verbosity)

  def write_to(self, filepath, verbosity=None):
    if verbosity == None:
      verbosity = global_verbosity
    _verbose("\twrite", 3, verbosity=verbosity)
    C = "select * from %s;" % self.full_table_name
    _psql(C, header=True, psql_action=('query', filepath))

  def match_columns(self, *patterns, full_match=True, verbosity=None):
    if verbosity == None:
      verbosity = global_verbosity
    match = []
    for c in self.columns:
      is_match = False
      for pattern in patterns:
        if full_match:
          pattern = "^%s$" % pattern
        if re.match(pattern, c):
          is_match = True
          break
      if is_match:
        match.append(c)
    return match

  def outer_join(self, other, key, verbosity=None):
    if verbosity == None:
      verbosity = global_verbosity
    _verbose("\tjoin", 3, verbosity=verbosity)

    t1 = self.full_table_name
    t2 = other.full_table_name
    new_table_name = Table.__new_name()
    nt = other.table_schema + "." + new_table_name

    common_columns = list((set(self.columns) & set(other.columns)) - set(key))

    select = "*"
    if len(common_columns) > 0:
      common = lambda c: 'coalesce(t1."%s", t2."%s") as "%s"' % (c, c, c) if c in common_columns else '"%s"' % c
      all_columns = set(self.columns + other.columns)
      select = ",".join([common(c) for c in all_columns])

    conditions = ",".join(['"%s"' % col for col in key])
    C = """
      select %s into %s
      from
        %s as t1 full outer join %s as t2 using(%s);
    """ % (select, nt, t1, t2, conditions)
    output = _psql(C, out_action='return')
    rows = int(output[0].split()[-1])

    return Table(new_table_name, rows=rows, verbosity=verbosity)

  def where(self, condition, verbosity=None):
    if verbosity == None:
      verbosity = global_verbosity
    _verbose("\twhere", 3, verbosity=verbosity)
    t = self.full_table_name
    new_table_name = Table.__new_name()
    nt = self.table_schema + "." + new_table_name

    C = "select * into %s from %s where %s;" % (nt, t, condition)
    output = _psql(C, out_action='return')
    rows = int(output[0].split()[-1])

    return Table(new_table_name, rows=rows, verbosity=verbosity)

  def drop_columns(self, columns, verbosity=None):
    if verbosity == None:
      verbosity = global_verbosity
    _verbose("\tdrop", 3, verbosity=verbosity)
    t = self.full_table_name
    new_table_name = Table.__new_name()
    nt = self.table_schema + "." + new_table_name

    selects = ['"%s"' % c for c in self.columns if not c in columns]
    assert len(selects) == len(self.columns) - len(columns)
    selects = ",".join(selects)

    C = "select %s into %s from %s;" % (selects, nt, t)
    _psql(C)

    return Table(new_table_name, rows=self.rows, verbosity=verbosity)

  def drop_columns_inplace(self, columns, verbosity=None):
    if verbosity == None:
      verbosity = global_verbosity
    _verbose("\tdrop inplace", 3, verbosity=verbosity)
    columns = set([c for c in columns if c in self.columns])
    if len(columns) == 0:
      return
    t = self.full_table_name

    drops = ['drop column "%s"' % c for c in columns]
    drops = ",".join(drops)

    C = "alter table %s %s;" % (t, drops)
    _psql(C)
    self.columns = tuple([c for c in self.columns if not c in columns])
    self._print_columns(verbosity=verbosity)

  def dropna_columns_inplace(self, threshold=0, columns=None, verbosity=None):
    if verbosity == None:
      verbosity = global_verbosity
    _verbose("\tdrop empty columns", 3, verbosity=verbosity)
    threshold = 1 - threshold
    if self.rows == 0:
      return
    if columns == None:
      columns = self.columns
    valid_counts = self.count_valid(columns=self.columns, verbosity=min(verbosity, 2))
    valid_ps = [float(vc) / self.rows for vc in valid_counts]
    column_valid_counts = {}
    for i, valid_p in enumerate(valid_ps):
      column_name = self.columns[i]
      column_valid_counts[column_name] = valid_p

    invalid_columns = [c for c in column_valid_counts if column_valid_counts[c] < threshold]
    self.drop_columns_inplace(invalid_columns, verbosity=min(verbosity, 2))
    remaining_null_columns =  [c for c in column_valid_counts if column_valid_counts[c] != 1 and not c in invalid_columns]
    _verbose("Remaining null columns: %d" % len(remaining_null_columns), 2, verbosity=verbosity)
    return remaining_null_columns

  def dropna_inplace(self, columns=None, verbosity=None):
    if verbosity == None:
      verbosity = global_verbosity
    _verbose("\tdropna", 3, verbosity=verbosity)
    t = self.full_table_name

    if columns == None:
      C = "delete from %s as t where not(t is not null)" % t
    else:
      conditions = " or ".join(['%s is null' % c for c in columns])
      C = "delete from %s where %s" % (t, conditions)
    output = _psql(C, out_action='return')
    dropped_rows = int(output[0].split()[-1])
    self.rows = self.rows - dropped_rows
    self._print_rows(verbosity=verbosity)

  def fillna(self, value, columns=None, verbosity=None):
    if verbosity == None:
      verbosity = global_verbosity
    _verbose("\tfill", 3, verbosity=verbosity)
    t = self.full_table_name
    new_table_name = Table.__new_name()
    nt = self.table_schema + "." + new_table_name

    if columns == None:
      columns = self.columns

    coalesce = lambda c: 'coalesce("%s", \'%s\') as "%s"' % (c, value, c)
    fills = ",".join([coalesce(c) if c in columns else c for c in self.columns])
    C = "select %s into %s from %s;" % (fills, nt, t)
    output = _psql(C, out_action='return')
    rows = int(output[0].split()[-1])

    return Table(new_table_name, rows=rows, verbosity=verbosity)

  def replace_with_na(self, value, columns=None, verbosity=None):
    if verbosity == None:
      verbosity = global_verbosity
    _verbose("\treplace with null", 3, verbosity=verbosity)
    if columns == None:
      columns = self.columns

    t = self.full_table_name
    new_table_name = Table.__new_name()
    nt = self.table_schema + "." + new_table_name

    replace = lambda c: 'nullif("%s", \'%s\') as "%s"' % (c, value, c) if c in columns else '"%s"' % c
    selects = ",".join([replace(c) for c in self.columns])

    C = "select %s into %s from %s;" % (selects, nt, t)
    _psql(C)

    return Table(new_table_name, rows=self.rows, verbosity=verbosity)

  def replace_inplace(self, old_value, new_value, columns=None, regex=False, full_match=True, verbosity=None):
    if verbosity == None:
      verbosity = global_verbosity
    _verbose("\treplace", 3, verbosity=verbosity)

    if columns == None:
      columns = self.columns

    if new_value == None:
      new_value = 'default'
    else:
      new_value = "'%s'" % new_value

    if regex:
      if full_match:
        old_value = "^%s$" % old_value
      compare = '~'
    else:
      compare = '='

    if old_value == None:
      compare = ''
      old_value = "is null"
    else:
      old_value = "'%s'" % old_value

    t = self.full_table_name
    for i, c in enumerate(columns):
      C = """
        update %s set %s = %s where %s %s %s;
      """ % (t, c, new_value, c, compare, old_value)
      _verbose('%d %s' % (i, C.strip()), 3, verbosity=verbosity)
      _psql(C)

  def trim(self, columns=None, verbosity=None):
    if verbosity == None:
      verbosity = global_verbosity
    _verbose("\ttrim", 3, verbosity=verbosity)
    if columns == None:
      columns = self.columns

    t = self.full_table_name
    new_table_name = Table.__new_name()
    nt = self.table_schema + "." + new_table_name

    trim = lambda c: 'trim("%s") as "%s"' % (c, c) if c in columns else '"%s"' % c
    selects = ",".join([trim(c) for c in self.columns])

    C = "select %s into %s from %s;" % (selects, nt, t)
    _psql(C)

    return Table(new_table_name, rows=self.rows, verbosity=verbosity)

  def count(self, verbosity=None):
    if verbosity == None:
      verbosity = global_verbosity
    _verbose("\tcount", 3, verbosity=verbosity)
    C = "select count(*) from %s;" % self.full_table_name
    output = _psql(C, out_action='return')
    return int(output[2].strip())

  def count_valid(self, columns=None, verbosity=None):
    if verbosity == None:
      verbosity = global_verbosity
    _verbose("\tvalid count", 3, verbosity=verbosity)
    if columns == None:
      C = "select count(*) from %s as t where t is not null;" % self.full_table_name
    else:
      counts = ",".join(['count("%s") as %s' % (c, c) for c in columns])
      C = "select %s from %s;" % (counts, self.full_table_name)
    output = _psql(C, out_action='return')
    return [int(s.strip()) for s in output[2].split("|")]
