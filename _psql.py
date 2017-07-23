from pypostgres import _system

def _psql(command, header=False, psql_action=None, out_action='quiet', inpath=None, progress_bar=True):
  command = command.replace('"', '\\"').strip()

  try:
    psql_action, psql_arg = psql_action
  except:
    psql_arg = None

  if psql_action == 'query':
    logged_query = psql_arg == None
    if psql_arg == None:
      psql_arg = '.log.csv'

    command = command.rstrip(";")
    header = 'header' if header else ''

    C = "\\copy (%s) to '%s' with csv %s delimiter ',';" % (command, psql_arg, header)
  else:
    C = command

  sys_C = "psql -U postgres -c \"%s\"" % C
  sys_output = _system._call(sys_C, out_action=out_action, inpath=inpath, progress_bar=progress_bar)

  if psql_action == 'query' and logged_query:
    return _system._read_log(psql_arg)
  return sys_output
