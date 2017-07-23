import os

def _read_log(logpath):
  f = open(logpath)
  output = f.read()
  f.close()
  os.system("rm %s" % logpath)
  return output.strip().split("\n")

def _call(command, out_action='quiet', inpath=None, progress_bar=False):
  logpath='.log'
  try:
    out_action, out_arg = out_action
  except:
    out_arg = None

  if out_action == "return":
    if out_arg == None:
      direct_out = "> %s" % logpath
    else:
      direct_out = "| tee %s | %s" % (logpath, out_arg)
  elif out_action == "quiet":
    direct_out = "&> /dev/null"
  else:
    direct_out = ""

  if inpath == None:
    pipe_in = ""
  elif progress_bar:
    pipe_in = "pv %s |" % inpath
  else:
    pipe_in = "cat %s |" % inpath

  command = "%s %s %s" % (pipe_in, command, direct_out)
  code = os.system(command)
  status = os.WEXITSTATUS(code)
  if status != 0:
    raise Exception(status)
  if out_action == "return":
    return _read_log(logpath)
