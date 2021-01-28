import sys
from itertools import compress
from pathlib import Path

current_path = Path(__file__).parent
sub_dir = [x for x in current_path.iterdir() if x.is_dir()]
core_path = current_path / 'core'
pycore_path = core_path / 'pycore'

sys.path.append(pycore_path.as_posix())


# userpath = "C:\\Users\\user2\\Documents\\" # should change by user
# functionpath = "fileIO\\core\\pycore\\"
# filepath = userpath + "fileIO\\core\\market\\" # path for ds.py file
# path = userpath+functionpath
# sys.path.append(path)

from ds import * # revise the path in "ds.py" file.

# Query

if __name__=='__main__':
    fetchdf('mcr','us',20190101,20190131) # data available from 20190101 to 20191231
    fetchdf('stk','us',20190101,20190131) # data available from 20190101 to 20190331