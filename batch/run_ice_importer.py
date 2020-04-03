import time
import sys
sys.path.append('..')
from service_constants import ei1630BrentCurve, ei1630Oil, ei1930LSGasOil, eiSGTBrentCrude

if(__name__ == "__main__"):
    ei1630BrentCurve.run()
    ei1630Oil.run()
    ei1930LSGasOil.run()
    eiSGTBrentCrude.run()