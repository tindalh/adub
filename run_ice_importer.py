import time
from service_constants import ei1630BrentCurve, ei1630Oil, ei1930LSGasOil, eiSGTBrentCrude

if(__name__ == "__main__"):
    ei1630BrentCurve.run()
    time.sleep(20)
    ei1630Oil.run()
    time.sleep(20)
    ei1930LSGasOil.run()
    time.sleep(20)
    eiSGTBrentCrude.run()
    time.sleep(20)