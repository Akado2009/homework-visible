from processor.processor import (
    Processor,
    Operation,
)

pp = Processor("data.ct.gov", "5mzw-sjtu", download=True)

data = pp.calculate("salesratio", Operation.MAX, 10)
print(data[2001])
