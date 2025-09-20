import json
import decimal


class DecimalEncoder(json.JSONEncoder):
    """
    DecimalEncoder class
    """

    def default(self, o):
        if isinstance(o, decimal.Decimal):
            if o % 1 == 0:
                return int(o)
            return float(o)
        return super(DecimalEncoder, self).default(o)
