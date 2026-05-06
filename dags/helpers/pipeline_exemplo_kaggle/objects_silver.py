
import re
from pyspark.sql.functions import udf, initcap, trim, col, current_timestamp, to_timestamp
from pyspark.sql.types import StringType

# UDF para padronizar nomes de cidades e estados (capitalizar e minúsculas preposições)
def standardize_name(name):
    if name is None:
        return None
    # Capitalize first letter of each word
    standardized = ' '.join([word.capitalize() for word in name.split()])
    # Lowercase specific prepositions
    prepositions = re.compile(r'\b(Da|De|Do|Das|Dos)\b', re.IGNORECASE)
    return prepositions.sub(lambda m: m.group(0).lower(), standardized)

standardize_name_udf = udf(standardize_name, StringType())