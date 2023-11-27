from pyiceberg.schema import Schema
from pyiceberg.types import *

from pyiceberg.catalog import load_catalog

catalog = load_catalog(
    "docs",
    **{
        "uri": "thrift://100.68.81.171:9083",
        "s3.endpoint": "https://tos-s3-cn-beijing.ivolces.com",
        "s3.access-key-id": "AKLTYzczNWQ4YjA0ZWQ3NDdlODg1NTE3MmE1ZGJjZDE0OWY",
        "s3.secret-access-key": "TkRGbU5qTmlOakZtTm1WaU5EaGlaVGd3WlRVM09EWmpZalZoWm1JMk5tUQ==",
    }
)

print(catalog.list_namespaces())