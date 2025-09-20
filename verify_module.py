import inspect
import sales_etl.etl as e


print(e.__file__)
print("has_write_parquet:", hasattr(e, "write_parquet"))
print("uses__read_one_in_read_many:", "_read_one(" in inspect.getsource(e.read_many))
print(
    "uses_normalize_types_in_read_many:",
    "normalize_types(" in inspect.getsource(e.read_many),
)
