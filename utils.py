import csv
from pathlib import Path


def create_dir_if_not_exist(dir_: Path) -> None:
    if not dir_.is_dir():
        if not dir_.parent.is_dir():
            create_dir_if_not_exist(
                dir_.parent
            )  # Recursively creates parent directories
        dir_.mkdir()


def write_list_of_dicts_to_csv(path, dict_list, num_to_check_for_fields=200):
    all_keys = set()
    for i in range(num_to_check_for_fields):
        if len(dict_list) <= i:
            break

        all_keys = all_keys.union(dict_list[i].keys())

    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=all_keys)
        writer.writeheader()
        writer.writerows(dict_list)


async def write_list_of_tortoise_objects_to_csv(path, object_list):
    object_dicts = [await obj.to_dict() for obj in object_list]
    write_list_of_dicts_to_csv(path, object_dicts)

