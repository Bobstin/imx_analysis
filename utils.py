import csv
from pathlib import Path
import pandas as pd

from models import Transfer, Asset


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

    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=all_keys)
        writer.writeheader()
        writer.writerows(dict_list)


async def write_list_of_tortoise_objects_to_csv(path, object_list):
    object_dicts = [await obj.to_dict() for obj in object_list]
    write_list_of_dicts_to_csv(path, object_dicts)


async def create_transfer_summaries(transfer_dicts, direction) -> tuple[pd.DataFrame, pd.DataFrame]:
    transfers_df = pd.DataFrame(transfer_dicts)

    transfers_df['ones'] = 1
    if direction == 'out':
        index_col = 'receiver'
    else:
        index_col = 'user'

    reduced_transfers_df = transfers_df[['asset_blueprint_name', index_col, 'ones']]
    transfer_counts_by_user = pd.pivot_table(reduced_transfers_df, index=index_col, columns='asset_blueprint_name', values='ones', aggfunc='count')

    transfer_counts = reduced_transfers_df[['asset_blueprint_name', 'ones']].groupby('asset_blueprint_name').sum().rename(columns={'ones': 'count'})

    return transfer_counts_by_user, transfer_counts


async def create_transfer_output_files(transfers: list[Transfer], assets: list[Asset],direction: str, output_dir: Path, file_prefix: str) -> None:
    assets = list(set(assets))
    transfer_dicts = [await transfer.to_dict() for transfer in transfers]
    write_list_of_dicts_to_csv(output_dir / f"{file_prefix} transfers {direction}.csv", transfer_dicts)
    await write_list_of_tortoise_objects_to_csv(output_dir / f"{file_prefix} transferred {direction} assets.csv", assets)
    transfer_counts_by_user, transfer_counts = await create_transfer_summaries(transfer_dicts, direction)
    transfer_counts_by_user.to_csv(output_dir / f"{file_prefix} transfer {direction} counts by user.csv")
    transfer_counts.to_csv(output_dir / f"{file_prefix} transfer {direction} counts.csv")
