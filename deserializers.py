from models import Asset, Blueprint


async def create_asset(asset_dict: dict):
    asset, created = await Asset.get_or_create(
        id=f"{asset_dict['token_address']}-{asset_dict['token_id']}",
        defaults=dict(
            token_address=asset_dict['token_address'],
            token_id=asset_dict['token_id'],
            user=asset_dict['user'],
            status=asset_dict['status'],
            uri=asset_dict['uri'],
            name=asset_dict['name'],
            description=asset_dict['description'],
            image_url=asset_dict['image_url'],
            metadata=asset_dict['metadata'],
            collection=asset_dict['collection'],
            created_at=asset_dict['created_at'],
            updated_at=asset_dict['updated_at'],
        )
    )

    return asset