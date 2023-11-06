from tortoise.models import Model
from tortoise import fields


def get_blueprint_data(blueprint):
    try:
        blueprint_full = blueprint.blueprint
        blueprint_name = blueprint.name
        blueprint_edition = blueprint.edition
    except Exception:
        blueprint_full = None
        blueprint_name = None
        blueprint_edition = None

    return {
        "blueprint": blueprint_full,
        "blueprint_name": blueprint_name,
        "blueprint_edition": blueprint_edition,
    }



class Blueprint(Model):
    blueprint = fields.TextField(pk=True)
    name = fields.TextField()
    edition = fields.TextField()
    token_address = fields.TextField()
    token_id = fields.TextField()

    def __str__(self):
        return self.blueprint


class Asset(Model):
    id = fields.TextField(pk=True)
    token_address = fields.TextField()
    token_id = fields.TextField()
    user = fields.TextField()
    status = fields.TextField(null=True)
    uri = fields.TextField(null=True)
    name = fields.TextField()
    description = fields.TextField(null=True)
    image_url = fields.TextField(null=True)
    metadata = fields.JSONField(null=True)
    collection = fields.JSONField(null=True)
    created_at = fields.TextField()
    updated_at = fields.TextField()
    blueprint: fields.ForeignKeyRelation[Blueprint] = fields.ForeignKeyField("models.Blueprint", related_name="assets", null=True)
    mint_address = fields.TextField(null=True)
    first_non_mint_address = fields.TextField(null=True)
    num_transfers = fields.IntField(null=True)

    async def to_dict(self):
        blueprint = await self.blueprint
        blueprint_data = get_blueprint_data(blueprint)

        return {
            "id": self.id,
            "token_address": self.token_address,
            "token_id": self.token_id,
            "user": self.user,
            "status": self.status,
            "uri": self.uri,
            "name": self.name,
            "description": self.description,
            "image_url": self.image_url,
            "collection": self.collection.get("name"),
            "created_at": self.created_at,
            "updated_at": self.updated_at,
            "blueprint": blueprint_data["blueprint"],
            "blueprint_name": blueprint_data["blueprint_name"],
            "blueprint_edition": blueprint_data["blueprint_edition"],
            "mint_address": self.mint_address,
            "first_non_mint_address": self.first_non_mint_address,
            "num_transfers": self.num_transfers,
        } | self.metadata


class Transfer(Model):
    receiver = fields.TextField()
    status = fields.TextField()
    timestamp = fields.TextField()
    transaction_id = fields.IntField()
    user = fields.TextField()
    asset: fields.ForeignKeyRelation[Asset] = fields.ForeignKeyField("models.Asset", related_name="transfers")

    async def to_dict(self):
        asset = await self.asset
        blueprint = await asset.blueprint

        try:
            blueprint_data = get_blueprint_data(blueprint)
        except Exception:
            blueprint_data = {
                "blueprint": None,
                "blueprint_name": None,
                "blueprint_edition": None,
            }

        return {
            "receiver": self.receiver,
            "status": self.status,
            "timestamp": self.timestamp,
            "transaction_id": self.transaction_id,
            "user": self.user,
            "asset_token_address": asset.token_address,
            "asset_token_id": asset.token_id,
            "asset_blueprint": blueprint_data["blueprint"],
            "asset_blueprint_name": blueprint_data["blueprint_name"],
            "asset_blueprint_edition": blueprint_data["blueprint_edition"],
        }
