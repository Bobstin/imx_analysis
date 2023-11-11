import pytz
import httpx
import time
from datetime import datetime
from textual.app import App
from textual.widgets import RichLog, Static, ProgressBar, Label, LoadingIndicator, Pretty

from models import Asset, Blueprint, Transfer
from deserializers import create_asset

BASE_URL = "https://api.x.immutable.com/v1"
HEADERS = {"Content-Type": "application/json"}
TEST_LIMIT = 100


class Searcher:
    def __init__(self, app: App, test_mode: bool = False):
        self.app = app
        self.test_mode = test_mode

    def send_to_log(self, message):
        timezone = pytz.timezone("America/New_York")
        timestamp = datetime.now(timezone).strftime("%Y-%m-%d %H:%M")
        log = self.app.query_one("#log", RichLog)
        log.write(f"{timestamp}: {message}")

    async def rate_limited_request(self, url, headers, params):
        backoff_time = .2  # Default rate throttling is 5 requests per second
        status_code = 200
        first_run = True

        while first_run or status_code == 429 or (status_code >= 500 and status_code <= 599):
            first_run = False
            async with httpx.AsyncClient(verify=False, timeout=60) as client:
                response = await client.get(url, headers=headers, params=params)
            status_code = response.status_code
            if status_code == 429:
                self.send_to_log(f'Detected rate limit. Backing off for {backoff_time} seconds')
                time.sleep(backoff_time)
                backoff_time = backoff_time * 2
            elif 500 <= status_code <= 599:
                self.send_to_log(f"Got {status_code}; retrying in {backoff_time} seconds")
                time.sleep(backoff_time)
                backoff_time = backoff_time * 2

        return response

    async def get_asset_list_by_metadata(self, asset_name: str) -> list[Asset]:
        remaining = 1
        cursor = None
        all_assets: list[Asset] = []
        while remaining > 0:
            params = {'name': asset_name }
            if cursor is not None:
                params['cursor'] = cursor

            asset_list_response = await self.rate_limited_request(BASE_URL + '/assets', HEADERS, params)

            asset_list_response = asset_list_response.json()
            remaining = asset_list_response['remaining']
            cursor = asset_list_response['cursor']
            assets = [await create_asset(asset_dict) for asset_dict in asset_list_response['result']]
            all_assets.extend(assets)

            if self.test_mode and len(all_assets) >= TEST_LIMIT:
                break

        return all_assets

    async def get_asset_list_by_blueprint(self, blueprint: str) -> list[Asset]:
        all_assets: list[Asset] = []

        matching_blueprints = await Blueprint.filter(name=blueprint)
        progress_box = self.app.query_one("#progress", Static)

        progress_bar_label = Label("Getting asset details")
        await progress_box.mount(progress_bar_label)
        progress_bar = ProgressBar(total=len(matching_blueprints))
        await progress_box.mount(progress_bar)

        for matching_blueprint in matching_blueprints:
            asset = await Asset.get_or_none(
                blueprint=matching_blueprint
            )
            if asset is None:
                asset_detail_response = await self.rate_limited_request(BASE_URL + f'/assets/{matching_blueprint.token_address}/{matching_blueprint.token_id}', HEADERS, None)
                asset = await create_asset(asset_detail_response.json())
                asset.blueprint = matching_blueprint
                await asset.save()

            all_assets.append(asset)
            progress_bar.advance(1)

        await progress_bar_label.remove()
        await progress_bar.remove()

        return all_assets

    async def get_blueprint_of_asset(self, asset) -> Blueprint:
        full_mintable_token_url = f"{BASE_URL}/mintable-token/{asset.token_address}/{asset.token_id}"
        mintable_token = await self.rate_limited_request(full_mintable_token_url, HEADERS, None)
        blueprint = mintable_token.json()['blueprint']
        try:
            split_blueprint = blueprint.split(',')
            blueprint_name = split_blueprint[0]
            blueprint_edition = split_blueprint[1]
        except Exception:
            blueprint_name = None
            blueprint_edition = None

        blueprint, created = await Blueprint.get_or_create(
            blueprint=blueprint,
            defaults=dict(
                name=blueprint_name,
                edition=blueprint_edition,
                token_address=asset.token_address,
                token_id=asset.token_id
            )
        )

        return blueprint

    async def get_asset_details(self, token_address: str, token_id:str, get_first_non_mint_user: bool) -> Asset:
        print(token_id)
        asset_id = f"{token_address}-{token_id}"
        asset = await Asset.get_or_none(id=asset_id)
        if asset is None:
            print("HERE")
            asset_detail_response = await self.rate_limited_request(
                BASE_URL + f'/assets/{token_address}/{token_id}', HEADERS, None)
            print(asset_detail_response.json())
            asset = await create_asset(asset_detail_response.json())
            blueprint = await self.get_blueprint_of_asset(asset)
            asset.blueprint = blueprint
            await asset.save()

        # This updates the asset with the first non-mint user
        if (not asset.checked_first_non_mint_address) and get_first_non_mint_user:
            asset, _ = await self.get_transfer_history_of_asset(asset)
            await asset.save()

        return asset

    async def get_transfer_history_of_user(self, user_address: str, direction: str, get_first_non_mint_user: bool) -> tuple[list[Transfer], list[Asset]]:
        progress_box = self.app.query_one("#progress", Static)
        loading_indicator_label = Pretty(f"Getting transfer history {direction} (0 transfers so far)")
        await progress_box.mount(loading_indicator_label)
        loading_indicator = LoadingIndicator()
        await progress_box.mount(loading_indicator)

        self.send_to_log(f"Getting transfer {direction} history")

        transfer_history: list[Transfer] = []
        all_assets: list[Asset] = []
        remaining = 1
        cursor = None
        total_transfers = 0
        while remaining > 0:
            if direction == 'out':
                params = {'user': user_address}
            else:
                params = {'receiver': user_address}
            if cursor is not None:
                params['cursor'] = cursor
            transfers_response = await self.rate_limited_request(BASE_URL + '/transfers', HEADERS, params)
            transfers_response = transfers_response.json()
            remaining = transfers_response['remaining']

            cursor = transfers_response['cursor']
            for transfer_dict in transfers_response['result']:
                asset_token_address = transfer_dict['token']['data']['token_address']
                asset_token_id = transfer_dict['token']['data']['token_id']
                asset = await self.get_asset_details(asset_token_address, asset_token_id, get_first_non_mint_user)

                transfer, _ = await Transfer.get_or_create(
                    transaction_id=transfer_dict['transaction_id'],
                    defaults=dict(
                        receiver=transfer_dict['receiver'],
                        status=transfer_dict['status'],
                        timestamp=transfer_dict['timestamp'],
                        user=transfer_dict['user'],
                        asset=asset
                    )
                )
                transfer_history.append(transfer)
                all_assets.append(asset)

                total_transfers += 1
                loading_indicator_label.update(f"Getting transfer history {direction} ({total_transfers} transfers so far)")

            if self.test_mode and len(transfer_history) >= TEST_LIMIT:
                break

        await loading_indicator_label.remove()
        await loading_indicator.remove()
        self.send_to_log(f"Successfully got transfer {direction} history")

        return transfer_history, all_assets

    async def get_transfer_history_of_asset(self, asset: Asset) -> tuple[Asset, list[Transfer]]:
        current_holder = asset.user
        prior_holder = None
        num_transfers = 0
        transfers_querystring = {
             'token_address': asset.token_address,
             'token_id': asset.token_id,
             'direction': 'desc'
        }
        transfers_response = await self.rate_limited_request(BASE_URL + '/transfers', HEADERS, transfers_querystring)
        response_json = transfers_response.json()['result']

        transfer_history: list[Transfer] = []
        for transfer_dict in response_json:
            num_transfers += 1
            transfer, _ = await Transfer.get_or_create(
                transaction_id=transfer_dict['transaction_id'],
                defaults=dict(
                    receiver=transfer_dict['receiver'],
                    status=transfer_dict['status'],
                    timestamp=transfer_dict['timestamp'],
                    user=transfer_dict['user'],
                    asset=asset
                )
            )

            transfer_history.append(transfer)
            if current_holder == transfer.receiver:
                prior_holder = current_holder
                current_holder = transfer.user
            else:
                raise ValueError(f'Attempted to transfer {asset}, but history did not match final owner')

        asset.mint_address = current_holder
        asset.first_non_mint_address = prior_holder
        asset.num_transfers = num_transfers
        asset.checked_first_non_mint_address = True  # We need this because sometimes we have checked but there is no first non-mint user

        return asset, transfer_history

    async def get_minted_assets(self, user_address: str, get_first_non_mint_user: bool) -> list[Asset]:
        progress_box = self.app.query_one("#progress", Static)
        loading_indicator_label = Pretty("Getting mints (0 so far)")
        await progress_box.mount(loading_indicator_label)
        loading_indicator = LoadingIndicator()
        await progress_box.mount(loading_indicator)

        self.send_to_log("Getting mints")

        minted_assets: list[Asset] = []
        total_mints = 0
        remaining = 1
        cursor = None

        while remaining > 0:
            params = {'user': user_address}
            if cursor is not None:
                params['cursor'] = cursor
            mints_response = await self.rate_limited_request(BASE_URL + '/mints', HEADERS, params)

            mints_response = mints_response.json()
            remaining = mints_response['remaining']

            cursor = mints_response['cursor']
            for mint in mints_response['result']:
                asset_token_address = mint['token']['data']['token_address']
                asset_token_id = mint['token']['data']['token_id']
                asset = await self.get_asset_details(asset_token_address, asset_token_id, get_first_non_mint_user)
                minted_assets.append(asset)

                total_mints += 1
                loading_indicator_label.update(f"Getting mints ({total_mints} so far)")

            if self.test_mode and len(minted_assets) >= TEST_LIMIT:
                break

        await loading_indicator_label.remove()
        await loading_indicator.remove()
        self.send_to_log(f"Successfully got mints")

        return minted_assets

    async def asset_search(self, asset_name: str, search_type: str, get_transfer_history: bool = True) -> tuple[list[Asset], list[Transfer]]:
        full_transfer_history: list[Transfer] = []

        progress_box = self.app.query_one("#progress", Static)
        if search_type is None:
            search_type = "blueprint"

        loading_indicator_label = Label(f"Getting assets by {search_type}")
        await progress_box.mount(loading_indicator_label)
        loading_indicator = LoadingIndicator()
        await progress_box.mount(loading_indicator)

        if search_type == "metadata":
            all_assets = await self.get_asset_list_by_metadata(asset_name)
        else:
            self.send_to_log("WARNING: This only searches assets in the pre-populated blueprints database! Use the blueprint prefetch option to populate it")
            all_assets = await self.get_asset_list_by_blueprint(asset_name)

        await loading_indicator_label.remove()
        await loading_indicator.remove()

        progress_bar_label = Label("Getting asset details")
        progress_bar = ProgressBar(
            total=len(all_assets),
        )
        await progress_box.mount(progress_bar_label)
        await progress_box.mount(progress_bar)
        for asset in all_assets:
            blueprint = await self.get_blueprint_of_asset(asset)
            asset.blueprint = blueprint
            if get_transfer_history:
                asset, transfer_history = await self.get_transfer_history_of_asset(asset)
            else:
                transfer_history = []
            await asset.save()
            full_transfer_history += transfer_history
            progress_bar.advance(1)

            if self.test_mode:
                break

        await progress_bar_label.remove()
        await progress_bar.remove()

        return all_assets, full_transfer_history

    async def blueprint_prefetch(self, token_address: str, starting_token_id: int, ending_token_id: int):
        progress_box = self.app.query_one("#progress", Static)
        progress_bar_label = Label("Getting asset details")
        await progress_box.mount(progress_bar_label)
        progress_bar = ProgressBar(total=ending_token_id - starting_token_id)
        await progress_box.mount(progress_bar)

        for token_id in range(starting_token_id, ending_token_id):
            await self.get_asset_details(token_address, str(token_id), False)
            progress_bar.advance(1)

        await progress_bar_label.remove()
        await progress_bar.remove()
