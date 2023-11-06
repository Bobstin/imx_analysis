import pytz
import httpx
import time
from datetime import datetime
from textual.app import App
from textual.widgets import RichLog, Static, ProgressBar, Label, LoadingIndicator

from models import Asset, Blueprint, Transfer
from deserializers import create_asset

BASE_URL = "https://api.x.immutable.com/v1"
HEADERS = {"Content-Type": "application/json"}


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
            async with httpx.AsyncClient(verify=False) as client:
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

    async def get_asset_lists_by_metadata(self, asset_name: str, get_transfer_history: bool = True) -> tuple[list[Asset], list[Transfer]]:
        remaining = 1
        cursor = None
        all_assets: list[Asset] = []
        full_transfer_history: list[Transfer] = []

        progress_box = self.app.query_one("#progress", Static)

        loading_indicator_label = Label("Getting assets")
        await progress_box.mount(loading_indicator_label)
        loading_indicator = LoadingIndicator()
        await progress_box.mount(loading_indicator)
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

            if self.test_mode and len(all_assets) >= 100:
                break
        await loading_indicator_label.remove()
        await loading_indicator.remove()

        progress_bar_label = Label("Getting asset details")
        progress_bar = ProgressBar(
            total=len(all_assets),
        )
        await progress_box.mount(progress_bar_label)
        await progress_box.mount(progress_bar)
        for asset in all_assets:
            print(asset.token_id)
            blueprint = await self.get_blueprint_of_asset(asset)
            asset.blueprint = blueprint
            if get_transfer_history:
                asset, transfer_history = await self.get_transfer_history_of_asset(asset)
            else:
                transfer_history = []
            await asset.save()
            full_transfer_history += transfer_history
            progress_bar.advance(1)

        await progress_bar_label.remove()
        await progress_bar.remove()

        return all_assets, full_transfer_history

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

        return asset, transfer_history
