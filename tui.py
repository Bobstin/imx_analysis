from pathlib import Path
from tortoise import Tortoise, run_async
from datetime import datetime

from textual import on, work
from textual.app import App, ComposeResult
from textual.widgets import Header, Static, Input, Select, Button, RichLog, Checkbox

from searches import Searcher
from utils import create_dir_if_not_exist, write_list_of_tortoise_objects_to_csv, write_list_of_dicts_to_csv, \
    create_transfer_summaries, create_transfer_output_files


class ImxApp(App):
    CSS_PATH = "main.tcss"
    BINDINGS = [("d", "quit", "Quit")]

    def __init__(self, *args, **kwargs):
        self.searcher = Searcher(self, True)
        self.output_dir = kwargs["output_dir"]
        del kwargs["output_dir"]
        super().__init__(*args, **kwargs)

    @on(Button.Pressed, "#run_asset_search")
    async def on_asset_search(self, event: Button.Pressed) -> None:
        self.asset_search()

    @work(exclusive=True)
    async def asset_search(self) -> None:
        asset_name_box = self.query_one("#asset_name", Input)
        original_asset_name = asset_name_box.value
        if original_asset_name == "":
            original_asset_name = "#100 Todd McFarlane Batman"

        search_type_box = self.query_one("#search_type", Select)
        search_type = search_type_box.value

        if search_type == "metadata":
            asset_name = original_asset_name.replace(" ", "_")
        else:
            asset_name = original_asset_name

        self.searcher.send_to_log(f"Getting asset data for {original_asset_name}")

        assets, transfers = await self.searcher.asset_search(asset_name, search_type)

        self.searcher.send_to_log(f"Data collected, creating outputs")

        file_prefix = f'{datetime.now().strftime("%Y%m%d_%H%M")} {original_asset_name}'
        await write_list_of_tortoise_objects_to_csv(self.output_dir / f"{file_prefix} assets.csv", assets)
        await write_list_of_tortoise_objects_to_csv(self.output_dir / f"{file_prefix} transfers.csv", transfers)

        self.searcher.send_to_log(f"Job complete")

    @on(Button.Pressed, "#run_user_search")
    async def on_user_search(self, event: Button.Pressed) -> None:
        self.user_search()

    @work(exclusive=True)
    async def user_search(self) -> None:
        user_address = self.query_one("#user_address", Input).value
        if user_address == "":
            user_address = "0x7be178ba43a9828c22997a3ec3640497d88d2fd3"

        get_transfers_out = self.query_one("#transfers_out", Checkbox).value
        get_transfers_in = self.query_one("#transfers_in", Checkbox).value
        get_mints = self.query_one("#mints", Checkbox).value
        get_first_non_mint = self.query_one("#first_non_mint", Checkbox).value

        file_prefix = f'{datetime.now().strftime("%Y%m%d_%H%M")} {user_address}'

        if get_transfers_out:
            transfer_out_history, assets_transferred_out = await self.searcher.get_transfer_history_of_user(user_address, "out", get_first_non_mint)
            await create_transfer_output_files(transfer_out_history, assets_transferred_out, 'out', self.output_dir, file_prefix)

        if get_transfers_in:
            transfer_in_history, assets_transferred_in = await self.searcher.get_transfer_history_of_user(user_address, "in", get_first_non_mint)
            await create_transfer_output_files(transfer_in_history, assets_transferred_in, 'in', self.output_dir, file_prefix)

        if get_mints:
            mints = await self.searcher.get_minted_assets(user_address, get_first_non_mint)
            await write_list_of_tortoise_objects_to_csv(self.output_dir / f"{file_prefix} minted assets.csv", mints)

        self.searcher.send_to_log(f"Job complete")

    def compose(self) -> ComposeResult:
        yield Header(id="header")
        yield Drawer(id="drawer")
        yield RichLog(id="log")
        yield Static("Progress", id="progress")

    async def action_quit(self) -> None:
        await Tortoise.close_connections()
        self.exit()


class Drawer(Static):
    @on(Select.Changed, "#analysis_type")
    async def on_analysis_type_change(self, event: Select.Changed) -> None:
        await self.query("#asset_name").remove()
        await self.query("#search_type").remove()
        await self.query("#run_asset_search").remove()
        await self.query("#user_address").remove()
        await self.query("#transfers_out").remove()
        await self.query("#transfers_in").remove()
        await self.query("#mints").remove()
        await self.query("#first_non_mint").remove()
        await self.query("#run_user_search").remove()

        if event.value == "asset":
            await self.mount(
        Input(placeholder="Asset name", id="asset_name"),
                Select(prompt="Search type", options=[("By blueprint", "blueprint"), ("By metadata name", "metadata")], id="search_type"),
                Button("Run search", variant="primary", id="run_asset_search"),
            )

        if event.value == "user":
            await self.mount(
        Input(placeholder="User address", id="user_address"),
                Checkbox("Get transfers out", id="transfers_out", value=True),
                Checkbox("Get transfers in", id="transfers_in"),
                Checkbox("Get mints", id="mints"),
                Checkbox("Get get first non-mint user", id="first_non_mint", value=True),
                Button("Run search", variant="primary", id="run_user_search"),
            )

    def compose(self) -> ComposeResult:
        yield Select(prompt="Analysis type", options=[("Asset search", "asset"), ("User search", "user")], id="analysis_type")


async def setup_db():
    await Tortoise.init(db_url="sqlite://db.sqlite3", modules={"models": ["models"]})
    await Tortoise.generate_schemas()


if __name__ == "__main__":
    output_dir = Path() / "output"
    create_dir_if_not_exist(output_dir)
    run_async(setup_db())
    app = ImxApp(output_dir=output_dir)
    app.run()
