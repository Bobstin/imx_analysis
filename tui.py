from pathlib import Path
from tortoise import Tortoise, run_async
from datetime import datetime

from textual import on, work
from textual.app import App, ComposeResult
from textual.widgets import Header, Static, Input, Select, Button, RichLog

from searches import Searcher
from utils import create_dir_if_not_exist, write_list_of_tortoise_objects_to_csv


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
        self.searcher.send_to_log(f"Getting asset list by metadata")
        original_asset_name = "#100 Todd McFarlane Batman"
        asset_name = "#100_Todd_McFarlane_Batman"
        assets, transfers = await self.searcher.get_asset_lists_by_metadata(asset_name)
        self.searcher.send_to_log(f"Data collected, creating outputs")

        file_prefix = f'{datetime.now().strftime("%Y%m%d_%H%M")} {original_asset_name}'
        await write_list_of_tortoise_objects_to_csv(self.output_dir / f"{file_prefix} assets.csv", assets)
        await write_list_of_tortoise_objects_to_csv(self.output_dir / f"{file_prefix} transfers.csv", transfers)

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
    def compose(self) -> ComposeResult:
        yield Input(placeholder="Asset name")
        yield Select(prompt="Search type", options=[("By blueprint", "blueprint"), ("By metadata name", "metadata")])
        yield Button("Run search", variant="primary", id="run_asset_search")


async def setup_db():
    await Tortoise.init(db_url="sqlite://db.sqlite3", modules={"models": ["models"]})
    await Tortoise.generate_schemas()


if __name__ == "__main__":
    output_dir = Path() / "output"
    create_dir_if_not_exist(output_dir)
    run_async(setup_db())
    app = ImxApp(output_dir=output_dir)
    app.run()
