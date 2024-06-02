import typer
from typing_extensions import Annotated
from rich.progress import track
from rich.console import Console
from rich.table import Table
import requests
import concurrent.futures
from pathlib import Path
from datetime import datetime, timedelta, timezone

app = typer.Typer()
DOWNLOAD_PATH = "./download/"

@app.command()
def download(assets:Annotated[list[str], typer.Argument(help="Give a list of assets to download. Eg. EURUSD AUDUSD")],
             start:Annotated[str, typer.Argument(help="Start date to download in YYYY-MM-DD format. Eg. 2024-01-08")],
             end:Annotated[str, typer.Option(help="End date to download in YYYY-MM-DD format. If not provided, will download until current date Eg. 2024-01-08")]="",
             concurrent:Annotated[int, typer.Option(help="Max number of concurrent downloads (defaults to max number of threads + 4 or 32 (which ever is less)) (Sometimes using too high of a number results in missing files)")]=0,
             force:Annotated[bool, typer.Option(help="Redownload files. By default, without this flag, files that already exist will be skipped")]=False):
    start_date_str = start.split("-")
    end_date = datetime.now(timezone.utc).replace(tzinfo=None) - timedelta(hours=1)
    base_url = "https://datafeed.dukascopy.com/datafeed/"

    if end != "":
        end_date = end.split("-")
        end_date = datetime(int(end_date[0]), int(end_date[1]), int(end_date[2]))

    processes = None
    if concurrent > 0:
        processes = concurrent

    delta = timedelta(hours=1)
    for asset in assets:
        filenames = []
        urls = []
        forces = []

        start_date = datetime(int(start_date_str[0]), int(start_date_str[1]), int(start_date_str[2]))
        while start_date <= end_date:
            year = start_date.year
            month = start_date.month-1
            day = start_date.day
            hour = start_date.hour

            filenames.append(Path(f"{DOWNLOAD_PATH}{asset}/{year}/{month:0>2}/{day:0>2}/{hour:0>2}h_ticks.bi5"))
            urls.append(f"{base_url}{asset}/{year}/{month:0>2}/{day:0>2}/{hour:0>2}h_ticks.bi5")
            forces.append(force)

            start_date += delta
        inputs = zip(filenames, urls, forces)
        download_file_parallel(inputs, asset, len(filenames), processes)
    print("Download completed")

def download_file_parallel(file_url_zip, asset:str, length:int, processes_num=None):
    with concurrent.futures.ThreadPoolExecutor(max_workers=processes_num) as executor:
        args_list = tuple(file_url_zip)
        results = [executor.submit(download_file, args) for args in args_list]
        for _ in track(concurrent.futures.as_completed(results), total=length, description=f"Downloading {asset}..."):
            pass

def download_file(args):
    filename, url, is_force = args[0], args[1], args[2]

    if filename.exists() and not is_force:
        return

    r = requests.get(url)
    if not r:
        print(f"Error: {r} for {url}")
        return

    filename.parent.mkdir(exist_ok=True, parents=True)

    with open(filename, 'wb') as f:
        f.write(r.content)

@app.command()
def export():
    pass

@app.command("list")
def list_command():
    dirs = Path(DOWNLOAD_PATH).glob("*/*/*/*")
    assets = {}
    for dir in dirs:
        parts = dir.parts
        asset = parts[1]
        if asset not in assets:
            assets[asset] = []
        assets[asset].append(datetime(int(parts[2]), int(parts[3])+1, int(parts[4])))

    table = Table(title="Downloaded Data")

    table.add_column("Asset")
    table.add_column("Start Date (YYYY-MM-DD)")
    table.add_column("End Date (YYYY-MM-DD)")

    for asset in assets:
        table.add_row(asset, min(assets[asset]).strftime("%Y-%m-%d"), max(assets[asset]).strftime("%Y-%m-%d"))

    console = Console()
    console.print(table)

if __name__ == "__main__":
    app()
