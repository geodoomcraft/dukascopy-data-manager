import typer
from typing_extensions import Annotated
from rich.progress import track
import requests
from multiprocessing import Pool
from pathlib import Path
from datetime import datetime, timedelta
import os

app = typer.Typer()

@app.command()
def download(assets:Annotated[list[str], typer.Argument(help="Give a list of assets to download. Eg. EURUSD AUDUSD")],
             start:Annotated[str, typer.Argument(help="Start date to download in YYYY-MM-DD format. Eg. 2024-01-08")],
             end:Annotated[str, typer.Option(help="End date to download in YYYY-MM-DD format. If not provided, will download until current date Eg. 2024-01-08")]="",
             concurrent:Annotated[int, typer.Option(help="Max number of concurrent downloads (automatically limited by available threads)")]=0,
             force:Annotated[bool, typer.Option(help="Redownload files. By default, without this flag, files that already exist will be skipped")]=False):
    start_date_str = start.split("-")
    end_date = datetime.today()

    if end != "":
        end_date = end.split("-")
        end_date = datetime(int(end_date[0]), int(end_date[1]), int(end_date[2]))

    processes = os.cpu_count()
    if concurrent > 0 and processes != None:
        if concurrent < processes:
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

            filenames.append(Path(f"./download/{asset}/{year}/{month:0>2}/{day:0>2}/{hour:0>2}h_ticks.bi5"))
            urls.append(f"https://datafeed.dukascopy.com/datafeed/{asset}/{year}/{month:0>2}/{day:0>2}/{hour:0>2}h_ticks.bi5")
            forces.append(force)

            start_date += delta
        inputs = zip(filenames, urls, forces)
        download_file_parallel(inputs, asset, len(filenames), processes)
    print("Download completed")

def download_file_parallel(file_url_zip, asset:str, length:int, processes_num=os.cpu_count()):
    with Pool(processes=processes_num) as pool:
        for i in track(pool.imap_unordered(download_file, file_url_zip), total=length, description=f"Downloading {asset}..."):
            pass

def download_file(args):
    filename, url, is_force = args[0], args[1], args[2]

    if filename.exists() and not is_force:
        return

    r = requests.get(url)
    if not r:
        return

    filename.parent.mkdir(exist_ok=True, parents=True)

    with open(filename, 'wb') as f:
        f.write(r.content)

@app.command()
def export():
    pass

@app.command()
def list():
    pass

if __name__ == "__main__":
    app()
