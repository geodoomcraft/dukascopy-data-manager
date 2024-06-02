import typer
from typing_extensions import Annotated
from rich.progress import track
from rich.console import Console
from rich.table import Table
import requests
import concurrent.futures
from pathlib import Path
from datetime import datetime, timedelta, timezone
import lzma
import numpy as np
import pandas as pd

app = typer.Typer()
DOWNLOAD_PATH = "./download/"
EXPORT_PATH = "./export/"

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
def export(assets:Annotated[list[str], typer.Argument(help="Give a list of assets to export. Use 'all' for all downloaded assets. Eg. EURUSD AUDUSD. Check export --help for more info")],
           timeframe:Annotated[str, typer.Argument(help="Timeframe to export. Format should be [Number][Unit] eg. 1h or 1t. Check export --help for more info about units.")],
           start:Annotated[str, typer.Argument(help="Start date to export in YYYY-MM-DD format. Eg. 2024-01-08")],
           end:Annotated[str, typer.Option(help="End date to export in YYYY-MM-DD format. If not provided, will export until current date Eg. 2024-01-08")]=""):
    """
    Export downloaded data into different timeframes/units.\n
    assets can be selected by listing multiple with a space dividing them or a single asset.\n
    Eg. export AUDUSD EURUSD\n
    Can also use all to select all downloaded assets.\n
    Eg. export all\n
    Available units:\n
        t: ticks (eg. 1t)\n
        s: seconds (eg. 10s)\n
        m: minutes (eg. 15m)\n
        h: hours (eg. 4h)\n
        D: days (eg. 2D)\n
        W: weeks (eg. 2W)\n
    """
    asset_list = []
    if assets[0] == "all":
        dirs = Path(DOWNLOAD_PATH).glob("*")
        for dir in dirs:
            parts = dir.parts
            asset_list.append(parts[1])
    else:
        asset_list = assets

    start_date_str = start.split("-")
    end_date = datetime.now(timezone.utc).replace(tzinfo=None) - timedelta(hours=1)

    if end != "":
        end_date = end.split("-")
        end_date = datetime(int(end_date[0]), int(end_date[1]), int(end_date[2]))

    delta = timedelta(hours=1)
    for asset in asset_list:
        filenames = []
        file_times = []

        start_date = datetime(int(start_date_str[0]), int(start_date_str[1]), int(start_date_str[2]))
        while start_date <= end_date:
            year = start_date.year
            month = start_date.month-1
            day = start_date.day
            hour = start_date.hour

            filenames.append(Path(f"{DOWNLOAD_PATH}{asset}/{year}/{month:0>2}/{day:0>2}/{hour:0>2}h_ticks.bi5"))
            file_times.append(datetime(year, month+1, day, hour))

            start_date += delta

        df_list = []
        
        for i in track(range(len(filenames)), description=f"Reading {asset} tick files..."):
            file = filenames[i]
            if not file.is_file():
                print(f"{file} is missing, skipping this file.")
                continue
            if file.stat().st_size == 0:
                continue
            dt = np.dtype([('TIME', '>i4'), ('ASKP', '>i4'), ('BIDP', '>i4'), ('ASKV', '>f4'), ('BIDV', '>f4')])
            data = np.frombuffer(lzma.open(file, mode="rb").read(),dt)
            df = pd.DataFrame(data)
            df["TIME"] = pd.to_datetime(df["TIME"], unit="ms", origin=file_times[i])
            df_list.append(df)

        df = pd.concat(df_list, ignore_index=True)
        df["ASKP"] = df["ASKP"].astype(np.int32)
        df["BIDP"] = df["BIDP"].astype(np.int32)
        df["ASKV"] = df["ASKV"].astype(np.float32)
        df["BIDV"] = df["BIDV"].astype(np.float32)

        df["ASKP"] = df["ASKP"] / 100_000
        df["BIDP"] = df["BIDP"] / 100_000

        console = Console()
        with console.status(f"Aggregating {asset} data...") as status:
            agg_df = aggregate_data(df, timeframe)
            console.print(f"{asset} data aggregated")

            status.update(f"Exporting {asset} to file...")

            export_file = Path(f"{EXPORT_PATH}{asset}.csv")
            export_file.parent.mkdir(exist_ok=True, parents=True)
            agg_df.to_csv(export_file, index=False)
            console.print(f"{asset} exported to {export_file}")

    print(f"Export completed. Data located at {Path(EXPORT_PATH).resolve()}")

def aggregate_data(df:pd.DataFrame, tf:str):
    if "t" in tf:
        tick_num = int(tf.split("t")[0])
        if tick_num == 1:
            return df
        df_group = df.groupby(df.index // tick_num)
        agg_df = pd.DataFrame()
        agg_df["date"] = df_group["TIME"].first()
        agg_df["open"] = df_group["BIDP"].first()
        agg_df["high"] = df_group["BIDP"].max()
        agg_df["low"] = df_group["BIDP"].min()
        agg_df["close"] = df_group["BIDP"].last()
        agg_df["vol"] = df_group["BIDV"].sum()
        return agg_df

    agg_time = pd.Timedelta(tf)

    df = df.set_index("TIME")
    df_group = df.resample(agg_time)

    agg_df = pd.DataFrame()
    agg_df["open"] = df_group["BIDP"].first()
    agg_df["high"] = df_group["BIDP"].max()
    agg_df["low"] = df_group["BIDP"].min()
    agg_df["close"] = df_group["BIDP"].last()
    agg_df["vol"] = df_group["BIDV"].sum()
    agg_df = agg_df.reset_index(names="date")

    agg_df = agg_df.dropna()
    return agg_df

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
