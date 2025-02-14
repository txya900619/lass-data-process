import csv
import os
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Dict

import tqdm
from aac_datasets.datasets.functional.audiocaps import _download_from_youtube_and_verify
from torch.hub import download_url_to_file

CSV_URL = "https://raw.githubusercontent.com/hche11/VGGSound/refs/heads/master/data/vggsound.csv"


def main(args):
    root = args.root
    vggsound_root = os.path.join(root, "VGGSound")
    os.makedirs(vggsound_root, exist_ok=True)

    download_url_to_file(CSV_URL, os.path.join(vggsound_root, "vggsound.csv"))

    download_kwds = {}
    with open(os.path.join(vggsound_root, "vggsound.csv"), "r") as f:
        reader = csv.reader(f)

        for row in reader:
            yt_id, start, caption, subset = row
            filename = f"{yt_id}_{start}.wav"
            download_kwds[filename] = {
                "youtube_id": yt_id,
                "start_time": start,
                "fname": filename,
            }

    audio_root = os.path.join(vggsound_root, "audio")
    os.makedirs(audio_root, exist_ok=True)

    common_kwds: Dict[str, Any] = dict(
        audio_subset_dpath=audio_root,
        verify_files=False,
        present_audio_fpaths=os.listdir(audio_root),
        audio_duration=10,
        sr=16_000,
        audio_n_channels=1,
        ffmpeg_path=None,
        ytdlp_path=None,
        verbose=1,
    )

    with ThreadPoolExecutor(max_workers=args.max_workers) as executor:
        submitted_dict = {
            fname: executor.submit(
                _download_from_youtube_and_verify,
                id=i % args.max_workers,
                **kwds,
                **common_kwds,
            )
            for i, (fname, kwds) in enumerate(download_kwds.items())
        }
        for i, (fname, submitted) in enumerate(tqdm.tqdm(submitted_dict.items())):
            file_exists, download_success, valid_file = submitted.result()

    os.makedirs(audio_root, exist_ok=True)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--root", type=str, default="Corpora")
    parser.add_argument("--max_workers", type=int, default=64)
    args = parser.parse_args()
    main(args)
