import csv

from aac_datasets import WavCaps

SUBSETS = ["bbc", "soundbible", "audioset_no_audiocaps", "freesound_no_clotho_v2"]
REMAIN_KETS = ["id", "fpath", "duration", "subset", "captions"]


def main(args):
    filtered_data = {key: [] for key in REMAIN_KETS}
    for subset in SUBSETS:
        dataset = WavCaps(args.input_path, subset=subset)
        raw_data = dataset._raw_data
        for i in range(len(raw_data["fpath"])):
            if raw_data["duration"][i] > args.max_duration:
                continue
            for key in REMAIN_KETS:
                if isinstance(raw_data[key][i], list) and len(raw_data[key][i]) == 1:
                    filtered_data[key].append(raw_data[key][i][0])
                    continue
                filtered_data[key].append(raw_data[key][i])

    print(f"Filtered data size: {len(filtered_data['fpath'])}")
    print(f"Filtered data duration: {sum(filtered_data['duration']) / 3600:.2f} hours")

    with open(args.output_csv, "w") as f:
        writer = csv.writer(f)
        writer.writerow(REMAIN_KETS)
        for i in range(len(filtered_data["fpath"])):
            writer.writerow([filtered_data[key][i] for key in REMAIN_KETS])


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--input_path", type=str, default="../Corpora")
    parser.add_argument("--max_duration", type=float, default=10)
    parser.add_argument("--output_csv", type=str, default="filtered_wavcaps.csv")
    args = parser.parse_args()
    main(args)
