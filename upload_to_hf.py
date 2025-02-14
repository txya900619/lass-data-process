from datasets import Audio, DatasetDict, load_dataset


def main(args):
    dataset = load_dataset(
        "csv", split="train", data_files=args.input_csv, delimiter=","
    )
    dataset_subsets = dataset.unique("subset")
    dataset_dict = DatasetDict()
    for subset in dataset_subsets:
        dataset_dict[subset] = dataset.filter(lambda x: x["subset"] == subset)

    dataset_dict = dataset_dict.rename_columns(
        {"captions": "caption", "fpath": "audio"}
    )
    dataset_dict = dataset_dict.remove_columns(["subset"])
    dataset_dict = dataset_dict.cast_column("audio", Audio(sampling_rate=16000))

    dataset_dict.push_to_hub(args.repo_name)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--input_csv", type=str, default="filtered_wavcaps.csv")
    parser.add_argument("--repo_name", type=str, default="wavcaps")
    args = parser.parse_args()
    main(args)
