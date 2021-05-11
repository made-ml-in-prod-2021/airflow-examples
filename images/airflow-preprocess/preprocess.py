import os
import pandas as pd
import click


@click.command("predict")
@click.option("--input-dir")
@click.option("--output-dir")
def preprocess(input_dir: str, output_dir):
    data = pd.read_csv(os.path.join(input_dir, "data.csv"))
    # do something instead
    data["features"] = 0

    os.makedirs(output_dir, exist_ok=True)
    data.to_csv(os.path.join(output_dir, "data.csv"))


if __name__ == '__main__':
    preprocess()