import json
import click
import os
import pandas as pd


@click.command()
@click.option(
    '--input_path',
    type=click.Path(dir_okay=False),
    required=True,
    help="Path read data movies"
    )
@click.option(
    '--output_path',
    type=click.Path(dir_okay=False),
    required=True,
    help="Path export data movies"
    )

def main(input_path, output_path):
    print(input_path)
    data=pd.read_json(input_path)
    ranking=data.loc[data.rating>3,:].groupby(["movieId","rating"])\
                .agg(
                    count_rating=pd.NamedAgg(column="rating", aggfunc="count"),
                )\
                .sort_values("count_rating",ascending=False)
    dir_path=os.path.dirname(output_path)
    os.makedirs(dir_path,exist_ok=True)
    ranking.to_csv(output_path,index=True)
if __name__ == "__main__":
    main()
