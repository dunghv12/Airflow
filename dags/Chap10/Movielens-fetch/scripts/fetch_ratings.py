import json
import click
import requests
import os


@click.command()
@click.option(
    '--output_path',
    type=click.Path(dir_okay=False),
    required=True,
    help="Output file path"
    )
@click.option(
    "--base_url",
    type=str,
    default="http://movielens:5000",
    help="API take data"
)
@click.option(
    "--start_date",
    type=click.DateTime(formats=["%Y-%m-%d"]),
    required=True,
    help="Date to take data from API"
)
@click.option(
    "--end_date",
    type=click.DateTime(formats=["%Y-%m-%d"]),
    required=True,
    help="Date to take data from API"
)
@click.option(
    "--batch_size",
    type=int,
    default=100,
    help="Batch size to take data from API"
)

def main(output_path,base_url,start_date, end_date,batch_size):
    session=requests.Session()
    data=_get_ratings(
        session=session,
        base_url=base_url,
        start_date=start_date,
        end_date=end_date,
        batch_size=batch_size
    )
    print("co data",data)
    output_dir=os.path.dirname(output_path)
    os.makedirs(output_dir,exist_ok=True)
    with open(output_path, 'w') as f:
        json.dump(data,fp=f)
    f = open(output_path)
    data_w = json.load(f)
    print("sau khi viet xong",data_w)

def _get_ratings(session,base_url,start_date, end_date, batch_size=100):
    x=list(_get_with_pagination(
        session=session,
        url=base_url+"/ratings",
        params={"start_date":start_date.strftime("%Y-%m-%d")
                ,"end_date":end_date.strftime("%Y-%m-%d")},
        batch_size=batch_size
    ))
    return x

def _get_with_pagination(session, url, params, batch_size=100):
    """
    Fetches records using a get request with given url/params,
    taking pagination into account.
    """
    offset=0
    total=None
    while total is None or offset < total:
        response=session.get(
            url,
            params={**params,**{"offset": offset, "limit": batch_size}}
            )
        data=response.json()
        yield from data["result"]
        offset+=batch_size
        total=data["total"]
if __name__ == "__main__":
    main()
