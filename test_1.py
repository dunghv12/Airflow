import click
import argparse


parser=argparse.ArgumentParser()
parser.add_argument('--name',type=str,required=True)
parser.add_argument('--age',type=int,required=True)
args=parser.parse_args()
print(args.name)
print(args.age)