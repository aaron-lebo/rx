import spacy
from spacy.tokens import DocBin
import typer

nlp = spacy.load('en_core_web_lg')

def main(file: str, i: int):
    bin = DocBin().from_disk(file)
    for j, d in enumerate(bin.get_docs(nlp.vocab)):
        if i == j:
            print(d.user_data)
            break

if __name__ == '__main__':
    typer.run(main)
