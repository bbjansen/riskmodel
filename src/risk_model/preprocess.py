"""Preprocess JSON files."""

from risk_model.storage import make_folder
import os
import json
from itertools import chain, islice


def make_readable_json(
    unprocessed_filename: str = 'data/mongo/incorporation-processes.json',
    preprocessed_dir: str = 'data/preprocess/incorporation_processes/',
    split_variabele: str = '{"_id":',
    chunk_size=4096
):
    """Reformat unreadable oneline JSON to python readable format."""
    make_folder(preprocessed_dir)
    processed_filename = os.path.join(
        preprocessed_dir, unprocessed_filename.split('/')[-1])

    with open(unprocessed_filename) as f_read:
        with open(processed_filename, 'w') as f_write:
            for chunk in each_chunk(f_read, chunk_size, split_variabele):
                format_json = split_variabele + chunk
                if format_json != split_variabele and len(format_json) > 0:
                    try:
                        reformated_json = json.loads(format_json)
                        f_write.write('{}\n'.format(
                            json.dumps(reformated_json)))
                    except json.JSONDecodeError:
                        print('Fail to parse')
                        pass


def each_chunk(
    stream,
    chunk_size: int,
    separator: str
):
    """Separates the oneline JSON into multiple JSON lines."""
    buffer = ''
    while True:
        chunk = stream.read(chunk_size)
        if not chunk:
            yield buffer
            break

        buffer += chunk
        while True:
            try:
                part, buffer = buffer.split(separator, 1)
            except ValueError:
                break
            else:
                yield part


def chunks(
    iterable,
    n
):
    "chunks(ABCDE,2) => AB CD E"
    iterable = iter(iterable)
    while True:
        try:
            yield chain([next(iterable)], islice(iterable, n - 1))
        except StopIteration:
            return


def split_file_into_multiple_files(
    directory='data/preprocess/incorporation_processes',
    file_to_split='incorporation-processes.json',
    new_sub_file_name='processes',
    n_files=35,
    file_type='json'
):
    "Splits big file into separate files."

    file = os.path.join(directory, file_to_split)
    num_lines = sum(1 for line in open(file))
    n_lines = round(num_lines / n_files)

    with open(file) as bigfile:
        for i, lines in enumerate(chunks(bigfile, n_lines)):
            file_split = os.path.join(
                directory, '{}_{}.{}'.format(
                    new_sub_file_name, i, file_type))

            with open(file_split, 'w') as f:
                f.writelines(lines)
