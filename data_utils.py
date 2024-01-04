from pathlib import Path

from constants import DATA_PATH


class DataUtils:
    @staticmethod
    def create_dir(dir: Path):
        dir.mkdir(parents=True, exist_ok=True)

    @staticmethod
    def dump_data(file: Path, data: str):
        output_file = DATA_PATH / file
        output_file.write_text(data)

    @staticmethod
    def create_file(file: Path):
        path = DATA_PATH / file
        path.touch()

    @staticmethod
    def delete_file(file: Path):
        path = DATA_PATH / file
        path.unlink()

    @staticmethod
    def analyze_file(file: Path):
        path = DATA_PATH / file
        print(f'file {file} stats:')
        print(Path(path).stat())
