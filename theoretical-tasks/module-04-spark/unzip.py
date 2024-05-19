import zipfile
import os

_data_path = os.path.join(os.getcwd(), "data", "source")


def unzip_combine_weather_zips(weather_dir: str) -> None:
    """Unzipper for weather dataset.
    And combine all partitioned data into one directory and save its structure

    :param weather_dir: directory of weather file.
    """

    weather_subzips = [os.path.join(_data_path, weather_dir, file_name) for file_name in
                       os.listdir(os.path.join(_data_path, weather_dir))]

    for weather_subzip in weather_subzips:
        with zipfile.ZipFile(weather_subzip, "r") as weather_subfiles:
            weather_subfiles.extractall(f'{_data_path}/')


def unzip_restaurant_zip(restaurant_path: str) -> None:
    """Unzipper for restaurant dataset.

    :param restaurant_path: directory of weather file.
    """

    with zipfile.ZipFile(os.path.join(_data_path, restaurant_path), "r") as weather_subfiles:
        weather_subfiles.extractall(f'{_data_path}/')


if __name__ == "__main__":
    unzip_combine_weather_zips("weather_zip_folder")
    unzip_restaurant_zip("restaurant_csv.zip")
