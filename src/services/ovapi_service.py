import os

import requests
from dotenv import load_dotenv

load_dotenv()


class Ovapi:

    OVAPI_URL: str = str(os.getenv("OVAPI_URL"))

    @staticmethod
    def get_data() -> dict:
        """Returns data from OV's API.

        Returns:
            dict: list of available lines
        """
        response = requests.get(Ovapi.OVAPI_URL)
        json_data = response.json()

        return json_data
