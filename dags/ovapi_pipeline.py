from src.services.ovapi_service import Ovapi

data = Ovapi.get_data()

print(data['ARR_26007_2'])
