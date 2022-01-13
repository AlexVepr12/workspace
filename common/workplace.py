class ExportData:
    def __init__(self, names, tokens):
        self.BASE_URL = 'https://graph.facebook.com/'
        self.ACCESS = {f'{i}': {'name': names[i], 'access_token': tokens[i]} for i in range(len(names))}
