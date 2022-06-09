class StockMarket:

    def __init__(self, name: str):
        if type(name) != str:
            raise Exception(f'Type {type(name)} is not a string')
        self.name = name
    
    