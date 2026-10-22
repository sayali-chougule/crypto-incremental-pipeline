import requests

BASE_URL = "https://api.coinpaprika.com/v1"


def get_coin_ticker(coin_id):
    url = f"{BASE_URL}/tickers/{coin_id}"

    response = requests.get(
        url,
        params={"quotes": "USD"},
        timeout=30
    )

    response.raise_for_status()
    return response.json()


if __name__ == "__main__":
    data = get_coin_ticker("btc-bitcoin")

    print("Coin:", data["name"])
    print("Symbol:", data["symbol"])
    print("Price USD:", data["quotes"]["USD"]["price"])
    print("Last Updated:", data["last_updated"])