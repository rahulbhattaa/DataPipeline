from pycelonis import get_celonis
from urllib.request import getproxies

proxies=getproxies()
proxies = {
    "http://": "http://localhost:3128",
    "https://": "http://localhost:3128",
}
c = get_celonis(
    base_url="https://bosch.eu-4.celonis.cloud",
    api_token='MjQ4N2QzMTAtYmNhZi00YTRjLTg0NDQtMjgyNmYwMzcwMjA1OkExKzZGRzBDM1FiaVVzaDFQK2NEa3FaMTVvSFE5RjVPa1VITUR3Sm5EUzlV',
    key_type='USER_KEY',
    proxies=proxies,
    permissions=False)

a=c.data_integration.get_data_pool('2613346f-0185-4fea-81b2-24657c091a9c')
print(a)
