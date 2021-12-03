import urllib
import requests
from urllib.request import urlopen, Request
import json
import time
import logging



import pygsheets as ps
import pandas as pd

TIMEOUT=10

class ZapperBalance(object):
    _baseUrl='https://api.zapper.fi/v1/protocols/'
    _walletUrl='tokens/balances'
    _convexUrl='convex/balances'
    # _address='0xdc72db066b35811dbdeb0b7d1b39852d14bc3450'
    _address='0x8c6c8c306fbcea9330e9dd6c18b8659bdf2445a4'
    # _apikey='5d1237c2-3840-4733-8e92-c5a58fe81b88'
    _apikey='96e0cc51-a62e-42ca-acee-910ea7d2a241'

    def setup_custom_logger(self,name, log_level=logging.DEBUG):
        formatter = logging.Formatter(fmt='%(asctime)s - %(levelname)s - %(module)s - %(message)s')
        handler = logging.StreamHandler()
        handler.setFormatter(formatter)
        logger = logging.getLogger(name)
        logger.setLevel(log_level)
        logger.addHandler(handler)
        return logger

    def getTime(self,Time):
        if len(str(float(Time)))>9:
            Time=str(Time)[0:10]
        timeStamp=int(Time)
        timeArray = time.localtime(timeStamp)
        otherStyleTime = time.strftime("%Y-%m-%d_%H:%M:%S", timeArray)
        return otherStyleTime

    def _http_get_request(self, params={},url=None):
        headers = {
            'User-Agent': 'PostmanRuntime/7.28.4',
            'Content-type': 'application/json; charset=utf-8',
        }
        postdata=''
        for key,value in params.items():
            postdata+=key
            postdata+='='
            postdata+=value
            postdata+='&'
        url+="?"+postdata[:-1]
        request = Request(url=url, headers=headers,unverifiable=True)
        while True:
            try:
                #本地访问
                content = urlopen(request, timeout=30).read()
                #服务器访问（ssl验证）
                # content = urlopen(url,context=ssl._create_unverified_context(), timeout=30).read()
                # print(type(content))
                logger.debug(content)
                break
            except Exception as e:
                print("(get)Http Error try to resend in one second error: {} \n url:{}".format(e,url))
                time.sleep(1)
        content = content.decode('utf-8')
        json_data = json.loads(content)
        return json_data

    def _get_wallet_tokens(self,):
        params={
            "addresses%5B%5D":self._address,
            'network':'ethereum',
            "api_key":self._apikey
        }
        result=self._http_get_request(params,self._baseUrl+self._walletUrl)
        assets=result[self._address]['products'][0]['assets']
        meta=result[self._address]['meta']
        total_value=next(filter(lambda x:x['label']=='Total',iter(meta)))['value']
        l=[]
        for i in assets:
            l.append([i['symbol'],round(i['balance'],2),i['price'],round(i['balanceUSD'],2)])
        return l,total_value

    def _get_convex_tokens(self,):
        params={
            "addresses%5B%5D":self._address,
            'network':'ethereum',
            "api_key":self._apikey
        }
        result=self._http_get_request(params,self._baseUrl+self._convexUrl)
        assets=result[self._address]['products'][0]['assets']
        meta=result[self._address]['meta']
        total_value=next(filter(lambda x:x['label']=='Total',iter(meta)))['value']
        # l=[]
        # for i in assets:
        #     balanceUSD=i['balanceUSD']
        #     if balanceUSD > 0:
        #         tokens=i['tokens']
        #         for k in tokens:
        #             l.append([k['label'] if 'label' in k.keys() and k['label'] else k['symbol'],k['balance'],k['price'],k['balanceUSD']])
        convex_list=[]
        self.get_vonvex_data(assets,convex_list)
        return convex_list,total_value

    def get_vonvex_data(self,assets,convex_list):
        if len(assets)==0:
            # print(convex_list)
            return
        else:
            for i in assets:
                if i['balanceUSD']==0:
                    return self.get_vonvex_data(assets[1:],convex_list)
                elif 'tokens' in i:
                    return self.get_vonvex_data(i['tokens']+assets[1:],convex_list)
                else:
                    symbol_list=[x[0] for x in convex_list]
                    if i['symbol'] in symbol_list:
                        index=symbol_list.index(i['symbol'])
                        convex_list[index][1]+='+'
                        convex_list[index][1]+=str(round(i['balance'],2))
                        convex_list[index][3]+='+'
                        convex_list[index][3]+=str(round(i['balanceUSD'],2))
                        return self.get_vonvex_data(assets[1:],convex_list)
                    else:
                        convex_list.append([i['symbol'],'='+str(round(i['balance'],2)),i['price'],'='+str(round(i['balanceUSD'],2))])
                        return self.get_vonvex_data(assets[1:],convex_list)               
                    
    def get_balances(self,):
        wallet_balance,wallet_value=self._get_wallet_tokens()
        convex_balance,convex_value=self._get_convex_tokens()
        return wallet_balance+convex_balance,round(wallet_value+convex_value,2)

def getIndex():
    #global maxVolume 
    while True:
        try:
            df = pd.DataFrame(wks.get_all_records())
            break
        except:
            print("google sheet read error, retry in 5 seconds")
            time.sleep(5)
    return len(df)+2

zb=ZapperBalance()
logger=zb.setup_custom_logger('ZapperBalance')

# client = ps.authorize(service_file = "koin-294706-4d2a55d671fc.json")
client = ps.authorize(service_file = "defi-334000-410407de395b.json")

sh = client.open('defi-database')
wks = sh.sheet1

while True:
    # clock_12_at_morning='04:00:'
    # UTC time
    clock_12_at_morning='16:21:'
    current_for_seconds=zb.getTime(time.time())
    print(current_for_seconds[11:17])
    if current_for_seconds[11:17]==clock_12_at_morning:
        
        result,value=zb.get_balances()
        logger.debug("get data {}".format(result))
        wksIndex = getIndex()
        #record NY Time
        wks.update_value(addr="A"+str(wksIndex), val=zb.getTime(time.time()-18000)[:14]+'00:00')
        wks.update_value(addr="B"+str(wksIndex), val=value)
        for i in range(0,len(result)):
            for k in range(0,len(result[i])-1):
                index=(len(result[i])-1)*i+k
                addr=chr(65+2+index)+str(wksIndex) if 65+2+index<=90 else chr(64+int(((65+2+index)/90)))+chr(64+(65+2+index)%90)+str(wksIndex)
                wks.update_value(addr=addr, val=result[i][k+1])
        logger.debug('set data finish')
    else:
        logger.debug(current_for_seconds)
        logger.debug('waiting...... ')
    time.sleep(60)
    
"""
_get_wallet_tokens respond
{"0xdc72db066b35811dbdeb0b7d1b39852d14bc3450":{"products":[{"label":"Tokens","assets":[{"type":"base","category":"wallet","network":"ethereum","address":"0x0000000000000000000000000000000000000000","symbol":"ETH","decimals":18,"label":"ETH","img":"https://storage.googleapis.com/zapper-fi-assets/tokens/ethereum/0x0000000000000000000000000000000000000000.png","hide":false,"canExchange":false,"price":4733.52,"balance":4.26791546231532,"balanceRaw":"4267915462315319569","balanceUSD":20202.263199178815}],"meta":[]}],"meta":[{"label":"Total","value":20202.263199178815,"type":"dollar"},{"label":"Assets","value":20202.263199178815,"type":"dollar"},{"label":"Debt","value":0,"type":"dollar"}]}}
"""
"""
_get_convex_tokens respond
{"0xdc72db066b35811dbdeb0b7d1b39852d14bc3450":{"products":[{"label":"Staked","assets":[{"type":"farm","tokens":[{"metaType":"staked","type":"vault","network":"ethereum","address":"0x74b79021ea6de3f0d1731fb8bdff6ee7df10b8ae","symbol":"cvxcrvRenWBTC","label":"renBTC Curve in Convex","decimals":18,"supply":13387.308538167757,"price":67157.6938692403,"pricePerShare":1,"liquidity":899060768.5393372,"appId":"convex","tokens":[{"type":"pool","category":"pool","network":"ethereum","symbol":"renBTC Curve","address":"0x49849c98ae39fff122806c06791fa73784fb3675","exchangeAddress":"0x93054188d876f558f4a66b2ef1d97d16edf0895b","label":"renBTC Curve","decimals":18,"appId":"curve","price":67157.6938692403,"liquidity":988404529.196502,"supply":14719.297589073178,"volume":1540513.1778052873,"fee":0.0004,"tokens":[{"type":"base","network":"ethereum","address":"0xeb4c2781e4eba804ce9a9803c67d0893436bb27d","decimals":8,"symbol":"renBTC","price":65931,"reserve":9774.87237916,"balance":59.13031787,"balanceRaw":"5913031787","balanceUSD":3898520.98748697,"tokenImageUrl":"https://storage.googleapis.com/zapper-fi-assets/tokens/ethereum/0xeb4c2781e4eba804ce9a9803c67d0893436bb27d.png"},{"type":"base","network":"ethereum","address":"0x2260fac5e5542a773aa44fbcfedf7c193bc2c599","decimals":8,"symbol":"WBTC","price":66004,"reserve":5210.85719602,"balance":31.52160258,"balanceRaw":"3152160258","balanceUSD":2080551.85669032,"tokenImageUrl":"https://storage.googleapis.com/zapper-fi-assets/tokens/ethereum/0x2260fac5e5542a773aa44fbcfedf7c193bc2c599.png"}],"reserve":13387.308538167757,"balance":89.04021571761595,"balanceRaw":"89040215717615937979","balanceUSD":5979735.549214771,"share":0.006049216355521928,"tokenImageUrl":false,"appName":"Curve","appImageUrl":"https://storage.googleapis.com/zapper-fi-assets/apps/curve.png","protocolDisplay":"Curve"}],"balance":89.04021571761595,"balanceRaw":"89040215717615937979","balanceUSD":5979735.549214771,"tokenImageUrl":false,"appName":"Convex","appImageUrl":"https://storage.googleapis.com/zapper-fi-assets/apps/convex.png","protocolDisplay":"Convex"},{"metaType":"claimable","type":"base","network":"ethereum","symbol":"CRV","address":"0xd533a949740bb3306d119cc777fa900ba034cd52","decimals":18,"price":4.11,"balance":1787.5267328313266,"balanceRaw":"1787526732831326837449","balanceUSD":7346.734871936753,"tokenImageUrl":"https://storage.googleapis.com/zapper-fi-assets/tokens/ethereum/0xd533a949740bb3306d119cc777fa900ba034cd52.png"},{"metaType":"claimable","type":"base","network":"ethereum","symbol":"CVX","address":"0x4e3fbd56cd56c3e72c1403e103b45db9da5b9d2b","decimals":18,"price":30.11,"balance":450.70986689574914,"balanceRaw":"450709866895749171956","balanceUSD":13570.874092231006,"tokenImageUrl":"https://storage.googleapis.com/zapper-fi-assets/tokens/ethereum/0x4e3fbd56cd56c3e72c1403e103b45db9da5b9d2b.png"}],"address":"0x8e299c62eed737a5d5a53539df37b5356a27b07d","appId":"convex","valueLockedUSD":899060768.5393372,"implementation":"single-staking","isActive":true,"dailyROI":0.00005658548553366671,"weeklyROI":0.000396098398735667,"yearlyROI":0.02065370221978835,"balanceUSD":6000653.158178939,"appName":"Convex","appImageUrl":"https://storage.googleapis.com/zapper-fi-assets/apps/convex.png","protocolDisplay":"Convex"},{"type":"farm","tokens":[{"metaType":"staked","type":"vault","network":"ethereum","address":"0xba723e335ec2939d52a2efca2a8199cb4cb93cc3","symbol":"cvxcrvRenWSBTC","label":"sBTC Curve in Convex","decimals":18,"supply":3193.496627068119,"price":66659.45789200165,"pricePerShare":1,"liquidity":212876753.94029656,"appId":"convex","tokens":[{"type":"pool","category":"pool","network":"ethereum","symbol":"sBTC Curve","address":"0x075b1bb99792c9e1041ba13afef80c91a1e70fb3","exchangeAddress":"0x7fc77b5c7614e1533320ea6ddc2eb61fa00a9714","label":"sBTC Curve","decimals":18,"appId":"curve","price":66659.45789200165,"liquidity":617540858.0208677,"supply":9262.2157003947,"volume":5914234.498403655,"fee":0.0004,"tokens":[{"type":"base","network":"ethereum","address":"0xeb4c2781e4eba804ce9a9803c67d0893436bb27d","decimals":8,"symbol":"renBTC","price":65931,"reserve":2935.11421965,"balance":0,"balanceRaw":"0","balanceUSD":0,"tokenImageUrl":"https://storage.googleapis.com/zapper-fi-assets/tokens/ethereum/0xeb4c2781e4eba804ce9a9803c67d0893436bb27d.png"},{"type":"base","network":"ethereum","address":"0x2260fac5e5542a773aa44fbcfedf7c193bc2c599","decimals":8,"symbol":"WBTC","price":66004,"reserve":2846.59694109,"balance":0,"balanceRaw":"0","balanceUSD":0,"tokenImageUrl":"https://storage.googleapis.com/zapper-fi-assets/tokens/ethereum/0x2260fac5e5542a773aa44fbcfedf7c193bc2c599.png"},{"type":"base","network":"ethereum","address":"0xfe18be6b3bd88a2d2a7f928d00292e7a9963cfc6","decimals":18,"symbol":"sBTC","price":66134,"hide":true,"reserve":3570.6150830952197,"balance":0,"balanceRaw":"0","balanceUSD":0,"tokenImageUrl":"https://storage.googleapis.com/zapper-fi-assets/tokens/ethereum/0xfe18be6b3bd88a2d2a7f928d00292e7a9963cfc6.png"}],"reserve":3193.496627068119,"balance":0,"balanceRaw":"0","balanceUSD":0,"share":0,"tokenImageUrl":false,"appName":"Curve","appImageUrl":"https://storage.googleapis.com/zapper-fi-assets/apps/curve.png","protocolDisplay":"Curve"}],"balance":0,"balanceRaw":"0","balanceUSD":0,"tokenImageUrl":false,"appName":"Convex","appImageUrl":"https://storage.googleapis.com/zapper-fi-assets/apps/convex.png","protocolDisplay":"Convex"},{"metaType":"claimable","type":"base","network":"ethereum","symbol":"CRV","address":"0xd533a949740bb3306d119cc777fa900ba034cd52","decimals":18,"price":4.11,"balance":2523.8776092559306,"balanceRaw":"2523877609255930460086","balanceUSD":10373.136974041876,"tokenImageUrl":"https://storage.googleapis.com/zapper-fi-assets/tokens/ethereum/0xd533a949740bb3306d119cc777fa900ba034cd52.png"},{"metaType":"claimable","type":"base","network":"ethereum","symbol":"CVX","address":"0x4e3fbd56cd56c3e72c1403e103b45db9da5b9d2b","decimals":18,"price":30.11,"balance":636.3745618098352,"balanceRaw":"636374561809835246748","balanceUSD":19161.23805609414,"tokenImageUrl":"https://storage.googleapis.com/zapper-fi-assets/tokens/ethereum/0x4e3fbd56cd56c3e72c1403e103b45db9da5b9d2b.png"}],"address":"0xd727a5a6d1c7b31ff9db4db4d24045b7df0cff93","appId":"convex","valueLockedUSD":212876753.94029656,"implementation":"single-staking","isActive":true,"dailyROI":0.000005029025279101873,"weeklyROI":0.00003520317695371311,"yearlyROI":0.0018355942268721837,"balanceUSD":29534.375030136012,"appName":"Convex","appImageUrl":"https://storage.googleapis.com/zapper-fi-assets/apps/convex.png","protocolDisplay":"Convex"},{"type":"farm","tokens":[{"metaType":"staked","type":"vault","network":"ethereum","address":"0xabb54222c2b77158cc975a2b715a3d703c256f05","symbol":"cvxMIM-3LP3CRV-f","label":"MIM Curve in Convex","decimals":18,"supply":2675022049.730057,"price":1.012881926676426,"pricePerShare":1,"liquidity":2709481487.632502,"appId":"convex","tokens":[{"type":"pool","category":"pool","network":"ethereum","symbol":"MIM Curve","address":"0x5a6a4d54456819380173272a5e8e9b9904bdf41b","exchangeAddress":"0x5a6a4d54456819380173272a5e8e9b9904bdf41b","label":"MIM Curve","decimals":18,"appId":"curve","price":1.012881926676426,"liquidity":2724955049.3820505,"supply":2716332514.714218,"volume":58525319.25091114,"fee":0.0004,"tokens":[{"type":"base","network":"ethereum","address":"0x99d8a9c45b2eca8864373a26d1459e3dff1e17f3","decimals":18,"symbol":"MIM","price":1,"reserve":1142298764.097206,"balance":2095327.1759971413,"balanceRaw":"2095327175997141300000000","balanceUSD":2095327.1759971413,"tokenImageUrl":"https://storage.googleapis.com/zapper-fi-assets/tokens/ethereum/0x99d8a9c45b2eca8864373a26d1459e3dff1e17f3.png"},{"type":"pool","category":"pool","network":"ethereum","symbol":"3Pool Curve","address":"0x6c3f90f043a72fa612cbac8115ee7e52bde6e490","exchangeAddress":"0xbebc44782c7db0a1a60cb6fe97d0b483032ff1c7","label":"3Pool Curve","decimals":18,"appId":"curve","price":1.0195217045055491,"liquidity":4113511319.495022,"supply":4034551297.707999,"volume":86683469.6990338,"fee":0.0003,"tokens":[{"type":"base","network":"ethereum","address":"0x6b175474e89094c44da98b954eedeac495271d0f","decimals":18,"symbol":"DAI","price":1,"reserve":1711837590.0801756,"balance":1208174.282117706,"balanceRaw":"1208174282117706000000000","balanceUSD":1208174.282117706,"tokenImageUrl":"https://storage.googleapis.com/zapper-fi-assets/tokens/ethereum/0x6b175474e89094c44da98b954eedeac495271d0f.png"},{"type":"base","network":"ethereum","address":"0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48","decimals":6,"symbol":"USDC","price":1,"reserve":1324651032.712253,"balance":934907.213031,"balanceRaw":"934907213031","balanceUSD":934907.213031,"tokenImageUrl":"https://storage.googleapis.com/zapper-fi-assets/tokens/ethereum/0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48.png"},{"type":"base","network":"ethereum","address":"0xdac17f958d2ee523a2206206994597c13d831ec7","decimals":6,"symbol":"USDT","price":1,"reserve":1077022696.702593,"balance":760137.019396,"balanceRaw":"760137019396","balanceUSD":760137.019396,"tokenImageUrl":"https://storage.googleapis.com/zapper-fi-assets/tokens/ethereum/0xdac17f958d2ee523a2206206994597c13d831ec7.png"}],"reserve":1552351733.4556463,"balance":2847490.4079814735,"balanceRaw":"2847490407981473500000000","balanceUSD":2903078.2743084733,"share":0.0007057762308287202,"tokenImageUrl":false,"appName":"Curve","appImageUrl":"https://storage.googleapis.com/zapper-fi-assets/apps/curve.png","protocolDisplay":"Curve"}],"reserve":2675022049.730057,"balance":4982589.070402792,"balanceRaw":"4982589070402791784253904","balanceUSD":5046774.417466482,"share":0.001834307487545207,"tokenImageUrl":false,"appName":"Curve","appImageUrl":"https://storage.googleapis.com/zapper-fi-assets/apps/curve.png","protocolDisplay":"Curve"}],"balance":4982589.070402792,"balanceRaw":"4982589070402791784253904","balanceUSD":5046774.417466482,"tokenImageUrl":false,"appName":"Convex","appImageUrl":"https://storage.googleapis.com/zapper-fi-assets/apps/convex.png","protocolDisplay":"Convex"},{"metaType":"claimable","type":"base","network":"ethereum","symbol":"CRV","address":"0xd533a949740bb3306d119cc777fa900ba034cd52","decimals":18,"price":4.11,"balance":5050.835567495526,"balanceRaw":"5050835567495526390321","balanceUSD":20758.934182406614,"tokenImageUrl":"https://storage.googleapis.com/zapper-fi-assets/tokens/ethereum/0xd533a949740bb3306d119cc777fa900ba034cd52.png"},{"metaType":"claimable","type":"base","network":"ethereum","symbol":"CVX","address":"0x4e3fbd56cd56c3e72c1403e103b45db9da5b9d2b","decimals":18,"price":30.11,"balance":1273.525807769295,"balanceRaw":"1273525807769295083230","balanceUSD":38345.86207193347,"tokenImageUrl":"https://storage.googleapis.com/zapper-fi-assets/tokens/ethereum/0x4e3fbd56cd56c3e72c1403e103b45db9da5b9d2b.png"},{"metaType":"claimable","type":"base","network":"ethereum","symbol":"SPELL","address":"0x090185f2135308bad17527004364ebcc2d37e5f6","decimals":18,"price":0.02482282,"balance":1044514.452076679,"balanceRaw":"1044514452076679027224910","balanceUSD":25927.794231298027,"tokenImageUrl":"https://storage.googleapis.com/zapper-fi-assets/tokens/ethereum/0x090185f2135308bad17527004364ebcc2d37e5f6.png"}],"address":"0xfd5abf66b003881b88567eb9ed9c651f14dc4771","appId":"convex","valueLockedUSD":2709481487.632502,"implementation":"single-staking","isActive":true,"dailyROI":0.00014233485299830484,"weeklyROI":0.0009963439709881339,"yearlyROI":0.051952221344381266,"balanceUSD":5131807.00795212,"appName":"Convex","appImageUrl":"https://storage.googleapis.com/zapper-fi-assets/apps/convex.png","protocolDisplay":"Convex"}],"meta":[]}],"meta":[{"label":"Total","value":11161994.541161194,"type":"dollar"},{"label":"Assets","value":11161994.541161194,"type":"dollar"},{"label":"Debt","value":0,"type":"dollar"}]}}
"""
