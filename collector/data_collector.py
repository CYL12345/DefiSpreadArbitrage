import os
import json
import websocket
import time
import asyncio
import logging
from web3 import Web3
from dotenv import load_dotenv
from decimal import Decimal
import logging
from logging.handlers import RotatingFileHandler

#加载环境变量
load_dotenv()

# ---------- 日志配置 ----------
log_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
log_handler = RotatingFileHandler('arbitrage.log', maxBytes=10*1024*1024, backupCount=5)
log_handler.setFormatter(log_formatter)
logger = logging.getLogger()
logger.addHandler(log_handler)
logger.setLevel(logging.INFO)

#c初始化连接
w3 = Web3(Web3.HTTPProvider(os.getenv("INFURA_URL")))
UNiSWAP_ABI = json.load(open("uniswap_pool_abi.json"))

#OKX配置
OKX_WS_URL = "wss://ws.okx.com:8443/ws/v5/public"
OKX_REST_URL = "https://www.okx.com/api/v5/market/ticker?instId=ETH-USDT-SWAP"

class Config:
    MIN_PROFIT_BPS = 5       # 最小套利利润基点 (0.05%)
    MAX_LOSS_BPS = 50        # 最大允许亏损基点
    GAS_LIMIT = 300000       # Gas限制
    MAX_GAS_PRICE = 100      # Gwei


class ArbitrageBot:
    def __init__(self):
        self.uniswap_pool = w3.eth.contract(
            address=os.getenv("UNISWAP_POOL"),
            abi=UNiSWAP_ABI
        )
        self.cex_price = 0
        self.dex_price = 0
        self.min_profit_bps = 5#最小套利利润基点(0.05%)
        self.is_running = True

    async def get_okx_price(self):
        ws = websocket.WebSocket()
        ws.connect(OKX_WS_URL)
        ws.send(json.dumps({
            "op": "subscribe",
            "args": [{"channel": "tickers", "instId": "ETH-USDT-SWAP"}]
        }))
        while True:
            data = json.loads(ws.recv())
            if 'data' in data:
                self.cex_price = float(data['data'][0]['last'])
                break
        ws.close()

    async def get_uniswap_price(self):
        slot0 = self.uniswap_pool.functions.slot0().call()
        sqrt_price_x96 = slot0[0]
        token0 = self.uniswap_pool.functions.token0().call()

        #计算价格
        price = ((Decimal(sqrt_price_x96) / Decimal(2**96)) ** 2) * Decimal(10**12)

        if token0.lower() == os.getenv("WETH_ADDRESS"):
            self.dex_price = float(price)
        else:
            self.dex_price = float(1 / price)

    async def check_balance(self):
        usdt_contract = w3.eth.contract(
            address=os.getenv("USDT_ADDRESS"),
            abi=[{
                "inputs":[],
                "name":"balanceOf",
                "outputs":[{
                    "name":"",
                    "type":"uint256"
                }],
                "stateMutability":"view",
                "type":"function"
            }]
        )
        wallet_address = os.getenv("WALLET_ADDRESS")
        if wallet_address:
            checksum_address = w3.to_checksum_address(wallet_address)
            eth_balance = w3.eth.get_balance(checksum_address)
            usdt_balance = usdt_contract.functions.balanceOf(checksum_address).call()
        else:
            # 处理环境变量未设置的情况
            eth_balance = 0
            print("WALLET_ADDRESS 环境变量未设置，余额默认为 0。")
        return {
            'eth':eth_balance,
            'usdt':usdt_balance,
            'min_eth': w3.to_wei(0.1, 'ether'),
            'min_usdt': 100 * 10**6  # 100 USDT
        }

    async def check_arbitrage(self,buy_on_dex:bool):
        balances = await self.check_balance()
        if balances['eth'] < balances['min_eth'] or balances['usdt'] < balances['min_usdt']:
            print("余额不足，无法进行套利")
            return False
        current_gas_price = w3.eth.gas_price
        if current_gas_price > w3.to_wei(100, 'gwei'):
            logger.warning(f"Gas价格过高: {current_gas_price} wei")
            return False
        
        try:
            #构建uniswap交易（带滑点保护）：
            if buy_on_dex:
                #在DEX用USDT买ETH：
                amount_in = min(balances['usdt'], 1000*10**6)#最多1000U
                min_amount_out = int(amount_in/self.dex_price * 0.995)#滑点保护
                tx_data = self.uniswap_pool.functions.swap(
                    os.getenv("WALLET_ADDRESS"),
                    True,#0 fot 1
                    amount_in,
                    int(self.dex_price * 0.995 * (2**96)),#滑点sqrtPriceLimit
                    b""
                ).build_transaction({
                    'from':os.getenv("WALLET_ADDRESS"),
                    'gas':os.getenv("GAS_LIMIT"),
                    'maxFeePerGas': current_gas_price,
                    'maxPriorityFeePerGas': int(current_gas_price * 0.1),
                    'nonce': w3.eth.get_transaction_count(os.getenv("WALLET_ADDRESS"))
                })
            else:
                #在DEX用ETH买USDT：
                amount_in = min(balances["eth"],w3.to_wei(0.1, 'ether'))
                min_amount_out = int(amount_in * self.dex_price * 0.995)
                tx_data = self.uniswap_pool.functions.swap(
                    os.getenv("WALLET_ADDRESS"),
                    False,#0 fot 1
                    amount_in, 
                    int(self.dex_price * 1.005 * (2**96)),#滑点sqrtPriceLimit
                    b""
                ).build_transaction({
                    'from':os.getenv("WALLET_ADDRESS"),
                    'value':amount_in,
                    'gas':os.getenv("GAS_LIMIT"),
                    'maxFeePerGas': current_gas_price,
                    'maxPriorityFeePerGas': int(current_gas_price * 0.1),
                    'nonce': w3.eth.get_transaction_count(os.getenv("WALLET_ADDRESS"))
                })
            #签名并发送交易
            sign_tx = w3.eth.account.sign_transaction(tx_data,private_key=os.getenv("PRIVATE_KEY"))
            tx_hash = w3.eth.send_raw_transaction(sign_tx.raw_transaction)
            logger.info(f"交易已发送，哈希: {tx_hash.hex()}")

            #等待交易确认
            receipt = w3.eth.wait_for_transaction_receipt(tx_hash)
            if receipt["status"] == 1:
                    logger.info("交易成功确认")
                    return True
            else:
                    logger.error("交易失败")
                    return False
        except Exception as e:
            logger.error(f"交易执行错误: {str(e)}")
            return False

    def check_arbitrage_log(self):
        price_diff = self.dex_price - self.cex_price
        spread_bps = (abs(price_diff) / self.dex_price) * 10000
        # 熔断检查
        if spread_bps < -Config.MAX_LOSS_BPS:
            logger.critical(f"价差异常: {spread_bps}bps，触发熔断")
            self.is_running = False

        if spread_bps > self.min_profit_bps:
            print(f"套利机会：Uniswap价格 {self.dex_price}, OKX价格 {self.cex_price}, 价差 {spread_bps:.2f} bps")
            self.execute_trade(price_diff > 0)
        else:
            print(f"当前价差 {spread_bps:.2f} bps, 未触发套利,CEX: {self.cex_price}, DEX: {self.dex_price}")

    def execute_trade(self,buy_on_dex):
        # 这里添加执行交易的逻辑
        if buy_on_dex:
            print("在Uniswap上买入")
        else:
            print("在OKX上买入")

    async def monitor(self):
        """主监控循环"""
        while self.is_running:
            try:
                #异步获取价格
                await asyncio.gather(
                    self.get_okx_price(),
                    self.get_uniswap_price(),
                )

                #计算价差
                self.check_arbitrage_log()
                time.sleep(3) #3秒轮训
            except Exception as e:
                logger.error(f"监控循环错误: {str(e)}")
                await asyncio.sleep(8)

    async def shutdown(self):
        """安全关闭"""
        #await self.session.close()
        self.is_running = False
    # ---------- 主程序 ----------
    async def main(self):
        try:
            await self.monitor()
        except KeyboardInterrupt:
            await self.shutdown()
            logger.info("程序已安全退出")

if __name__ == "__main__":
    bot = ArbitrageBot()
    asyncio.run(bot.main())






        


