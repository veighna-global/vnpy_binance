import hashlib
import hmac
import time
import urllib.parse
from copy import copy
from typing import cast, Any
from collections.abc import Callable
from enum import Enum
from time import sleep
from datetime import datetime, timedelta

from numpy import format_float_positional

from vnpy.event import Event, EventEngine
from vnpy.trader.constant import (
    Direction,
    Exchange,
    Product,
    Status,
    OrderType,
    Interval
)
from vnpy.trader.gateway import BaseGateway
from vnpy.trader.object import (
    TickData,
    OrderData,
    TradeData,
    AccountData,
    ContractData,
    PositionData,
    BarData,
    OrderRequest,
    CancelRequest,
    SubscribeRequest,
    HistoryRequest
)
from vnpy.trader.event import EVENT_TIMER
from vnpy.trader.utility import round_to, ZoneInfo
from vnpy_rest import Request, RestClient, Response
from vnpy_websocket import WebsocketClient


# Timezone constant
UTC_TZ = ZoneInfo("UTC")

# Real server hosts
REAL_REST_HOSTT: str = "https://fapi.binance.com"
REAL_TRADE_HOST: str = "wss://ws-fapi.binance.com/ws-fapi/v1"
REAL_USER_HOST: str = "wss://fstream.binance.com/ws/"
REAL_DATA_HOST: str = "wss://fstream.binance.com/stream"

# Testnet server hosts
TESTNET_REST_HOST: str = "https://testnet.binancefuture.com"
TESTNET_TRADE_HOST: str = "wss://testnet.binancefuture.com/ws-fapi/v1"
TESTNET_USER_HOST: str = "wss://stream.binancefuture.com/ws/"
TESTNET_DATA_HOST: str = "wss://stream.binancefuture.com/stream"

# Order status map
STATUS_BINANCE2VT: dict[str, Status] = {
    "NEW": Status.NOTTRADED,
    "PARTIALLY_FILLED": Status.PARTTRADED,
    "FILLED": Status.ALLTRADED,
    "CANCELED": Status.CANCELLED,
    "REJECTED": Status.REJECTED,
    "EXPIRED": Status.CANCELLED
}

# Order type map
ORDERTYPE_VT2BINANCE: dict[OrderType, tuple[str, str]] = {
    OrderType.LIMIT: ("LIMIT", "GTC"),
    OrderType.MARKET: ("MARKET", "GTC"),
    OrderType.FAK: ("LIMIT", "IOC"),
    OrderType.FOK: ("LIMIT", "FOK"),
}
ORDERTYPE_BINANCE2VT: dict[tuple[str, str], OrderType] = {v: k for k, v in ORDERTYPE_VT2BINANCE.items()}

# Direction map
DIRECTION_VT2BINANCE: dict[Direction, str] = {
    Direction.LONG: "BUY",
    Direction.SHORT: "SELL"
}
DIRECTION_BINANCE2VT: dict[str, Direction] = {v: k for k, v in DIRECTION_VT2BINANCE.items()}

# Product map
PRODUCT_BINANCE2VT: dict[str, Product] = {
    "PERPETUAL": Product.SWAP,
    "PERPETUAL_DELIVERING": Product.SWAP,
    "CURRENT_MONTH": Product.FUTURES,
    "NEXT_MONTH": Product.FUTURES,
    "CURRENT_QUARTER": Product.FUTURES,
    "NEXT_QUARTER": Product.FUTURES,
}

# Kline interval map
INTERVAL_VT2BINANCE: dict[Interval, str] = {
    Interval.MINUTE: "1m",
    Interval.HOUR: "1h",
    Interval.DAILY: "1d",
}

# Timedelta map
TIMEDELTA_MAP: dict[Interval, timedelta] = {
    Interval.MINUTE: timedelta(minutes=1),
    Interval.HOUR: timedelta(hours=1),
    Interval.DAILY: timedelta(days=1),
}

# Set weboscket timeout to 24 hour
WEBSOCKET_TIMEOUT = 24 * 60 * 60

# Global dict for contract data
symbol_contract_map: dict[str, ContractData] = {}
name_contract_map: dict[str, ContractData] = {}


# Authentication level
class Security(Enum):
    NONE = 0
    SIGNED = 1
    API_KEY = 2


class BinanceLinearGateway(BaseGateway):
    """
    The Binance linear trading gateway for VeighNa.

    1. Only support crossed position
    2. Only support one-way mode
    """

    default_name: str = "BINANCE_LINEAR"

    default_setting: dict = {
        "API Key": "",
        "API Secret": "",
        "Server": ["REAL", "TESTNET"],
        "Kline Stream": ["False", "True"],
        "Proxy Host": "",
        "Proxy Port": 0
    }

    exchanges: Exchange = [Exchange.GLOBAL]

    def __init__(self, event_engine: EventEngine, gateway_name: str) -> None:
        """
        The init method of the gateway.

        event_engine: the global event engine object of VeighNa
        gateway_name: the unique name for identifying the gateway
        """
        super().__init__(event_engine, gateway_name)

        self.rest_api: RestApi = RestApi(self)
        self.trade_api: TradeApi = TradeApi(self)
        self.user_api: UserApi = UserApi(self)
        self.md_api: MdApi = MdApi(self)

        self.orders: dict[str, OrderData] = {}

    def connect(self, setting: dict) -> None:
        """Start server connections"""
        key: str = setting["API Key"]
        secret: str = setting["API Secret"]
        server: str = setting["Server"]
        kline_stream: bool = setting["Kline Stream"] == "True"
        proxy_host: str = setting["Proxy Host"]
        proxy_port: int = setting["Proxy Port"]

        self.rest_api.connect(key, secret, server, proxy_host, proxy_port)
        self.trade_api.connect(key, secret, server, proxy_host, proxy_port)
        self.data_api.connect(server, kline_stream, proxy_host, proxy_port)

        self.event_engine.register(EVENT_TIMER, self.process_timer_event)

    def subscribe(self, req: SubscribeRequest) -> None:
        """Subscribe market data"""
        self.data_api.subscribe(req)

    def send_order(self, req: OrderRequest) -> str:
        """Send new order"""
        return self.trade_api.send_order(req)

    def cancel_order(self, req: CancelRequest) -> None:
        """Cancel existing order"""
        self.trade_api.cancel_order(req)

    def query_account(self) -> None:
        """Not required since Binance provides websocket update"""
        pass

    def query_position(self) -> None:
        """Not required since Binance provides websocket update"""
        pass

    def query_history(self, req: HistoryRequest) -> list[BarData]:
        """Query kline history data"""
        return self.rest_api.query_history(req)

    def close(self) -> None:
        """Close server connections"""
        self.rest_api.stop()
        self.user_api.stop()
        self.data_api.stop()
        self.trade_api.stop()

    def process_timer_event(self, event: Event) -> None:
        """Process timer task"""
        self.rest_api.keep_user_stream()

    def on_order(self, order: OrderData) -> None:
        """Save a copy of order and then push"""
        self.orders[order.orderid] = copy(order)
        super().on_order(order)

    def get_order(self, orderid: str) -> OrderData:
        """Get previously saved order"""
        return self.orders.get(orderid, None)


class RestApi(RestClient):
    """The REST API of BinanceLinearGateway"""

    def __init__(self, gateway: BinanceLinearGateway) -> None:
        """
        The init method of the api.

        gateway: the parent gateway object for pushing callback data.
        """
        super().__init__()

        self.gateway: BinanceLinearGateway = gateway
        self.gateway_name: str = gateway.gateway_name

        self.user_api: UserApi = self.gateway.user_api

        self.key: str = ""
        self.secret: bytes = b""

        self.user_stream_key: str = ""
        self.keep_alive_count: int = 0
        self.time_offset: int = 0

        self.order_count: int = 1_000_000
        self.order_prefix: str = ""

    def sign(self, request: Request) -> Request:
        """Standard callback for signing a request"""
        security: Security = request.data["security"]
        if security == Security.NONE:
            request.data = None
            return request

        if request.params:
            path: str = request.path + "?" + urllib.parse.urlencode(request.params)
        else:
            request.params = dict()
            path = request.path

        if security == Security.SIGNED:
            timestamp: int = int(time.time() * 1000)

            if self.time_offset > 0:
                timestamp -= abs(self.time_offset)
            elif self.time_offset < 0:
                timestamp += abs(self.time_offset)

            request.params["timestamp"] = timestamp

            query: str = urllib.parse.urlencode(sorted(request.params.items()))
            signature: str = hmac.new(
                self.secret,
                query.encode("utf-8"),
                hashlib.sha256
            ).hexdigest()

            query += f"&signature={signature}"
            path = request.path + "?" + query

        request.path = path
        request.params = {}
        request.data = {}

        # Add header to the request
        headers = {
            "Content-Type": "application/x-www-form-urlencoded",
            "Accept": "application/json",
            "X-MBX-APIKEY": self.key,
            "Connection": "close"
        }

        if security in [Security.SIGNED, Security.API_KEY]:
            request.headers = headers

        return request

    def connect(
        self,
        key: str,
        secret: str,
        server: str,
        proxy_host: str,
        proxy_port: int
    ) -> None:
        """Start server connection"""
        self.key = key
        self.secret = secret.encode()
        self.proxy_port = proxy_port
        self.proxy_host = proxy_host
        self.server = server

        self.order_prefix = datetime.now().strftime("%y%m%d%H%M%S")

        if self.server == "REAL":
            self.init(REAL_REST_HOSTT, proxy_host, proxy_port)
        else:
            self.init(TESTNET_REST_HOST, proxy_host, proxy_port)

        self.start()

        self.gateway.write_log("REST API started")

        self.query_time()

    def query_time(self) -> None:
        """Query server time"""
        data: dict = {"security": Security.NONE}

        path: str = "/fapi/v1/time"

        self.add_request(
            "GET",
            path,
            callback=self.on_query_time,
            data=data
        )

    def query_account(self) -> None:
        """Query account balance"""
        data: dict = {"security": Security.SIGNED}

        path: str = "/fapi/v3/account"

        self.add_request(
            method="GET",
            path=path,
            callback=self.on_query_account,
            data=data
        )

    def query_position(self) -> None:
        """Query holding positions"""
        data: dict = {"security": Security.SIGNED}

        path: str = "/fapi/v3/positionRisk"

        self.add_request(
            method="GET",
            path=path,
            callback=self.on_query_position,
            data=data
        )

    def query_order(self) -> None:
        """Query open orders"""
        data: dict = {"security": Security.SIGNED}

        path: str = "/fapi/v1/openOrders"

        self.add_request(
            method="GET",
            path=path,
            callback=self.on_query_order,
            data=data
        )

    def query_contract(self) -> None:
        """Query available contracts"""
        data: dict = {"security": Security.NONE}

        path: str = "/fapi/v1/exchangeInfo"

        self.add_request(
            method="GET",
            path=path,
            callback=self.on_query_contract,
            data=data
        )

    def start_user_stream(self) -> None:
        """Create listen key for user stream"""
        data: dict = {"security": Security.API_KEY}

        path: str = "/fapi/v1/listenKey"

        self.add_request(
            method="POST",
            path=path,
            callback=self.on_start_user_stream,
            data=data
        )

    def keep_user_stream(self) -> None:
        """Extend listen key validity"""
        if not self.user_stream_key:
            return

        self.keep_alive_count += 1
        if self.keep_alive_count < 600:
            return
        self.keep_alive_count = 0

        data: dict = {"security": Security.API_KEY}

        params: dict = {"listenKey": self.user_stream_key}

        path: str = "/fapi/v1/listenKey"

        self.add_request(
            method="PUT",
            path=path,
            callback=self.on_keep_user_stream,
            params=params,
            data=data,
            on_error=self.on_keep_user_stream_error
        )

    def on_query_time(self, data: dict, request: Request) -> None:
        """Callback of server time query"""
        local_time: int = int(time.time() * 1000)
        server_time: int = int(data["serverTime"])
        self.time_offset = local_time - server_time

        self.gateway.write_log(f"Server time updated, local offset: {self.time_offset}ms")

        self.query_contract()

    def on_query_account(self, data: dict, request: Request) -> None:
        """Callback of account balance query"""
        for asset in data["assets"]:
            account: AccountData = AccountData(
                accountid=asset["asset"],
                balance=float(asset["walletBalance"]),
                frozen=float(asset["maintMargin"]),
                gateway_name=self.gateway_name
            )

            self.gateway.on_account(account)

        self.gateway.write_log("Account balance data is received")

    def on_query_position(self, data: list, request: Request) -> None:
        """Callback of holding positions query"""
        for d in data:
            name: str = d["symbol"]
            contract: ContractData | None = name_contract_map.get(name, None)
            if not contract:
                continue

            position: PositionData = PositionData(
                symbol=contract.symbol,
                exchange=Exchange.GLOBAL,
                direction=Direction.NET,
                volume=float(d["positionAmt"]),
                price=float(d["entryPrice"]),
                pnl=float(d["unRealizedProfit"]),
                gateway_name=self.gateway_name,
            )

            self.gateway.on_position(position)

        self.gateway.write_log("Holding positions data is received")

    def on_query_order(self, data: list, request: Request) -> None:
        """Callback of open orders query"""
        for d in data:
            key: tuple[str, str] = (d["type"], d["timeInForce"])
            order_type: OrderType | None = ORDERTYPE_BINANCE2VT.get(key, None)
            if not order_type:
                continue

            contract: ContractData | None = name_contract_map.get(d["symbol"], None)
            if not contract:
                continue

            order: OrderData = OrderData(
                orderid=d["clientOrderId"],
                symbol=contract.symbol,
                exchange=Exchange.GLOBAL,
                price=float(d["price"]),
                volume=float(d["origQty"]),
                type=order_type,
                direction=DIRECTION_BINANCE2VT[d["side"]],
                traded=float(d["executedQty"]),
                status=STATUS_BINANCE2VT.get(d["status"], None),
                datetime=generate_datetime(d["time"]),
                gateway_name=self.gateway_name,
            )
            self.gateway.on_order(order)

        self.gateway.write_log("Open orders data is received")

    def on_query_contract(self, data: dict, request: Request) -> None:
        """Callback of available contracts query"""
        for d in data["symbols"]:
            pricetick: float = 1
            min_volume: float = 1
            max_volume: float = 1

            for f in d["filters"]:
                if f["filterType"] == "PRICE_FILTER":
                    pricetick = float(f["tickSize"])
                elif f["filterType"] == "LOT_SIZE":
                    min_volume = float(f["minQty"])
                    max_volume = float(f["maxQty"])

            product: Product = PRODUCT_BINANCE2VT.get(d["contractType"], Product.SWAP)
            if product == Product.SWAP:
                symbol: str = d["symbol"] + "_SWAP_BINANCE"
            else:
                symbol = d["symbol"] + "_BINANCE"

            contract: ContractData = ContractData(
                symbol=symbol,
                exchange=Exchange.GLOBAL,
                name=d["symbol"],
                pricetick=pricetick,
                size=1,
                min_volume=min_volume,
                max_volume=max_volume,
                product=PRODUCT_BINANCE2VT.get(d["contractType"], Product.SWAP),
                net_position=True,
                history_data=True,
                gateway_name=self.gateway_name,
                stop_supported=True
            )
            self.gateway.on_contract(contract)

            symbol_contract_map[contract.symbol] = contract
            name_contract_map[contract.name] = contract

        self.gateway.write_log("Available contracts data is received")

        # Query private data after time offset is calculated
        if self.key and self.secret:
            self.query_order()
            self.query_account()
            self.query_position()
            self.start_user_stream()

    def on_start_user_stream(self, data: dict, request: Request) -> None:
        """Successful callback of start_user_stream"""
        self.user_stream_key = data["listenKey"]
        self.keep_alive_count = 0

        if self.server == "REAL":
            url = REAL_USER_HOST + self.user_stream_key
        else:
            url = TESTNET_USER_HOST + self.user_stream_key

        self.user_api.connect(url, self.proxy_host, self.proxy_port)

    def on_keep_user_stream(self, data: dict, request: Request) -> None:
        """Successful callback of keep_user_stream"""
        pass

    def on_keep_user_stream_error(self, exception_type: type, exception_value: Exception, tb: Any, request: Request) -> None:
        """Error callback of keep_user_stream"""
        if not issubclass(exception_type, TimeoutError):        # Ignore timeout exception
            self.on_error(exception_type, exception_value, tb, request)

    def query_history(self, req: HistoryRequest) -> list[BarData]:
        """Query kline history data"""
        history: list[BarData] = []
        limit: int = 1500

        start_time: int = int(datetime.timestamp(req.start))

        while True:
            # Create query parameters
            params: dict = {
                "symbol": req.symbol,
                "interval": INTERVAL_VT2BINANCE[req.interval],
                "limit": limit
            }

            params["startTime"] = start_time * 1000
            path: str = "/fapi/v1/klines"
            if req.end:
                end_time = int(datetime.timestamp(req.end))
                params["endTime"] = end_time * 1000     # Convert to milliseconds

            resp: Response = self.request(
                "GET",
                path=path,
                data={"security": Security.NONE},
                params=params
            )

            # Break the loop if request failed
            if resp.status_code // 100 != 2:
                msg: str = f"Query kline history failed, status code: {resp.status_code}, message: {resp.text}"
                self.gateway.write_log(msg)
                break
            else:
                data: dict = resp.json()
                if not data:
                    msg = f"No kline history data is received, start time: {start_time}"
                    self.gateway.write_log(msg)
                    break

                buf: list[BarData] = []

                for row in data:
                    bar: BarData = BarData(
                        symbol=req.symbol,
                        exchange=req.exchange,
                        datetime=generate_datetime(row[0]),
                        interval=req.interval,
                        volume=float(row[5]),
                        turnover=float(row[7]),
                        open_price=float(row[1]),
                        high_price=float(row[2]),
                        low_price=float(row[3]),
                        close_price=float(row[4]),
                        gateway_name=self.gateway_name
                    )
                    bar.extra = {
                        "trade_count": int(row[8]),
                        "active_volume": float(row[9]),
                        "active_turnover": float(row[10]),
                    }
                    buf.append(bar)

                begin: datetime = buf[0].datetime
                end: datetime = buf[-1].datetime

                history.extend(buf)
                msg = f"Query kline history finished, {req.symbol} - {req.interval.value}, {begin} - {end}"
                self.gateway.write_log(msg)

                # Break the loop if the latest data received
                if (
                    len(data) < limit
                    or (req.end and end >= req.end)
                ):
                    break

                # Update query start time
                start_dt = bar.datetime + TIMEDELTA_MAP[req.interval]
                start_time = int(datetime.timestamp(start_dt))

            # Wait to meet request flow limit
            sleep(0.5)

        # Remove the unclosed kline
        if history:
            history.pop(-1)

        return history


class UserApi(WebsocketClient):
    """The user data websocket API of BinanceLinearGateway"""

    def __init__(self, gateway: BinanceLinearGateway) -> None:
        """
        The init method of the api.

        gateway: the parent gateway object for pushing callback data.
        """
        super().__init__()

        self.gateway: BinanceLinearGateway = gateway
        self.gateway_name: str = gateway.gateway_name

    def connect(self, url: str, proxy_host: str, proxy_port: int) -> None:
        """Start server connection"""
        self.init(url, proxy_host, proxy_port, receive_timeout=WEBSOCKET_TIMEOUT)
        self.start()

    def on_connected(self) -> None:
        """Callback when server is connected"""
        self.gateway.write_log("User data Websocket API is connected")

    def on_packet(self, packet: dict) -> None:
        """Callback of data update"""
        match packet["e"]:
            case "ACCOUNT_UPDATE":
                self.on_account(packet)
            case "ORDER_TRADE_UPDATE":
                self.on_order(packet)
            case "listenKeyExpired":
                self.on_listen_key_expired()

    def on_listen_key_expired(self) -> None:
        """Callback of listen key expired"""
        self.gateway.write_log("Listen key is expired")
        self.disconnect()

    def on_account(self, packet: dict) -> None:
        """Callback of account balance and holding position update"""
        for acc_data in packet["a"]["B"]:
            account: AccountData = AccountData(
                accountid=acc_data["a"],
                balance=float(acc_data["wb"]),
                frozen=float(acc_data["wb"]) - float(acc_data["cw"]),
                gateway_name=self.gateway_name
            )

            if account.balance:
                self.gateway.on_account(account)

        for pos_data in packet["a"]["P"]:
            if pos_data["ps"] == "BOTH":
                volume = pos_data["pa"]
                if "." in volume:
                    volume = float(volume)
                else:
                    volume = int(volume)

                name: str = pos_data["s"]
                contract: ContractData | None = name_contract_map.get(name, None)
                if not contract:
                    continue

                position: PositionData = PositionData(
                    symbol=contract.symbol,
                    exchange=Exchange.GLOBAL,
                    direction=Direction.NET,
                    volume=volume,
                    price=float(pos_data["ep"]),
                    pnl=float(pos_data["up"]),
                    gateway_name=self.gateway_name,
                )
                self.gateway.on_position(position)

    def on_order(self, packet: dict) -> None:
        """Callback of order and trade update"""
        ord_data: dict = packet["o"]

        # Filter unsupported order type
        key: tuple[str, str] = (ord_data["o"], ord_data["f"])
        order_type: OrderType | None = ORDERTYPE_BINANCE2VT.get(key, None)
        if not order_type:
            return

        # Filter unsupported symbol
        name: str = ord_data["s"]
        contract: ContractData | None = name_contract_map.get(name, None)
        if not contract:
            return

        # Create and push order
        order: OrderData = OrderData(
            symbol=contract.symbol,
            exchange=Exchange.GLOBAL,
            orderid=str(ord_data["c"]),
            type=order_type,
            direction=DIRECTION_BINANCE2VT[ord_data["S"]],
            price=float(ord_data["p"]),
            volume=float(ord_data["q"]),
            traded=float(ord_data["z"]),
            status=STATUS_BINANCE2VT[ord_data["X"]],
            datetime=generate_datetime(packet["E"]),
            gateway_name=self.gateway_name,
        )

        self.gateway.on_order(order)

        # Round trade volume to meet step size
        trade_volume: float = float(ord_data["l"])
        trade_volume = round_to(trade_volume, contract.min_volume)
        if not trade_volume:
            return

        # Create and push trade
        trade: TradeData = TradeData(
            symbol=order.symbol,
            exchange=order.exchange,
            orderid=order.orderid,
            tradeid=ord_data["t"],
            direction=order.direction,
            price=float(ord_data["L"]),
            volume=trade_volume,
            datetime=generate_datetime(ord_data["T"]),
            gateway_name=self.gateway_name,
        )
        self.gateway.on_trade(trade)

    def on_disconnected(self, status_code: int, msg: str) -> None:
        """Callback when server is disconnected"""
        self.gateway.write_log(f"Trade Websocket API is disconnected, code: {status_code}, msg: {msg}")
        self.gateway.rest_api.start_user_stream()

    def on_error(self, e: Exception) -> None:
        """
        Callback when exception raised.
        """
        self.gateway.write_log(f"Trade Websocket API exception: {e}")


class MdApi(WebsocketClient):
    """The data websocket API of BinanceLinearGateway"""

    def __init__(self, gateway: BinanceLinearGateway) -> None:
        """
        The init method of the api.

        gateway: the parent gateway object for pushing callback data.
        """
        super().__init__()

        self.gateway: BinanceLinearGateway = gateway
        self.gateway_name: str = gateway.gateway_name

        self.ticks: dict[str, TickData] = {}
        self.reqid: int = 0
        self.kline_stream: bool = False

    def connect(
        self,
        server: str,
        kline_stream: bool,
        proxy_host: str,
        proxy_port: int,
    ) -> None:
        """Start server connection"""
        self.kline_stream = kline_stream

        if server == "REAL":
            self.init(REAL_DATA_HOST, proxy_host, proxy_port, receive_timeout=WEBSOCKET_TIMEOUT)
        else:
            self.init(TESTNET_DATA_HOST, proxy_host, proxy_port, receive_timeout=WEBSOCKET_TIMEOUT)

        self.start()

    def on_connected(self) -> None:
        """Callback when server is connected"""
        self.gateway.write_log("Data Websocket API is connected")

        # Resubscribe market data
        if self.ticks:
            channels = []
            for symbol in self.ticks.keys():
                channels.append(f"{symbol}@ticker")
                channels.append(f"{symbol}@depth10")

                if self.kline_stream:
                    channels.append(f"{symbol}@kline_1m")

            packet: dict = {
                "method": "SUBSCRIBE",
                "params": channels,
                "id": self.reqid
            }
            self.send_packet(packet)

    def subscribe(self, req: SubscribeRequest) -> None:
        """Subscribe market data"""
        if req.symbol in self.ticks:
            return

        contract: ContractData | None = symbol_contract_map.get(req.symbol, None)
        if not contract:
            self.gateway.write_log(f"Failed to subscribe market data, symbol not found: {req.symbol}")
            return

        self.reqid += 1

        # Initialize tick object
        tick: TickData = TickData(
            symbol=req.symbol,
            name=contract.name,
            exchange=Exchange.GLOBAL,
            datetime=datetime.now(UTC_TZ),
            gateway_name=self.gateway_name,
        )
        tick.extra = {}
        self.ticks[req.symbol] = tick

        channels: list[str] = [
            f"{contract.name.lower()}@ticker",
            f"{contract.name.lower()}@depth10"
        ]

        if self.kline_stream:
            channels.append(f"{contract.name.lower()}@kline_1m")

        packet: dict = {
            "method": "SUBSCRIBE",
            "params": channels,
            "id": self.reqid
        }
        self.send_packet(packet)

    def on_packet(self, packet: dict) -> None:
        """Callback of data update"""
        stream: str = packet.get("stream", None)
        if not stream:
            return

        data: dict = packet["data"]

        name, channel = stream.split("@")
        contract: ContractData = name_contract_map[name.upper()]
        tick: TickData = self.ticks[contract.symbol]

        if channel == "ticker":
            tick.volume = float(data["v"])
            tick.turnover = float(data["q"])
            tick.open_price = float(data["o"])
            tick.high_price = float(data["h"])
            tick.low_price = float(data["l"])
            tick.last_price = float(data["c"])
            tick.datetime = generate_datetime(float(data["E"]))
        elif channel == "depth10":
            bids: list = data["b"]
            for n in range(min(10, len(bids))):
                price, volume = bids[n]
                tick.__setattr__("bid_price_" + str(n + 1), float(price))
                tick.__setattr__("bid_volume_" + str(n + 1), float(volume))

            asks: list = data["a"]
            for n in range(min(10, len(asks))):
                price, volume = asks[n]
                tick.__setattr__("ask_price_" + str(n + 1), float(price))
                tick.__setattr__("ask_volume_" + str(n + 1), float(volume))
        else:
            kline_data: dict = data["k"]

            # Check if bar is closed
            bar_ready: bool = kline_data.get("x", False)
            if not bar_ready:
                return

            dt: datetime = generate_datetime(float(kline_data["t"]))

            tick.extra["bar"] = BarData(
                symbol=name.upper(),
                exchange=Exchange.GLOBAL,
                datetime=dt.replace(second=0, microsecond=0),
                interval=Interval.MINUTE,
                volume=float(kline_data["v"]),
                turnover=float(kline_data["q"]),
                open_price=float(kline_data["o"]),
                high_price=float(kline_data["h"]),
                low_price=float(kline_data["l"]),
                close_price=float(kline_data["c"]),
                gateway_name=self.gateway_name
            )

        if tick.last_price:
            tick.localtime = datetime.now()
            self.gateway.on_tick(copy(tick))

    def on_disconnected(self, status_code: int, msg: str) -> None:
        """Callback when server is disconnected"""
        self.gateway.write_log(f"Data Websocket API is disconnected, code: {status_code}, msg: {msg}")

    def on_error(self, e: Exception) -> None:
        """
        Callback when exception raised.
        """
        self.gateway.write_log(f"Data Websocket API exception: {e}")


class TradeApi(WebsocketClient):
    """The trade websocket API of BinanceLinearGateway"""

    def __init__(self, gateway: BinanceLinearGateway) -> None:
        """
        The init method of the api.

        gateway: the parent gateway object for pushing callback data.
        """
        super().__init__()

        self.gateway: BinanceLinearGateway = gateway
        self.gateway_name: str = gateway.gateway_name

        self.key: str = ""
        self.secret: bytes = b""
        self.proxy_port: int = 0
        self.proxy_host: str = ""
        self.server: str = ""

        self.reqid: int = 0
        self.order_count: int = 0
        self.order_prefix: str = ""

        self.reqid_callback_map: dict[int, Callable] = {}
        self.reqid_order_map: dict[int, OrderData] = {}

    def connect(
        self,
        key: str,
        secret: str,
        server: str,
        proxy_host: str,
        proxy_port: int
    ) -> None:
        """Start server connection"""
        self.key = key
        self.secret = secret.encode()
        self.proxy_port = proxy_port
        self.proxy_host = proxy_host
        self.server = server

        self.order_prefix = datetime.now().strftime("%y%m%d%H%M%S")

        if self.server == "REAL":
            self.init(REAL_TRADE_HOST, proxy_host, proxy_port)
        else:
            self.init(TESTNET_TRADE_HOST, proxy_host, proxy_port)

        self.start()

    def sign(self, params: dict) -> None:
        """Generate the signature for the request"""
        timestamp: int = int(time.time() * 1000)
        params["timestamp"] = timestamp

        payload: str = "&".join([f"{k}={v}" for k, v in sorted(params.items())])
        signature: str = hmac.new(
            self.secret,
            payload.encode("utf-8"),
            hashlib.sha256
        ).hexdigest()
        params["signature"] = signature

    def send_order(self, req: OrderRequest) -> str:
        """Send new order"""
        # Get contract
        contract: ContractData | None = symbol_contract_map.get(req.symbol, None)
        if not contract:
            self.gateway.write_log(f"Failed to send order, symbol not found: {req.symbol}")
            return ""

        # Generate new order id
        self.order_count += 1
        orderid: str = self.order_prefix + str(self.order_count)

        # Push a submitting order event
        order: OrderData = req.create_order_data(
            orderid,
            self.gateway_name
        )
        self.gateway.on_order(order)

        # Create order parameters
        params: dict = {
            "apiKey": self.key,
            "symbol": contract.name,
            "side": DIRECTION_VT2BINANCE[req.direction],
            "quantity": format_float(req.volume),
            "newClientOrderId": orderid,
        }

        if req.type == OrderType.MARKET:
            params["type"] = "MARKET"
        elif req.type == OrderType.STOP:
            params["type"] = "STOP_MARKET"
            params["stopPrice"] = format_float(req.price)
        else:
            order_type, time_condition = ORDERTYPE_VT2BINANCE[req.type]
            params["type"] = order_type
            params["timeInForce"] = time_condition
            params["price"] = format_float(req.price)

        self.sign(params)

        self.reqid += 1
        self.reqid_callback_map[self.reqid] = self.on_send_order
        self.reqid_order_map[self.reqid] = order

        packet: dict = {
            "id": self.reqid,
            "method": "order.place",
            "params": params,
        }
        self.send_packet(packet)
        return cast(str, order.vt_orderid)

    def cancel_order(self, req: CancelRequest) -> None:
        """Cancel existing order"""
        contract: ContractData | None = symbol_contract_map.get(req.symbol, None)
        if not contract:
            self.gateway.write_log(f"Failed to cancel order, symbol not found: {req.symbol}")
            return

        params: dict = {
            "apiKey": self.key,
            "symbol": contract.name,
            "origClientOrderId": req.orderid
        }
        self.sign(params)

        self.reqid += 1
        self.reqid_callback_map[self.reqid] = self.on_cancel_order

        packet: dict = {
            "id": self.reqid,
            "method": "order.cancel",
            "params": params,
        }
        self.send_packet(packet)

    def on_connected(self) -> None:
        """Callback when server is connected"""
        self.gateway.write_log("Trade Websocket API is connected")

    def on_packet(self, packet: dict) -> None:
        """Callback of data update"""
        reqid: int = packet.get("id", 0)
        callback: Callable | None = self.reqid_callback_map.get(reqid, None)
        if callback:
            callback(packet)

    def on_send_order(self, packet: dict) -> None:
        """Callback of send order"""
        error: dict = packet.get("error", None)
        if not error:
            return

        error_code: str = error["code"]
        error_msg: str = error["msg"]
        msg: str = f"Failed to send order, error code: {error_code}, message: {error_msg}"
        self.gateway.write_log(msg)

        reqid: int = packet.get("id", 0)
        order: OrderData = self.reqid_order_map.get(reqid, None)
        if order:
            order.status = Status.REJECTED
            self.gateway.on_order(order)

    def on_cancel_order(self, packet: dict) -> None:
        """Callback of cancel order"""
        error: dict = packet.get("error", None)
        if not error:
            return

        error_code: str = error["code"]
        error_msg: str = error["msg"]
        msg: str = f"Failed to cancel order, error code: {error_code}, message: {error_msg}"
        self.gateway.write_log(msg)


def generate_datetime(timestamp: float) -> datetime:
    """Generate datetime object from Binance timestamp"""
    dt: datetime = datetime.fromtimestamp(timestamp / 1000, tz=UTC_TZ)
    return dt


def format_float(f: float) -> str:
    """
    Convert float number to string with correct precision.

    Fix potential error -1111: Parameter "quantity" has too much precision
    """
    return format_float_positional(f, trim="-")
