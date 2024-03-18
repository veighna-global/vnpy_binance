import urllib
import hashlib
import hmac
import time
from copy import copy
from datetime import datetime, timedelta
from enum import Enum
from asyncio import run_coroutine_threadsafe
from zoneinfo import ZoneInfo
from time import sleep

from aiohttp import ClientSSLError

from vnpy_evo.event import Event, EventEngine
from vnpy_evo.trader.constant import (
    Direction,
    Exchange,
    Product,
    Status,
    OrderType,
    Interval
)
from vnpy_evo.trader.gateway import BaseGateway
from vnpy_evo.trader.object import (
    TickData,
    OrderData,
    TradeData,
    AccountData,
    ContractData,
    BarData,
    OrderRequest,
    CancelRequest,
    SubscribeRequest,
    HistoryRequest
)
from vnpy_evo.trader.event import EVENT_TIMER
from vnpy_evo.trader.utility import round_to

from vnpy_rest import RestClient, Request, Response
from vnpy_websocket import WebsocketClient


# Timezone constant
UTC_TZ = ZoneInfo("UTC")

# Real server hosts
REST_HOST: str = "https://api.binance.com"
WEBSOCKET_TRADE_HOST: str = "wss://stream.binance.com:9443/ws/"
WEBSOCKET_DATA_HOST: str = "wss://stream.binance.com:9443/stream"

# Testnet server hosts
TESTNET_REST_HOST: str = "https://testnet.binance.vision"
TESTNET_WEBSOCKET_TRADE_HOST: str = "wss://testnet.binance.vision/ws/"
TESTNET_WEBSOCKET_DATA_HOST: str = "wss://testnet.binance.vision/stream"

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
ORDERTYPE_VT2BINANCE: dict[OrderType, str] = {
    OrderType.LIMIT: "LIMIT",
    OrderType.MARKET: "MARKET",
    OrderType.STOP: "STOP_LOSS"
}
ORDERTYPE_BINANCE2VT: dict[str, OrderType] = {v: k for k, v in ORDERTYPE_VT2BINANCE.items()}

# Direction map
DIRECTION_VT2BINANCE: dict[Direction, str] = {
    Direction.LONG: "BUY",
    Direction.SHORT: "SELL"
}
DIRECTION_BINANCE2VT: dict[str, Direction] = {v: k for k, v in DIRECTION_VT2BINANCE.items()}

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


# Authentication level
class Security(Enum):
    NONE = 0
    SIGNED = 1
    API_KEY = 2


class BinanceSpotGateway(BaseGateway):
    """
    The Binance spot trading gateway for VeighNa.
    """

    default_name: str = "BINANCE_SPOT"

    default_setting: dict = {
        "API Key": "",
        "API Secret": "",
        "Server": ["REAL", "TESTNET"],
        "Kline Stream": ["False", "True"],
        "Proxy Host": "",
        "Proxy Port": 0
    }

    exchanges: Exchange = [Exchange.BINANCE]

    def __init__(self, event_engine: EventEngine, gateway_name: str) -> None:
        """
        The init method of the gateway.

        event_engine: the global event engine object of VeighNa
        gateway_name: the unique name for identifying the gateway
        """
        super().__init__(event_engine, gateway_name)

        self.trade_ws_api: BinanceSpotTradeWebsocketApi = BinanceSpotTradeWebsocketApi(self)
        self.market_ws_api: BinanceSpotDataWebsocketApi = BinanceSpotDataWebsocketApi(self)
        self.rest_api: BinanceSpotRestAPi = BinanceSpotRestAPi(self)

        self.orders: dict[str, OrderData] = {}

    def connect(self, setting: dict):
        """Start server connections"""
        key: str = setting["API Key"]
        secret: str = setting["API Secret"]
        server: str = setting["Server"]
        kline_stream: bool = setting["Kline Stream"] == "True"
        proxy_host: str = setting["Proxy Host"]
        proxy_port: int = setting["Proxy Port"]

        self.rest_api.connect(key, secret, server, proxy_host, proxy_port)
        self.market_ws_api.connect(server, kline_stream, proxy_host, proxy_port)

        self.event_engine.register(EVENT_TIMER, self.process_timer_event)

    def subscribe(self, req: SubscribeRequest) -> None:
        """Subscribe market data"""
        self.market_ws_api.subscribe(req)

    def send_order(self, req: OrderRequest) -> str:
        """Send new order"""
        return self.rest_api.send_order(req)

    def cancel_order(self, req: CancelRequest) -> None:
        """Cancel existing order"""
        self.rest_api.cancel_order(req)

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
        self.trade_ws_api.stop()
        self.market_ws_api.stop()

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


class BinanceSpotRestAPi(RestClient):
    """币安现货REST API"""

    def __init__(self, gateway: BinanceSpotGateway) -> None:
        """
        The init method of the api.

        gateway: the parent gateway object for pushing callback data.
        """
        super().__init__()

        self.gateway: BinanceSpotGateway = gateway
        self.gateway_name: str = gateway.gateway_name

        self.trade_ws_api: BinanceSpotTradeWebsocketApi = self.gateway.trade_ws_api

        self.key: str = ""
        self.secret: str = ""

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
            path: str = request.path

        if security == Security.SIGNED:
            timestamp: int = int(time.time() * 1000)

            if self.time_offset > 0:
                timestamp -= abs(self.time_offset)
            elif self.time_offset < 0:
                timestamp += abs(self.time_offset)

            request.params["timestamp"] = timestamp

            query: str = urllib.parse.urlencode(sorted(request.params.items()))
            signature: bytes = hmac.new(
                self.secret,
                query.encode("utf-8"),
                hashlib.sha256
            ).hexdigest()

            query += "&signature={}".format(signature)
            path: str = request.path + "?" + query

        request.path = path
        request.params = {}
        request.data = {}

        # 添加请求头
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
            self.init(REST_HOST, proxy_host, proxy_port)
        else:
            self.init(TESTNET_REST_HOST, proxy_host, proxy_port)

        self.start()

        self.gateway.write_log("REST API started")

        self.query_time()
        self.query_contract()

    def query_time(self) -> None:
        """Query server time"""
        data: dict = {"security": Security.NONE}

        path: str = "/api/v3/time"

        return self.add_request(
            "GET",
            path,
            callback=self.on_query_time,
            data=data
        )

    def query_account(self) -> None:
        """Query account balance"""
        data: dict = {"security": Security.SIGNED}

        self.add_request(
            method="GET",
            path="/api/v3/account",
            callback=self.on_query_account,
            data=data
        )

    def query_order(self) -> None:
        """Query open orders"""
        data: dict = {"security": Security.SIGNED}

        self.add_request(
            method="GET",
            path="/api/v3/openOrders",
            callback=self.on_query_order,
            data=data
        )

    def query_contract(self) -> None:
        """Query available contracts"""
        data: dict = {"security": Security.NONE}

        self.add_request(
            method="GET",
            path="/api/v3/exchangeInfo",
            callback=self.on_query_contract,
            data=data
        )

    def send_order(self, req: OrderRequest) -> str:
        """Send new order"""
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
        data: dict = {"security": Security.SIGNED}

        params: dict = {
            "symbol": req.symbol.upper(),
            "side": DIRECTION_VT2BINANCE[req.direction],
            "type": ORDERTYPE_VT2BINANCE[req.type],
            "quantity": format(req.volume, "f"),
            "newClientOrderId": orderid,
            "newOrderRespType": "ACK"
        }

        if req.type == OrderType.LIMIT:
            params["timeInForce"] = "GTC"
            params["price"] = str(req.price)
        elif req.type == OrderType.STOP:
            params["type"] = "STOP_LOSS"
            params["stopPrice"] = float(req.price)

        self.add_request(
            method="POST",
            path="/api/v3/order",
            callback=self.on_send_order,
            data=data,
            params=params,
            extra=order,
            on_error=self.on_send_order_error,
            on_failed=self.on_send_order_failed
        )

        return order.vt_orderid

    def cancel_order(self, req: CancelRequest) -> None:
        """Cancel existing order"""
        data: dict = {"security": Security.SIGNED}

        params: dict = {
            "symbol": req.symbol.upper(),
            "origClientOrderId": req.orderid
        }

        order: OrderData = self.gateway.get_order(req.orderid)

        self.add_request(
            method="DELETE",
            path="/api/v3/order",
            callback=self.on_cancel_order,
            params=params,
            data=data,
            on_failed=self.on_cancel_failed,
            extra=order
        )

    def start_user_stream(self) -> Request:
        """Create listen key for user stream"""
        data: dict = {"security": Security.API_KEY}

        self.add_request(
            method="POST",
            path="/api/v3/userDataStream",
            callback=self.on_start_user_stream,
            data=data
        )

    def keep_user_stream(self) -> Request:
        """Extend listen key validity"""
        self.keep_alive_count += 1
        if self.keep_alive_count < 600:
            return
        self.keep_alive_count = 0

        data: dict = {"security": Security.API_KEY}

        params: dict = {"listenKey": self.user_stream_key}

        self.add_request(
            method="PUT",
            path="/api/v3/userDataStream",
            callback=self.on_keep_user_stream,
            params=params,
            data=data,
            on_error=self.on_keep_user_stream_error
        )

    def on_query_time(self, data: dict, request: Request) -> None:
        """Callback of server time query"""
        local_time = int(time.time() * 1000)
        server_time = int(data["serverTime"])
        self.time_offset = local_time - server_time

        self.gateway.write_log(f"Server time updated, local offset: {self.time_offset}ms")

        # Query private data after time offset is calculated
        if self.key and self.secret:
            self.query_account()
            self.query_order()
            self.start_user_stream()

    def on_query_account(self, data: dict, request: Request) -> None:
        """Callback of account balance query"""
        for account_data in data["balances"]:
            account: AccountData = AccountData(
                accountid=account_data["asset"],
                balance=float(account_data["free"]) + float(account_data["locked"]),
                frozen=float(account_data["locked"]),
                gateway_name=self.gateway_name
            )

            if account.balance:
                self.gateway.on_account(account)

        self.gateway.write_log("Account balance data is received")

    def on_query_order(self, data: dict, request: Request) -> None:
        """Callback of open orders query"""
        for d in data:
            if d["type"] not in ORDERTYPE_BINANCE2VT:
                continue

            order: OrderData = OrderData(
                orderid=d["clientOrderId"],
                symbol=d["symbol"].lower(),
                exchange=Exchange.BINANCE,
                price=float(d["price"]),
                volume=float(d["origQty"]),
                type=ORDERTYPE_BINANCE2VT[d["type"]],
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
            base_currency: str = d["baseAsset"]
            quote_currency: str = d["quoteAsset"]
            name: str = f"{base_currency.upper()}/{quote_currency.upper()}"

            pricetick: int = 1
            min_volume: int = 1

            for f in d["filters"]:
                if f["filterType"] == "PRICE_FILTER":
                    pricetick = float(f["tickSize"])
                elif f["filterType"] == "LOT_SIZE":
                    min_volume = float(f["stepSize"])

            contract: ContractData = ContractData(
                symbol=d["symbol"].lower(),
                exchange=Exchange.BINANCE,
                name=name,
                pricetick=pricetick,
                size=1,
                min_volume=min_volume,
                product=Product.SPOT,
                history_data=True,
                gateway_name=self.gateway_name,
                stop_supported=True
            )
            self.gateway.on_contract(contract)

            symbol_contract_map[contract.symbol] = contract

        self.gateway.write_log("Available contracts data is received")

    def on_send_order(self, data: dict, request: Request) -> None:
        """Successful callback of send_order"""
        pass

    def on_send_order_failed(self, status_code: str, request: Request) -> None:
        """Failed callback of send_order"""
        order: OrderData = request.extra
        order.status = Status.REJECTED
        self.gateway.on_order(order)

        msg: str = f"Send order failed, status code: {status_code}, message: {request.response.text}"
        self.gateway.write_log(msg)

    def on_send_order_error(self, exception_type: type, exception_value: Exception, tb, request: Request) -> None:
        """Error callback of send_order"""
        order: OrderData = request.extra
        order.status = Status.REJECTED
        self.gateway.on_order(order)

        if not issubclass(exception_type, (ConnectionError, ClientSSLError)):
            self.on_error(exception_type, exception_value, tb, request)

    def on_cancel_order(self, data: dict, request: Request) -> None:
        """Successful callback of cancel_order"""
        pass

    def on_cancel_failed(self, status_code: str, request: Request) -> None:
        """Failed callback of cancel_order"""
        if request.extra:
            order = request.extra
            order.status = Status.REJECTED
            self.gateway.on_order(order)

        msg = f"Cancel orde failed, status code: {status_code}, message: {request.response.text}, order: {request.extra} "
        self.gateway.write_log(msg)

    def on_start_user_stream(self, data: dict, request: Request) -> None:
        """Successful callback of start_user_stream"""
        self.user_stream_key = data["listenKey"]
        self.keep_alive_count = 0

        if self.server == "REAL":
            url = WEBSOCKET_TRADE_HOST + self.user_stream_key
        else:
            url = TESTNET_WEBSOCKET_TRADE_HOST + self.user_stream_key

        self.trade_ws_api.connect(url, self.proxy_host, self.proxy_port)

    def on_keep_user_stream(self, data: dict, request: Request) -> None:
        """Successful callback of keep_user_stream"""
        pass

    def on_keep_user_stream_error(self, exception_type: type, exception_value: Exception, tb, request: Request) -> None:
        """Error callback of keep_user_stream"""
        if not issubclass(exception_type, TimeoutError):        # Ignore timeout exception
            self.on_error(exception_type, exception_value, tb, request)

    def query_history(self, req: HistoryRequest) -> list[BarData]:
        """Query kline history data"""
        history: list[BarData] = []
        limit: int = 1000
        start_time: int = int(datetime.timestamp(req.start))

        while True:
            # Create query parameters
            params: dict = {
                "symbol": req.symbol.upper(),
                "interval": INTERVAL_VT2BINANCE[req.interval],
                "limit": limit,
                "startTime": start_time * 1000,
            }

            if req.end:
                end_time: int = int(datetime.timestamp(req.end))
                params["endTime"] = end_time * 1000     # Convert to milliseconds

            resp: Response = self.request(
                "GET",
                "/api/v3/klines",
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
                    msg: str = f"No kline history data is received, start time: {start_time}"
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
                    buf.append(bar)

                history.extend(buf)

                begin: datetime = buf[0].datetime
                end: datetime = buf[-1].datetime
                msg: str = f"Query kline history finished, {req.symbol} - {req.interval.value}, {begin} - {end}"
                self.gateway.write_log(msg)

                # Break the loop if the latest data received
                if len(data) < limit:
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


class BinanceSpotTradeWebsocketApi(WebsocketClient):
    """The trade websocket API of BinanceSpotGateway"""

    def __init__(self, gateway: BinanceSpotGateway) -> None:
        """
        The init method of the api.

        gateway: the parent gateway object for pushing callback data.
        """
        super().__init__()

        self.gateway: BinanceSpotGateway = gateway
        self.gateway_name: str = gateway.gateway_name

    def connect(self, url: str, proxy_host: int, proxy_port: int) -> None:
        """Start server connection"""
        self.init(url, proxy_host, proxy_port, receive_timeout=WEBSOCKET_TIMEOUT)
        self.start()

    def on_connected(self) -> None:
        """Callback when server is connected"""
        self.gateway.write_log("Trade Websocket API is connected")

    def on_packet(self, packet: dict) -> None:
        """Callback of data update"""
        if packet["e"] == "outboundAccountPosition":
            self.on_account(packet)
        elif packet["e"] == "executionReport":
            self.on_order(packet)
        elif packet["e"] == "listenKeyExpired":
            self.on_listen_key_expired()

    def on_listen_key_expired(self) -> None:
        """Callback of listen key expired"""
        self.gateway.write_log("Listen key is expired")
        self.disconnect()

    def disconnect(self) -> None:
        """"Close server connection"""
        self._active = False
        ws = self._ws
        if ws:
            coro = ws.close()
            run_coroutine_threadsafe(coro, self._loop)

    def on_account(self, packet: dict) -> None:
        """Callback of account balance update"""
        for d in packet["B"]:
            account: AccountData = AccountData(
                accountid=d["a"],
                balance=float(d["f"]) + float(d["l"]),
                frozen=float(d["l"]),
                gateway_name=self.gateway_name
            )

            if account.balance:
                self.gateway.on_account(account)

    def on_order(self, packet: dict) -> None:
        """Callback of order and trade update"""
        if packet["o"] not in ORDERTYPE_BINANCE2VT:
            return

        if packet["C"] == "":
            orderid: str = packet["c"]
        else:
            orderid: str = packet["C"]

        offset = self.gateway.get_order(orderid).offset if self.gateway.get_order(orderid) else None

        order: OrderData = OrderData(
            symbol=packet["s"].lower(),
            exchange=Exchange.BINANCE,
            orderid=orderid,
            type=ORDERTYPE_BINANCE2VT[packet["o"]],
            direction=DIRECTION_BINANCE2VT[packet["S"]],
            price=float(packet["p"]),
            volume=float(packet["q"]),
            traded=float(packet["z"]),
            status=STATUS_BINANCE2VT[packet["X"]],
            datetime=generate_datetime(packet["O"]),
            gateway_name=self.gateway_name,
            offset=offset
        )

        self.gateway.on_order(order)

        # Round trade volume to meet step size
        trade_volume = float(packet["l"])
        contract: ContractData = symbol_contract_map.get(order.symbol, None)
        if contract:
            trade_volume = round_to(trade_volume, contract.min_volume)

        if not trade_volume:
            return

        trade: TradeData = TradeData(
            symbol=order.symbol,
            exchange=order.exchange,
            orderid=order.orderid,
            tradeid=packet["t"],
            direction=order.direction,
            price=float(packet["L"]),
            volume=trade_volume,
            datetime=generate_datetime(packet["T"]),
            gateway_name=self.gateway_name,
            offset=offset
        )
        self.gateway.on_trade(trade)

    def on_disconnected(self) -> None:
        """Callback when server is disconnected"""
        self.gateway.write_log("Trade Websocket API is disconnected")
        self.gateway.rest_api.start_user_stream()


class BinanceSpotDataWebsocketApi(WebsocketClient):
    """The data websocket API of BinanceSpotGateway"""

    def __init__(self, gateway: BinanceSpotGateway) -> None:
        """
        The init method of the api.

        gateway: the parent gateway object for pushing callback data.
        """
        super().__init__()

        self.gateway: BinanceSpotGateway = gateway
        self.gateway_name: str = gateway.gateway_name

        self.ticks: dict[str, TickData] = {}
        self.reqid: int = 0

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
            self.init(WEBSOCKET_DATA_HOST, proxy_host, proxy_port, receive_timeout=WEBSOCKET_TIMEOUT)
        else:
            self.init(TESTNET_WEBSOCKET_DATA_HOST, proxy_host, proxy_port, receive_timeout=WEBSOCKET_TIMEOUT)

        self.start()

    def on_connected(self) -> None:
        """Callback when server is connected"""
        self.gateway.write_log("Data Websocket API is connected")

        # 重新订阅行情
        if self.ticks:
            channels = []
            for symbol in self.ticks.keys():
                channels.append(f"{symbol}@ticker")
                channels.append(f"{symbol}@depth10")

                if self.kline_stream:
                    channels.append(f"{symbol}@kline_1m")

            req: dict = {
                "method": "SUBSCRIBE",
                "params": channels,
                "id": self.reqid
            }
            self.send_packet(req)

    def subscribe(self, req: SubscribeRequest) -> None:
        """Subscribe market data"""
        if req.symbol in self.ticks:
            return

        if req.symbol not in symbol_contract_map:
            self.gateway.write_log(f"Symbol not found {req.symbol}")
            return

        self.reqid += 1

        # Initialize tick object
        tick: TickData = TickData(
            symbol=req.symbol,
            name=symbol_contract_map[req.symbol].name,
            exchange=Exchange.BINANCE,
            datetime=datetime.now(UTC_TZ),
            gateway_name=self.gateway_name,
        )
        tick.extra = {}
        self.ticks[req.symbol] = tick

        channels = [
            f"{req.symbol}@ticker",
            f"{req.symbol}@depth10"
        ]

        if self.kline_stream:
            channels.append(f"{req.symbol.lower()}@kline_1m")

        req: dict = {
            "method": "SUBSCRIBE",
            "params": channels,
            "id": self.reqid
        }
        self.send_packet(req)

    def on_packet(self, packet: dict) -> None:
        """Callback of data update"""
        stream: str = packet.get("stream", None)

        if not stream:
            return

        data: dict = packet["data"]

        symbol, channel = stream.split("@")
        tick: TickData = self.ticks[symbol]

        if channel == "ticker":
            tick.volume = float(data['v'])
            tick.turnover = float(data['q'])
            tick.open_price = float(data['o'])
            tick.high_price = float(data['h'])
            tick.low_price = float(data['l'])
            tick.last_price = float(data['c'])
            tick.datetime = generate_datetime(float(data['E']))
        elif channel == "depth10":
            bids: list = data["bids"]
            for n in range(min(10, len(bids))):
                price, volume = bids[n]
                tick.__setattr__("bid_price_" + str(n + 1), float(price))
                tick.__setattr__("bid_volume_" + str(n + 1), float(volume))

            asks: list = data["asks"]
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

            dt: datetime = generate_datetime(float(kline_data['t']))

            tick.extra["bar"] = BarData(
                symbol=symbol.upper(),
                exchange=Exchange.BINANCE,
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

    def on_disconnected(self) -> None:
        """Callback when server is disconnected"""
        self.gateway.write_log("Data Websocket API is disconnected")


def generate_datetime(timestamp: float) -> datetime:
    """Generate datetime object from Binance timestamp"""
    dt: datetime = datetime.fromtimestamp(timestamp / 1000)
    dt: datetime = dt.replace(tzinfo=UTC_TZ)
    return dt
