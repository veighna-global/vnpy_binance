"""
Binance Portfolio Margin Gateway for VeighNa.

This module provides trading functionality for Binance Portfolio Margin account,
which supports unified account management across USDT-M futures, Coin-M futures,
and cross margin trading.
"""

import hashlib
import hmac
import time
import urllib.parse
from copy import copy
from enum import Enum
from typing import Any
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
REAL_REST_HOST: str = "https://papi.binance.com"
REAL_UM_REST_HOST: str = "https://fapi.binance.com"
REAL_CM_REST_HOST: str = "https://dapi.binance.com"
REAL_MARGIN_REST_HOST: str = "https://api.binance.com"

REAL_USER_HOST: str = "wss://fstream.binance.com/pm/ws/"
REAL_UM_DATA_HOST: str = "wss://fstream.binance.com/stream"
REAL_CM_DATA_HOST: str = "wss://dstream.binance.com/stream"
REAL_MARGIN_DATA_HOST: str = "wss://stream.binance.com:9443/stream"

# Testnet server hosts
TESTNET_REST_HOST: str = "https://testnet.binancefuture.com"
TESTNET_UM_REST_HOST: str = "https://testnet.binancefuture.com"
TESTNET_CM_REST_HOST: str = "https://testnet.binancefuture.com"
TESTNET_MARGIN_REST_HOST: str = "https://testnet.binance.vision"

TESTNET_USER_HOST: str = "wss://stream.binancefuture.com/ws/"
TESTNET_UM_DATA_HOST: str = "wss://stream.binancefuture.com/stream"
TESTNET_CM_DATA_HOST: str = "wss://dstream.binancefuture.com/stream"
TESTNET_MARGIN_DATA_HOST: str = "wss://testnet.binance.vision/stream"

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
ORDERTYPE_BINANCE2VT: dict[tuple[str, str], OrderType] = {
    v: k for k, v in ORDERTYPE_VT2BINANCE.items()
}

# Direction map
DIRECTION_VT2BINANCE: dict[Direction, str] = {
    Direction.LONG: "BUY",
    Direction.SHORT: "SELL"
}
DIRECTION_BINANCE2VT: dict[str, Direction] = {
    v: k for k, v in DIRECTION_VT2BINANCE.items()
}

# Product map for futures
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

# Set websocket timeout to 24 hours
WEBSOCKET_TIMEOUT = 24 * 60 * 60


class MarketType(Enum):
    """Market type for portfolio margin account"""
    UM = "um"           # USDT-M Futures
    CM = "cm"           # Coin-M Futures
    MARGIN = "margin"   # Cross Margin


def get_market_type(symbol: str) -> MarketType:
    """
    Determine market type from symbol.

    Parameters:
        symbol: VeighNa symbol string

    Returns:
        MarketType enum value
    """
    if "_SWAP_BINANCE" in symbol:
        base = symbol.replace("_SWAP_BINANCE", "")
        if base.endswith("USDT") or base.endswith("USDC"):
            return MarketType.UM
        else:
            return MarketType.CM
    elif "_SPOT_BINANCE" in symbol:
        return MarketType.MARGIN
    elif "_BINANCE" in symbol:
        # Delivery futures
        base = symbol.split("_")[0]
        if "USDT" in base or "USDC" in base:
            return MarketType.UM
        else:
            return MarketType.CM
    return MarketType.UM


def generate_datetime(timestamp: float) -> datetime:
    """
    Generate datetime object from Binance timestamp.

    Parameters:
        timestamp: Binance timestamp in milliseconds

    Returns:
        Datetime object with UTC timezone
    """
    dt: datetime = datetime.fromtimestamp(timestamp / 1000, tz=UTC_TZ)
    return dt


def format_float(f: float) -> str:
    """
    Convert float number to string with correct precision.

    Parameters:
        f: The floating point number to format

    Returns:
        Formatted string representation of the number
    """
    return format_float_positional(f, trim="-")


class BinancePortfolioGateway(BaseGateway):
    """
    The Binance portfolio margin trading gateway for VeighNa.

    This gateway provides unified trading functionality for Binance portfolio margin account,
    which supports USDT-M futures, Coin-M futures, and cross margin trading.

    Features:
    1. Unified account management across all markets
    2. Real-time market data with optional kline streaming
    3. Only support crossed position and one-way mode
    """

    default_name: str = "BINANCE_PORTFOLIO"

    default_setting: dict = {
        "API Key": "",
        "API Secret": "",
        "Server": ["REAL", "TESTNET"],
        "Kline Stream": ["False", "True"],
        "Proxy Host": "",
        "Proxy Port": 0
    }

    exchanges: list[Exchange] = [Exchange.GLOBAL]

    def __init__(self, event_engine: EventEngine, gateway_name: str) -> None:
        """
        The init method of the gateway.

        Parameters:
            event_engine: the global event engine object of VeighNa
            gateway_name: the unique name for identifying the gateway
        """
        super().__init__(event_engine, gateway_name)

        self.user_api: UserApi = UserApi(self)
        self.um_md_api: UmMdApi = UmMdApi(self)
        self.cm_md_api: CmMdApi = CmMdApi(self)
        self.margin_md_api: MarginMdApi = MarginMdApi(self)
        self.rest_api: RestApi = RestApi(self)

        self.orders: dict[str, OrderData] = {}
        self.symbol_contract_map: dict[str, ContractData] = {}
        self.um_name_contract_map: dict[str, ContractData] = {}
        self.cm_name_contract_map: dict[str, ContractData] = {}
        self.margin_name_contract_map: dict[str, ContractData] = {}

    def connect(self, setting: dict) -> None:
        """
        Start server connections.

        Parameters:
            setting: A dictionary containing connection parameters
        """
        key: str = setting["API Key"]
        secret: str = setting["API Secret"]
        server: str = setting["Server"]
        kline_stream: bool = setting["Kline Stream"] == "True"
        proxy_host: str = setting["Proxy Host"]
        proxy_port: int = setting["Proxy Port"]

        self.rest_api.connect(key, secret, server, proxy_host, proxy_port)
        self.um_md_api.connect(server, kline_stream, proxy_host, proxy_port)
        self.cm_md_api.connect(server, kline_stream, proxy_host, proxy_port)
        self.margin_md_api.connect(server, kline_stream, proxy_host, proxy_port)

        self.event_engine.register(EVENT_TIMER, self.process_timer_event)

    def subscribe(self, req: SubscribeRequest) -> None:
        """
        Subscribe to market data.

        Parameters:
            req: Subscription request object
        """
        market_type: MarketType = get_market_type(req.symbol)

        if market_type == MarketType.UM:
            self.um_md_api.subscribe(req)
        elif market_type == MarketType.CM:
            self.cm_md_api.subscribe(req)
        else:
            self.margin_md_api.subscribe(req)

    def send_order(self, req: OrderRequest) -> str:
        """
        Send new order.

        Parameters:
            req: Order request object

        Returns:
            str: The VeighNa order ID if successful, empty string if failed
        """
        return self.rest_api.send_order(req)

    def cancel_order(self, req: CancelRequest) -> None:
        """
        Cancel existing order.

        Parameters:
            req: Cancel request object
        """
        self.rest_api.cancel_order(req)

    def query_account(self) -> None:
        """Query account balance."""
        pass

    def query_position(self) -> None:
        """Query current positions."""
        pass

    def query_history(self, req: HistoryRequest) -> list[BarData]:
        """
        Query historical kline data.

        Parameters:
            req: History request object

        Returns:
            list[BarData]: List of historical kline data bars
        """
        return self.rest_api.query_history(req)

    def close(self) -> None:
        """Close server connections."""
        self.rest_api.stop()
        self.user_api.stop()
        self.um_md_api.stop()
        self.cm_md_api.stop()
        self.margin_md_api.stop()

    def process_timer_event(self, event: Event) -> None:
        """
        Process timer task.

        Parameters:
            event: Timer event object
        """
        self.rest_api.keep_user_stream()

    def on_order(self, order: OrderData) -> None:
        """
        Save a copy of order and then push to event engine.

        Parameters:
            order: Order data object
        """
        self.orders[order.orderid] = copy(order)
        super().on_order(order)

    def get_order(self, orderid: str) -> OrderData | None:
        """
        Get previously saved order by order id.

        Parameters:
            orderid: The ID of the order to retrieve

        Returns:
            Order data object if found, None otherwise
        """
        return self.orders.get(orderid)

    def on_contract(self, contract: ContractData) -> None:
        """
        Save contract data in mappings and push to event engine.

        Parameters:
            contract: Contract data object
        """
        self.symbol_contract_map[contract.symbol] = contract

        market_type: MarketType = get_market_type(contract.symbol)
        if market_type == MarketType.UM:
            self.um_name_contract_map[contract.name] = contract
        elif market_type == MarketType.CM:
            self.cm_name_contract_map[contract.name] = contract
        else:
            self.margin_name_contract_map[contract.name] = contract

        super().on_contract(contract)

    def get_contract_by_symbol(self, symbol: str) -> ContractData | None:
        """
        Get contract data by VeighNa symbol.

        Parameters:
            symbol: VeighNa symbol

        Returns:
            Contract data object if found, None otherwise
        """
        return self.symbol_contract_map.get(symbol, None)

    def get_contract_by_name(self, name: str, market_type: MarketType) -> ContractData | None:
        """
        Get contract data by exchange symbol name and market type.

        Parameters:
            name: Exchange symbol name
            market_type: Market type for contract lookup

        Returns:
            Contract data object if found, None otherwise
        """
        if market_type == MarketType.UM:
            return self.um_name_contract_map.get(name, None)
        elif market_type == MarketType.CM:
            return self.cm_name_contract_map.get(name, None)
        else:
            return self.margin_name_contract_map.get(name, None)

    def get_futures_contract_by_name(self, name: str) -> ContractData | None:
        """
        Get futures contract data by exchange symbol name.

        Parameters:
            name: Exchange symbol name

        Returns:
            Contract data object if found, None otherwise
        """
        contract: ContractData | None = self.um_name_contract_map.get(name, None)
        if contract:
            return contract
        return self.cm_name_contract_map.get(name, None)


class RestApi(RestClient):
    """
    The REST API of BinancePortfolioGateway.

    This class handles HTTP requests to Binance API endpoints, including:
    - Authentication and signature generation
    - Contract information queries for all markets
    - Account and position queries
    - Order management via Portfolio Margin endpoints
    - Historical data queries
    - User data stream management
    """

    def __init__(self, gateway: BinancePortfolioGateway) -> None:
        """
        The init method of the API.

        Parameters:
            gateway: the parent gateway object
        """
        super().__init__()

        self.gateway: BinancePortfolioGateway = gateway
        self.gateway_name: str = gateway.gateway_name

        self.user_api: UserApi = self.gateway.user_api

        self.key: str = ""
        self.secret: bytes = b""

        self.user_stream_key: str = ""
        self.keep_alive_count: int = 0
        self.time_offset: int = 0

        self.order_count: int = 1_000_000
        self.order_prefix: str = ""

        # Additional REST clients for exchange info queries
        self.um_client: RestClient | None = None
        self.cm_client: RestClient | None = None
        self.margin_client: RestClient | None = None

    def sign(self, request: Request) -> Request:
        """
        Standard callback for signing a request.

        Parameters:
            request: Request object to be signed

        Returns:
            Request: Modified request with authentication parameters
        """
        if request.params:
            path: str = request.path + "?" + urllib.parse.urlencode(request.params)
        else:
            request.params = {}
            path = request.path

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

        request.headers = {
            "Content-Type": "application/x-www-form-urlencoded",
            "Accept": "application/json",
            "X-MBX-APIKEY": self.key,
            "Connection": "close"
        }

        return request

    def connect(
        self,
        key: str,
        secret: str,
        server: str,
        proxy_host: str,
        proxy_port: int
    ) -> None:
        """Start server connection."""
        self.key = key
        self.secret = secret.encode()
        self.proxy_port = proxy_port
        self.proxy_host = proxy_host
        self.server = server

        self.order_prefix = datetime.now().strftime("%y%m%d%H%M%S")

        # Initialize main REST client (Portfolio Margin)
        if self.server == "REAL":
            self.init(REAL_REST_HOST, proxy_host, proxy_port)
        else:
            self.init(TESTNET_REST_HOST, proxy_host, proxy_port)

        self.start()
        self.gateway.write_log("REST API started")

        # Initialize additional REST clients for exchange info
        self._init_market_clients(proxy_host, proxy_port)

        self.query_time()

    def _init_market_clients(self, proxy_host: str, proxy_port: int) -> None:
        """Initialize REST clients for each market's exchange info."""
        if self.server == "REAL":
            um_host = REAL_UM_REST_HOST
            cm_host = REAL_CM_REST_HOST
            margin_host = REAL_MARGIN_REST_HOST
        else:
            um_host = TESTNET_UM_REST_HOST
            cm_host = TESTNET_CM_REST_HOST
            margin_host = TESTNET_MARGIN_REST_HOST

        # UM client
        self.um_client = RestClient()
        self.um_client.init(um_host, proxy_host, proxy_port)
        self.um_client.start()

        # CM client
        self.cm_client = RestClient()
        self.cm_client.init(cm_host, proxy_host, proxy_port)
        self.cm_client.start()

        # Margin client
        self.margin_client = RestClient()
        self.margin_client.init(margin_host, proxy_host, proxy_port)
        self.margin_client.start()

    def query_time(self) -> None:
        """Query server time to calculate local time offset."""
        path: str = "/papi/v1/time"

        self.add_request(
            "GET",
            path,
            callback=self.on_query_time
        )

    def query_account(self) -> None:
        """Query account balance for all markets."""
        # Query UM account
        path: str = "/papi/v1/um/account"
        self.add_request(
            method="GET",
            path=path,
            callback=self.on_query_um_account,
        )

        # Query CM account
        path = "/papi/v1/cm/account"
        self.add_request(
            method="GET",
            path=path,
            callback=self.on_query_cm_account,
        )

        # Query margin balance
        path = "/papi/v1/balance"
        self.add_request(
            method="GET",
            path=path,
            callback=self.on_query_margin_account,
        )

    def query_position(self) -> None:
        """Query holding positions for futures markets."""
        # Query UM positions
        path: str = "/papi/v1/um/positionRisk"
        self.add_request(
            method="GET",
            path=path,
            callback=self.on_query_um_position,
        )

        # Query CM positions
        path = "/papi/v1/cm/positionRisk"
        self.add_request(
            method="GET",
            path=path,
            callback=self.on_query_cm_position,
        )

    def query_order(self) -> None:
        """Query open orders for all markets."""
        # Query UM orders
        path: str = "/papi/v1/um/openOrders"
        self.add_request(
            method="GET",
            path=path,
            callback=self.on_query_um_order,
        )

        # Query CM orders
        path = "/papi/v1/cm/openOrders"
        self.add_request(
            method="GET",
            path=path,
            callback=self.on_query_cm_order,
        )

        # Query margin orders
        path = "/papi/v1/margin/openOrders"
        self.add_request(
            method="GET",
            path=path,
            callback=self.on_query_margin_order,
        )

    def query_contract(self) -> None:
        """Query available contracts for all markets."""
        # Query UM contracts via fapi
        if self.um_client:
            resp: Response = self.um_client.request(
                "GET",
                "/fapi/v1/exchangeInfo"
            )
            if resp.status_code == 200:
                self.on_query_um_contract(resp.json())
            else:
                self.gateway.write_log(f"Query UM contract failed: {resp.text}")

        # Query CM contracts via dapi
        if self.cm_client:
            resp = self.cm_client.request(
                "GET",
                "/dapi/v1/exchangeInfo"
            )
            if resp.status_code == 200:
                self.on_query_cm_contract(resp.json())
            else:
                self.gateway.write_log(f"Query CM contract failed: {resp.text}")

        # Query margin contracts via spot api
        if self.margin_client:
            resp = self.margin_client.request(
                "GET",
                "/api/v3/exchangeInfo"
            )
            if resp.status_code == 200:
                self.on_query_margin_contract(resp.json())
            else:
                self.gateway.write_log(f"Query margin contract failed: {resp.text}")

    def send_order(self, req: OrderRequest) -> str:
        """
        Send new order via REST API.

        Parameters:
            req: Order request object

        Returns:
            vt_orderid: The VeighNa order ID if successful, empty string otherwise
        """
        contract: ContractData | None = self.gateway.get_contract_by_symbol(req.symbol)
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
            "symbol": contract.name,
            "side": DIRECTION_VT2BINANCE[req.direction],
            "quantity": format_float(req.volume),
            "newClientOrderId": orderid,
        }

        if req.type == OrderType.MARKET:
            params["type"] = "MARKET"
        else:
            order_type, time_condition = ORDERTYPE_VT2BINANCE[req.type]
            params["type"] = order_type
            params["timeInForce"] = time_condition
            params["price"] = format_float(req.price)

        # Select endpoint based on market type
        market_type: MarketType = get_market_type(req.symbol)
        if market_type == MarketType.UM:
            path = "/papi/v1/um/order"
        elif market_type == MarketType.CM:
            path = "/papi/v1/cm/order"
        else:
            path = "/papi/v1/margin/order"

        self.add_request(
            method="POST",
            path=path,
            callback=self.on_send_order,
            params=params,
            extra=order,
            on_failed=self.on_send_order_failed,
            on_error=self.on_send_order_error
        )

        return order.vt_orderid

    def cancel_order(self, req: CancelRequest) -> None:
        """
        Cancel existing order.

        Parameters:
            req: Cancel request object
        """
        order: OrderData | None = self.gateway.get_order(req.orderid)
        if not order:
            self.gateway.write_log(f"Failed to cancel order, order not found: {req.orderid}")
            return

        contract: ContractData | None = self.gateway.get_contract_by_symbol(order.symbol)
        if not contract:
            self.gateway.write_log(f"Failed to cancel order, symbol not found: {order.symbol}")
            return

        params: dict = {
            "symbol": contract.name,
            "origClientOrderId": req.orderid
        }

        # Select endpoint based on market type
        market_type: MarketType = get_market_type(order.symbol)
        if market_type == MarketType.UM:
            path = "/papi/v1/um/order"
        elif market_type == MarketType.CM:
            path = "/papi/v1/cm/order"
        else:
            path = "/papi/v1/margin/order"

        self.add_request(
            method="DELETE",
            path=path,
            callback=self.on_cancel_order,
            params=params,
            extra=order,
            on_failed=self.on_cancel_order_failed
        )

    def start_user_stream(self) -> None:
        """Create listen key for user stream."""
        path: str = "/papi/v1/listenKey"

        self.add_request(
            method="POST",
            path=path,
            callback=self.on_start_user_stream,
        )

    def keep_user_stream(self) -> None:
        """Extend listen key validity."""
        if not self.user_stream_key:
            return

        self.keep_alive_count += 1
        if self.keep_alive_count < 600:
            return
        self.keep_alive_count = 0

        params: dict = {"listenKey": self.user_stream_key}
        path: str = "/papi/v1/listenKey"

        self.add_request(
            method="PUT",
            path=path,
            callback=self.on_keep_user_stream,
            params=params,
            on_error=self.on_keep_user_stream_error
        )

    def on_query_time(self, data: dict, request: Request) -> None:
        """Callback of server time query."""
        local_time: int = int(time.time() * 1000)
        server_time: int = int(data["serverTime"])
        self.time_offset = local_time - server_time

        self.gateway.write_log(f"Server time updated, local offset: {self.time_offset}ms")

        # Query contracts after time sync
        self.query_contract()

        # Query private data if authenticated
        if self.key and self.secret:
            self.query_order()
            self.query_account()
            self.query_position()
            self.start_user_stream()

    def on_query_um_account(self, data: dict, request: Request) -> None:
        """Callback of UM account balance query."""
        for asset in data["assets"]:
            account: AccountData = AccountData(
                accountid=asset["asset"] + "_UM",
                balance=float(asset["crossWalletBalance"]),
                frozen=float(asset["maintMargin"]),
                gateway_name=self.gateway_name
            )

            if account.balance:
                self.gateway.on_account(account)

        self.gateway.write_log("UM account data received")

    def on_query_cm_account(self, data: dict, request: Request) -> None:
        """Callback of CM account balance query."""
        for asset in data["assets"]:
            account: AccountData = AccountData(
                accountid=asset["asset"] + "_CM",
                balance=float(asset["crossWalletBalance"]),
                frozen=float(asset["maintMargin"]),
                gateway_name=self.gateway_name
            )

            if account.balance:
                self.gateway.on_account(account)

        self.gateway.write_log("CM account data received")

    def on_query_margin_account(self, data: list, request: Request) -> None:
        """Callback of margin account balance query."""
        for asset in data:
            account: AccountData = AccountData(
                accountid=asset["asset"] + "_MARGIN",
                balance=float(asset["crossMarginAsset"]),
                frozen=float(asset["crossMarginLocked"]),
                gateway_name=self.gateway_name
            )

            if account.balance:
                self.gateway.on_account(account)

        self.gateway.write_log("Margin account data received")

    def on_query_um_position(self, data: list, request: Request) -> None:
        """Callback of UM positions query."""
        for d in data:
            name: str = d["symbol"]
            contract: ContractData | None = self.gateway.get_contract_by_name(name, MarketType.UM)
            if not contract:
                continue

            volume_str = d["positionAmt"]
            if "." in volume_str:
                volume = float(volume_str)
            else:
                volume = int(volume_str)

            position: PositionData = PositionData(
                symbol=contract.symbol,
                exchange=Exchange.GLOBAL,
                direction=Direction.NET,
                volume=volume,
                price=float(d["entryPrice"]),
                pnl=float(d["unRealizedProfit"]),
                gateway_name=self.gateway_name,
            )

            if position.volume:
                self.gateway.on_position(position)

        self.gateway.write_log("UM position data received")

    def on_query_cm_position(self, data: list, request: Request) -> None:
        """Callback of CM positions query."""
        for d in data:
            name: str = d["symbol"]
            contract: ContractData | None = self.gateway.get_contract_by_name(name, MarketType.CM)
            if not contract:
                continue

            volume_str = d["positionAmt"]
            if "." in volume_str:
                volume = float(volume_str)
            else:
                volume = int(volume_str)

            position: PositionData = PositionData(
                symbol=contract.symbol,
                exchange=Exchange.GLOBAL,
                direction=Direction.NET,
                volume=volume,
                price=float(d["entryPrice"]),
                pnl=float(d["unRealizedProfit"]),
                gateway_name=self.gateway_name,
            )

            if position.volume:
                self.gateway.on_position(position)

        self.gateway.write_log("CM position data received")

    def on_query_um_order(self, data: list, request: Request) -> None:
        """Callback of UM open orders query."""
        for d in data:
            key: tuple[str, str] = (d["type"], d["timeInForce"])
            order_type: OrderType | None = ORDERTYPE_BINANCE2VT.get(key, None)
            if not order_type:
                continue

            contract: ContractData | None = self.gateway.get_contract_by_name(d["symbol"], MarketType.UM)
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
                status=STATUS_BINANCE2VT.get(d["status"], Status.SUBMITTING),
                datetime=generate_datetime(d["time"]),
                gateway_name=self.gateway_name,
            )
            self.gateway.on_order(order)

        self.gateway.write_log("UM order data received")

    def on_query_cm_order(self, data: list, request: Request) -> None:
        """Callback of CM open orders query."""
        for d in data:
            key: tuple[str, str] = (d["type"], d["timeInForce"])
            order_type: OrderType | None = ORDERTYPE_BINANCE2VT.get(key, None)
            if not order_type:
                continue

            contract: ContractData | None = self.gateway.get_contract_by_name(d["symbol"], MarketType.CM)
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
                status=STATUS_BINANCE2VT.get(d["status"], Status.SUBMITTING),
                datetime=generate_datetime(d["time"]),
                gateway_name=self.gateway_name,
            )
            self.gateway.on_order(order)

        self.gateway.write_log("CM order data received")

    def on_query_margin_order(self, data: list, request: Request) -> None:
        """Callback of margin open orders query."""
        for d in data:
            key: tuple[str, str] = (d["type"], d["timeInForce"])
            order_type: OrderType | None = ORDERTYPE_BINANCE2VT.get(key, None)
            if not order_type:
                continue

            contract: ContractData | None = self.gateway.get_contract_by_name(d["symbol"], MarketType.MARGIN)
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
                status=STATUS_BINANCE2VT.get(d["status"], Status.SUBMITTING),
                datetime=generate_datetime(d["time"]),
                gateway_name=self.gateway_name,
            )
            self.gateway.on_order(order)

        self.gateway.write_log("Margin order data received")

    def on_query_um_contract(self, data: dict) -> None:
        """Callback of UM contracts query."""
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

            product: Product | None = PRODUCT_BINANCE2VT.get(d["contractType"], None)
            if product == Product.SWAP:
                symbol = d["symbol"] + "_SWAP_BINANCE"
            elif product == Product.FUTURES:
                symbol = d["symbol"] + "_BINANCE"
            else:
                continue

            contract: ContractData = ContractData(
                symbol=symbol,
                exchange=Exchange.GLOBAL,
                name=d["symbol"],
                pricetick=pricetick,
                size=1,
                min_volume=min_volume,
                max_volume=max_volume,
                product=product,
                net_position=True,
                history_data=True,
                gateway_name=self.gateway_name,
                stop_supported=False
            )
            self.gateway.on_contract(contract)

        self.gateway.write_log("UM contract data received")

    def on_query_cm_contract(self, data: dict) -> None:
        """Callback of CM contracts query."""
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

            product: Product | None = PRODUCT_BINANCE2VT.get(d["contractType"], None)
            if product == Product.SWAP:
                symbol = d["symbol"].replace("_PERP", "") + "_SWAP_BINANCE"
            elif product == Product.FUTURES:
                symbol = d["symbol"] + "_BINANCE"
            else:
                continue

            contract: ContractData = ContractData(
                symbol=symbol,
                exchange=Exchange.GLOBAL,
                name=d["symbol"],
                pricetick=pricetick,
                size=1,
                min_volume=min_volume,
                max_volume=max_volume,
                product=product,
                net_position=True,
                history_data=True,
                gateway_name=self.gateway_name,
                stop_supported=False
            )
            self.gateway.on_contract(contract)

        self.gateway.write_log("CM contract data received")

    def on_query_margin_contract(self, data: dict) -> None:
        """Callback of margin contracts query."""
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

            symbol = d["symbol"] + "_SPOT_BINANCE"

            contract: ContractData = ContractData(
                symbol=symbol,
                exchange=Exchange.GLOBAL,
                name=d["symbol"],
                pricetick=pricetick,
                size=1,
                min_volume=min_volume,
                max_volume=max_volume,
                product=Product.SPOT,
                net_position=True,
                history_data=True,
                gateway_name=self.gateway_name,
                stop_supported=False
            )
            self.gateway.on_contract(contract)

        self.gateway.write_log("Margin contract data received")

    def on_send_order(self, data: dict, request: Request) -> None:
        """Callback of send order."""
        pass

    def on_send_order_failed(self, status_code: int, request: Request) -> None:
        """Callback when send order failed."""
        order: OrderData = request.extra
        order.status = Status.REJECTED
        self.gateway.on_order(order)

        data: dict = request.response.json()
        error_code: int = data["code"]
        error_msg: str = data["msg"]
        msg: str = f"Order failed, code: {error_code}, message: {error_msg}"
        self.gateway.write_log(msg)

    def on_send_order_error(
        self,
        exception_type: type,
        exception_value: Exception,
        tb: Any,
        request: Request
    ) -> None:
        """Callback when send order has error."""
        order: OrderData = request.extra
        order.status = Status.REJECTED
        self.gateway.on_order(order)

        if not issubclass(exception_type, ConnectionError | TimeoutError):
            self.on_error(exception_type, exception_value, tb, request)

    def on_cancel_order(self, data: dict, request: Request) -> None:
        """Callback of cancel order."""
        pass

    def on_cancel_order_failed(self, status_code: int, request: Request) -> None:
        """Callback when cancel order failed."""
        data: dict = request.response.json()
        error_code: int = data["code"]
        error_msg: str = data["msg"]
        msg: str = f"Cancel failed, code: {error_code}, message: {error_msg}"
        self.gateway.write_log(msg)

    def on_start_user_stream(self, data: dict, request: Request) -> None:
        """Callback of start user stream."""
        self.user_stream_key = data["listenKey"]
        self.keep_alive_count = 0

        if self.server == "REAL":
            url = REAL_USER_HOST + self.user_stream_key
        else:
            url = TESTNET_USER_HOST + self.user_stream_key

        self.user_api.connect(url, self.proxy_host, self.proxy_port)

    def on_keep_user_stream(self, data: dict, request: Request) -> None:
        """Callback of keep user stream."""
        pass

    def on_keep_user_stream_error(
        self,
        exception_type: type,
        exception_value: Exception,
        tb: Any,
        request: Request
    ) -> None:
        """Error callback of keep user stream."""
        if not issubclass(exception_type, TimeoutError):
            self.on_error(exception_type, exception_value, tb, request)

    def query_history(self, req: HistoryRequest) -> list[BarData]:
        """Query kline history data."""
        contract: ContractData | None = self.gateway.get_contract_by_symbol(req.symbol)
        if not contract:
            return []

        # Check interval
        if not req.interval:
            return []

        history: list[BarData] = []
        limit: int = 1500
        start_time: int = int(datetime.timestamp(req.start))

        # Select endpoint based on market type
        market_type: MarketType = get_market_type(req.symbol)
        if market_type == MarketType.UM:
            client = self.um_client
            path = "/fapi/v1/klines"
        elif market_type == MarketType.CM:
            client = self.cm_client
            path = "/dapi/v1/klines"
        else:
            client = self.margin_client
            path = "/api/v3/klines"
            limit = 1000

        if not client:
            return []

        while True:
            params: dict = {
                "symbol": contract.name,
                "interval": INTERVAL_VT2BINANCE[req.interval],
                "limit": limit,
                "startTime": start_time * 1000
            }

            if req.end:
                end_time = int(datetime.timestamp(req.end))
                params["endTime"] = end_time * 1000

            resp: Response = client.request(
                "GET",
                path=path,
                params=params
            )

            if resp.status_code // 100 != 2:
                msg: str = f"Query kline history failed, status code: {resp.status_code}, message: {resp.text}"
                self.gateway.write_log(msg)
                break
            else:
                data: list = resp.json()
                if not data:
                    msg = f"No kline history data received, start time: {start_time}"
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

                begin: datetime = buf[0].datetime
                end: datetime = buf[-1].datetime

                history.extend(buf)
                msg = f"Query kline history finished, {req.symbol} - {req.interval.value}, {begin} - {end}"
                self.gateway.write_log(msg)

                if len(data) < limit or (req.end and end >= req.end):
                    break

                start_dt = bar.datetime + TIMEDELTA_MAP[req.interval]
                start_time = int(datetime.timestamp(start_dt))

            sleep(0.5)

        if history:
            history.pop(-1)

        return history

    def stop(self) -> None:
        """Stop REST API and cleanup."""
        super().stop()

        if self.um_client:
            self.um_client.stop()
        if self.cm_client:
            self.cm_client.stop()
        if self.margin_client:
            self.margin_client.stop()


class UserApi(WebsocketClient):
    """
    The user data websocket API of BinancePortfolioGateway.

    Handles real-time updates for:
    - Account balance changes
    - Position updates
    - Order status changes
    - Trade executions
    """

    def __init__(self, gateway: BinancePortfolioGateway) -> None:
        """Initialize the API."""
        super().__init__()

        self.gateway: BinancePortfolioGateway = gateway
        self.gateway_name: str = gateway.gateway_name

    def connect(self, url: str, proxy_host: str, proxy_port: int) -> None:
        """Start server connection."""
        self.init(url, proxy_host, proxy_port, receive_timeout=WEBSOCKET_TIMEOUT)
        self.start()

    def on_connected(self) -> None:
        """Callback when server is connected."""
        self.gateway.write_log("User API connected")

    def on_packet(self, packet: dict) -> None:
        """Callback of data update."""
        match packet["e"]:
            case "ACCOUNT_UPDATE":
                self.on_account(packet)
            case "ORDER_TRADE_UPDATE":
                self.on_order(packet)
            case "outboundAccountPosition":
                self.on_account_outbound(packet)
            case "executionReport":
                self.on_execution_report(packet)
            case "listenKeyExpired":
                self.on_listen_key_expired()

    def on_listen_key_expired(self) -> None:
        """Callback of listen key expired."""
        self.gateway.write_log("Listen key expired")
        self.disconnect()

    def on_account(self, packet: dict) -> None:
        """Callback of futures account update."""
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
                contract: ContractData | None = self.gateway.get_futures_contract_by_name(name)
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

    def on_account_outbound(self, packet: dict) -> None:
        """Callback of margin account balance update."""
        for acc_data in packet["B"]:
            free = float(acc_data["f"])
            locked = float(acc_data["l"])

            if free or locked:
                account: AccountData = AccountData(
                    accountid=acc_data["a"] + "_MARGIN",
                    balance=free + locked,
                    frozen=locked,
                    gateway_name=self.gateway_name
                )
                self.gateway.on_account(account)

    def on_order(self, packet: dict) -> None:
        """Callback of futures order update."""
        ord_data: dict = packet["o"]

        key: tuple[str, str] = (ord_data["o"], ord_data["f"])
        order_type: OrderType | None = ORDERTYPE_BINANCE2VT.get(key, None)
        if not order_type:
            return

        name: str = ord_data["s"]
        contract: ContractData | None = self.gateway.get_futures_contract_by_name(name)
        if not contract:
            return

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

        trade_volume: float = float(ord_data["l"])
        trade_volume = round_to(trade_volume, contract.min_volume)
        if not trade_volume:
            return

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

    def on_execution_report(self, packet: dict) -> None:
        """Callback of margin order update."""
        key: tuple[str, str] = (packet["o"], packet["f"])
        order_type: OrderType | None = ORDERTYPE_BINANCE2VT.get(key, None)
        if not order_type:
            return

        name: str = packet["s"]
        contract: ContractData | None = self.gateway.get_contract_by_name(name, MarketType.MARGIN)
        if not contract:
            return

        # Handle cancel order
        if packet["x"] == "CANCELED":
            orderid = packet["C"]
        else:
            orderid = packet["c"]

        order: OrderData = OrderData(
            symbol=contract.symbol,
            exchange=Exchange.GLOBAL,
            orderid=str(orderid),
            type=order_type,
            direction=DIRECTION_BINANCE2VT[packet["S"]],
            price=float(packet["p"]),
            volume=float(packet["q"]),
            traded=float(packet["z"]),
            status=STATUS_BINANCE2VT[packet["X"]],
            datetime=generate_datetime(packet["E"]),
            gateway_name=self.gateway_name,
        )

        self.gateway.on_order(order)

        trade_volume: float = float(packet["l"])
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
        )
        self.gateway.on_trade(trade)

    def on_disconnected(self, status_code: int, msg: str) -> None:
        """Callback when server is disconnected."""
        self.gateway.write_log(f"User API disconnected, code: {status_code}, msg: {msg}")
        self.gateway.rest_api.start_user_stream()

    def on_error(self, e: Exception) -> None:
        """Callback when exception raised."""
        self.gateway.write_log(f"User API exception: {e}")


class UmMdApi(WebsocketClient):
    """
    The USDT-M futures market data websocket API.

    Handles real-time updates for:
    - Tickers (24hr statistics)
    - Order book depth (10 levels)
    - Klines (candlestick data) if enabled
    """

    def __init__(self, gateway: BinancePortfolioGateway) -> None:
        """Initialize the API."""
        super().__init__()

        self.gateway: BinancePortfolioGateway = gateway
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
        """Start server connection."""
        self.kline_stream = kline_stream

        if server == "REAL":
            self.init(REAL_UM_DATA_HOST, proxy_host, proxy_port, receive_timeout=WEBSOCKET_TIMEOUT)
        else:
            self.init(TESTNET_UM_DATA_HOST, proxy_host, proxy_port, receive_timeout=WEBSOCKET_TIMEOUT)

        self.start()

    def on_connected(self) -> None:
        """Callback when server is connected."""
        self.gateway.write_log("UM MD API connected")

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
        """Subscribe to market data."""
        if req.symbol in self.ticks:
            return

        contract: ContractData | None = self.gateway.get_contract_by_symbol(req.symbol)
        if not contract:
            self.gateway.write_log(f"Failed to subscribe data, symbol not found: {req.symbol}")
            return

        self.reqid += 1

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
        """Callback of market data update."""
        stream: str = packet.get("stream", "")
        if not stream:
            return

        data: dict = packet["data"]
        name, channel = stream.split("@")

        contract: ContractData | None = self.gateway.get_contract_by_name(name.upper(), MarketType.UM)
        if not contract:
            return

        tick: TickData | None = self.ticks.get(contract.symbol)
        if not tick:
            return

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
            bar_ready: bool = kline_data.get("x", False)
            if not bar_ready:
                return

            dt: datetime = generate_datetime(float(kline_data["t"]))

            if tick.extra is None:
                tick.extra = {}

            tick.extra["bar"] = BarData(
                symbol=contract.symbol,
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
        """Callback when server is disconnected."""
        self.gateway.write_log(f"UM MD API disconnected, code: {status_code}, msg: {msg}")

    def on_error(self, e: Exception) -> None:
        """Callback when exception raised."""
        self.gateway.write_log(f"UM MD API exception: {e}")


class CmMdApi(WebsocketClient):
    """
    The Coin-M futures market data websocket API.

    Handles real-time updates for:
    - Tickers (24hr statistics)
    - Order book depth (10 levels)
    - Klines (candlestick data) if enabled
    """

    def __init__(self, gateway: BinancePortfolioGateway) -> None:
        """Initialize the API."""
        super().__init__()

        self.gateway: BinancePortfolioGateway = gateway
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
        """Start server connection."""
        self.kline_stream = kline_stream

        if server == "REAL":
            self.init(REAL_CM_DATA_HOST, proxy_host, proxy_port, receive_timeout=WEBSOCKET_TIMEOUT)
        else:
            self.init(TESTNET_CM_DATA_HOST, proxy_host, proxy_port, receive_timeout=WEBSOCKET_TIMEOUT)

        self.start()

    def on_connected(self) -> None:
        """Callback when server is connected."""
        self.gateway.write_log("CM MD API connected")

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
        """Subscribe to market data."""
        if req.symbol in self.ticks:
            return

        contract: ContractData | None = self.gateway.get_contract_by_symbol(req.symbol)
        if not contract:
            self.gateway.write_log(f"Failed to subscribe data, symbol not found: {req.symbol}")
            return

        self.reqid += 1

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
        """Callback of market data update."""
        stream: str = packet.get("stream", "")
        if not stream:
            return

        data: dict = packet["data"]
        name, channel = stream.split("@")

        contract: ContractData | None = self.gateway.get_contract_by_name(name.upper(), MarketType.CM)
        if not contract:
            return

        tick: TickData | None = self.ticks.get(contract.symbol)
        if not tick:
            return

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
            bar_ready: bool = kline_data.get("x", False)
            if not bar_ready:
                return

            dt: datetime = generate_datetime(float(kline_data["t"]))

            if tick.extra is None:
                tick.extra = {}

            tick.extra["bar"] = BarData(
                symbol=contract.symbol,
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
        """Callback when server is disconnected."""
        self.gateway.write_log(f"CM MD API disconnected, code: {status_code}, msg: {msg}")

    def on_error(self, e: Exception) -> None:
        """Callback when exception raised."""
        self.gateway.write_log(f"CM MD API exception: {e}")


class MarginMdApi(WebsocketClient):
    """
    The margin/spot market data websocket API.

    Handles real-time updates for:
    - Tickers (24hr statistics)
    - Book ticker (best bid/ask)
    - Klines (candlestick data) if enabled
    """

    def __init__(self, gateway: BinancePortfolioGateway) -> None:
        """Initialize the API."""
        super().__init__()

        self.gateway: BinancePortfolioGateway = gateway
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
        """Start server connection."""
        self.kline_stream = kline_stream

        if server == "REAL":
            self.init(REAL_MARGIN_DATA_HOST, proxy_host, proxy_port, receive_timeout=WEBSOCKET_TIMEOUT)
        else:
            self.init(TESTNET_MARGIN_DATA_HOST, proxy_host, proxy_port, receive_timeout=WEBSOCKET_TIMEOUT)

        self.start()

    def on_connected(self) -> None:
        """Callback when server is connected."""
        self.gateway.write_log("Margin MD API connected")

        if self.ticks:
            channels = []
            for symbol in self.ticks.keys():
                channels.append(f"{symbol}@ticker")
                channels.append(f"{symbol}@bookTicker")

                if self.kline_stream:
                    channels.append(f"{symbol}@kline_1m")

            packet: dict = {
                "method": "SUBSCRIBE",
                "params": channels,
                "id": self.reqid
            }
            self.send_packet(packet)

    def subscribe(self, req: SubscribeRequest) -> None:
        """Subscribe to market data."""
        if req.symbol in self.ticks:
            return

        contract: ContractData | None = self.gateway.get_contract_by_symbol(req.symbol)
        if not contract:
            self.gateway.write_log(f"Failed to subscribe data, symbol not found: {req.symbol}")
            return

        self.reqid += 1

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
            f"{contract.name.lower()}@bookTicker"
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
        """Callback of market data update."""
        # Handle stream format
        stream: str = packet.get("stream", "")
        if stream:
            data: dict = packet["data"]
            name, channel = stream.split("@")

            contract: ContractData | None = self.gateway.get_contract_by_name(name.upper(), MarketType.MARGIN)
            if not contract:
                return

            tick: TickData | None = self.ticks.get(contract.symbol)
            if not tick:
                return

            if channel == "ticker":
                tick.volume = float(data["v"])
                tick.turnover = float(data["q"])
                tick.open_price = float(data["o"])
                tick.high_price = float(data["h"])
                tick.low_price = float(data["l"])
                tick.last_price = float(data["c"])
                tick.datetime = generate_datetime(float(data["E"]))
            elif channel == "bookTicker":
                tick.bid_price_1 = float(data["b"])
                tick.bid_volume_1 = float(data["B"])
                tick.ask_price_1 = float(data["a"])
                tick.ask_volume_1 = float(data["A"])
            elif channel.startswith("kline"):
                kline_data: dict = data["k"]
                bar_ready: bool = kline_data.get("x", False)
                if not bar_ready:
                    return

                dt: datetime = generate_datetime(float(kline_data["t"]))

                if tick.extra is None:
                    tick.extra = {}

                tick.extra["bar"] = BarData(
                    symbol=contract.symbol,
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
            return

        # Handle non-stream format (bookTicker)
        symbol_name: str = packet.get("s", "")
        if not symbol_name:
            return

        contract = self.gateway.get_contract_by_name(symbol_name, MarketType.MARGIN)
        if not contract:
            return

        tick = self.ticks.get(contract.symbol)
        if not tick:
            return

        channel = packet.get("e", "bookTicker")

        if channel == "24hrTicker":
            tick.volume = float(packet["v"])
            tick.turnover = float(packet["q"])
            tick.open_price = float(packet["o"])
            tick.high_price = float(packet["h"])
            tick.low_price = float(packet["l"])
            tick.last_price = float(packet["c"])
            tick.datetime = generate_datetime(float(packet["E"]))
        elif channel == "bookTicker":
            tick.bid_price_1 = float(packet["b"])
            tick.bid_volume_1 = float(packet["B"])
            tick.ask_price_1 = float(packet["a"])
            tick.ask_volume_1 = float(packet["A"])

        if tick.last_price:
            tick.localtime = datetime.now()
            self.gateway.on_tick(copy(tick))

    def on_disconnected(self, status_code: int, msg: str) -> None:
        """Callback when server is disconnected."""
        self.gateway.write_log(f"Margin MD API disconnected, code: {status_code}, msg: {msg}")

    def on_error(self, e: Exception) -> None:
        """Callback when exception raised."""
        self.gateway.write_log(f"Margin MD API exception: {e}")

