import vnpy_crypto
vnpy_crypto.init()

from vnpy.event import EventEngine
from vnpy.trader.engine import MainEngine
from vnpy.trader.ui import MainWindow, create_qapp

from vnpy_binance import (
    BinanceSpotGateway,
    BinanceUsdtGateway,
    BinanceInverseGateway
)


def main():
    """主入口函数"""
    qapp = create_qapp()

    event_engine = EventEngine()
    main_engine = MainEngine(event_engine)
    main_engine.add_gateway(BinanceSpotGateway)
    main_engine.add_gateway(BinanceUsdtGateway)
    main_engine.add_gateway(BinanceInverseGateway)

    main_window = MainWindow(main_engine, event_engine)
    main_window.showMaximized()

    qapp.exec()


if __name__ == "__main__":
    main()
