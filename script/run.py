from vnpy_evo.event import EventEngine
from vnpy_evo.trader.engine import MainEngine
from vnpy_evo.trader.ui import MainWindow, create_qapp

from vnpy_binance import (
    BinanceSpotGateway,
    BinanceLinearGateway,
    BinanceInverseGateway
)


def main():
    """主入口函数"""
    qapp = create_qapp()

    event_engine = EventEngine()
    main_engine = MainEngine(event_engine)
    main_engine.add_gateway(BinanceSpotGateway)
    main_engine.add_gateway(BinanceLinearGateway)
    main_engine.add_gateway(BinanceInverseGateway)

    main_window = MainWindow(main_engine, event_engine)
    main_window.showMaximized()

    qapp.exec()


if __name__ == "__main__":
    main()
