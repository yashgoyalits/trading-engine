stratgies

broker

managers

logger

infrastructure

muje ye hai ki broker ko easy add or delete kar sake to get the real time data and place order which is interdependent on each other taki agar data dusra broker se aaye to place order wale broker ko change nahi karna pade

stragies ko bhi easy add or delete kar sake
stragies apney hisab sey symbol ka data le aur dupicacy na ho matlab agar do stratgey ek symbol use kar rahi  hai toh symbol data two time na aaye iss liye infrastructure jo pura manage kare

setting mey sey pura startegy ka data set kar sake 

easily relaible and scalable system ho 

## Folder Structure

src/
├── core/
│   ├── data_model.py          # Tick, Candle, TradeData, TradeSignal (new)
│   └── interfaces/
│       ├── idata_broker.py    # IDataBroker ABC
│       ├── iorder_broker.py   # IOrderBroker ABC (separate from data)
│       └── istrategy.py       # IStrategy ABC
├── broker/
│   ├── registry.py            # BrokerRegistry
│   └── fyers/
│       ├── data_broker.py     # implements IDataBroker
│       └── order_broker.py    # implements IOrderBroker
├── infrastructure/
│   ├── event_bus.py           # pub/sub, per-subscriber bounded queues
│   ├── logger.py
│   └── error_handling.py
├── managers/
│   ├── symbol_manager.py      # dedup + refcount subscriptions
│   ├── candle_builder/
│   ├── order_placement_manager.py
│   └── order_state_manager.py # now global across strategies
├── strategies/
│   ├── registry.py            # dynamic loading from config
│   ├── base_strategy.py       # IStrategy base + queue consumer loop
│   ├── strategy_one/
│   └── strategy_two/          # adding strategy = folder + config entry
└── engine.py                  # orchestrator

config/
└── settings.yml

## Fixes

Fix 1 — uvloop
Fix 2 — run_in_executor ThreadPoolExecutor
Fix 3 — Vectorized batch
Fix 4 — Two-process split

## Pattern-to-failure-mode map — summary

| Pattern | Bina iske kya hota |
|---|---|
| Singleton (EventBus) | Multiple instances, subscriptions miss |
| Factory (BrokerRegistry) | `if/elif` broker chain, naya broker = 5 file change |
| Adapter | Engine Zerodha format nahi samajh sakta |
| Proxy (RiskManager) | Strategy risk bypass kar sakti hai |
| Chain of Resp. | New risk rule = monolith function modify |
| Template Method | Har strategy apna consumer loop likhti hai, bugs alag alag |
| State (Trade) | Invalid transition silent — CLOSED trade pe trailing attempt |
| Bounded Queue + Backpressure | Slow strategy publisher ko block kare |
| Active Object | One strategy exception = all strategies down |
| CQRS | Read queries write lock hold kare = latency |
| Event Sourcing | Koi audit trail nahi, SEBI compliance fail |
| Hexagonal | Fyers import andar = Zerodha add karna rewrite |